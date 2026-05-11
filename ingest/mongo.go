package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongoBatchSize = 1000

func createMongoIndexes(ctx context.Context, client *mongo.Client) error {
	db := client.Database("streaming")

	// actors ids
	_, err := db.Collection("movies").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "actors.id", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("movies.actors.id index: %w", err)
	}

	// genres ids
	_, err = db.Collection("movies").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "genres.id", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("movies.genres.id index: %w", err)
	}

	// ratings user ids + movie ids (pour éviter les doublons et accélérer les requêtes de type "quels films a noté cet utilisateur ?")
	_, err = db.Collection("ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "user_id", Value: 1}, {Key: "movie_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("ratings user/movie index: %w", err)
	}

	// ratings movie ids
	_, err = db.Collection("ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "movie_id", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("ratings.movie_id index: %w", err)
	}

	// ratings movie ids + rating (pour les requêtes de type "trouve moi les films les mieux notés")
	_, err = db.Collection("ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "movie_id", Value: 1}, {Key: "rating", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("ratings.movie_id_rating index: %w", err)
	}

	// ratings user ids
	_, err = db.Collection("ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "user_id", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("ratings.user_id index: %w", err)
	}

	return nil
}

// on tente le lot en une fois, sinon on repasse document par document
func insertMongoBatch(ctx context.Context, coll *mongo.Collection, docs []interface{}, errLog *ErrorLogger, source string) int {
	if len(docs) == 0 {
		return 0
	}

	_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err == nil {
		return len(docs)
	}

	inserted := 0
	for _, doc := range docs {
		if _, err := coll.InsertOne(ctx, doc); err != nil {
			errLog.Log(source, "", fmt.Sprintf("insertion: %v", err))
			continue
		}
		inserted++
	}
	return inserted
}

// pareil pour ratings, mais les doublons ne nous bloquent pas
func insertRatingsBatch(ctx context.Context, coll *mongo.Collection, docs []interface{}, errLog *ErrorLogger) error {
	if len(docs) == 0 {
		return nil
	}

	_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err == nil || mongo.IsDuplicateKeyError(err) {
		return nil
	}

	for _, doc := range docs {
		_, err := coll.InsertOne(ctx, doc)
		if err == nil || mongo.IsDuplicateKeyError(err) {
			continue
		}
		errLog.Log("mongo/ratings", "", fmt.Sprintf("insertion: %v", err))
	}
	return nil
}

/////////////////////////////////////////////////

func loadMongo(ctx context.Context, client *mongo.Client, jsonDir string, errLog *ErrorLogger) error {
	db := client.Database("streaming")
	// on repart de zéro
	for _, name := range []string{"movies", "users", "ratings"} {
		db.Collection(name).Drop(ctx)
	}

	// refs en mémoire pour monter les docs films et utilisateurs

	workerMap := make(map[string]string) // id -> name
	err := streamJSON(filepath.Join(jsonDir, "workers.json"), errLog, func(w Worker) {
		if w.ID != "" {
			workerMap[w.ID] = w.Name
		}
	})
	if err != nil {
		return fmt.Errorf("workers: %w", err)
	}

	genreMap := make(map[string]string)
	err = streamJSON(filepath.Join(jsonDir, "genres.json"), errLog, func(g Genre) {
		if g.ID != "" {
			genreMap[g.ID] = g.Name
		}
	})
	if err != nil {
		return fmt.Errorf("genres: %w", err)
	}

	awardMap := make(map[string]Award)
	err = streamJSON(filepath.Join(jsonDir, "awards.json"), errLog, func(a Award) {
		if a.ID != "" {
			awardMap[a.ID] = a
		}
	})
	if err != nil {
		return fmt.Errorf("awards: %w", err)
	}

	// bouts rattachés aux films

	movieGenres := make(map[string][]MongoGenreRef)
	err = streamJSON(filepath.Join(jsonDir, "movies_genres.json"), errLog, func(mg MovieGenre) {
		if mg.MovieID == "" || mg.GenreID == "" {
			return
		}
		name := genreMap[mg.GenreID]
		movieGenres[mg.MovieID] = append(movieGenres[mg.MovieID], MongoGenreRef{ID: mg.GenreID, Name: name})
	})
	if err != nil {
		return fmt.Errorf("movies_genres: %w", err)
	}

	movieActors := make(map[string][]MongoActorRef)
	err = streamJSON(filepath.Join(jsonDir, "movies_actors.json"), errLog, func(ma MovieActor) {
		if ma.MovieID == "" || ma.ActorID == "" {
			return
		}
		name := workerMap[ma.ActorID]
		movieActors[ma.MovieID] = append(movieActors[ma.MovieID], MongoActorRef{ID: ma.ActorID, Name: name, Role: ma.Role})
	})
	if err != nil {
		return fmt.Errorf("movies_actors: %w", err)
	}

	movieAwards := make(map[string][]MongoAwardRef)
	err = streamJSON(filepath.Join(jsonDir, "movies_awards.json"), errLog, func(maw MovieAward) {
		if maw.MovieID == "" || maw.AwardID == "" {
			return
		}
		a := awardMap[maw.AwardID]
		movieAwards[maw.MovieID] = append(movieAwards[maw.MovieID], MongoAwardRef{ID: a.ID, Name: a.Name, Category: a.Category, Year: maw.Year})
	})
	if err != nil {
		return fmt.Errorf("movies_awards: %w", err)
	}

	// films
	moviesColl := db.Collection("movies")
	start := time.Now()

	batch := make([]interface{}, 0, mongoBatchSize)
	totalMovies := 0

	err = streamJSON(filepath.Join(jsonDir, "movies.json"), errLog, func(m Movie) {
		if m.ID == "" {
			errLog.Log("mongo/movies.json", m.Title, "id manquant")
			return
		}

		var director *MongoWorkerRef
		if m.DirectorID != "" {
			if name, ok := workerMap[m.DirectorID]; ok {
				director = &MongoWorkerRef{ID: m.DirectorID, Name: name}
			}
		}

		// si metadata contient du json, on le garde comme ça
		var metadata interface{}
		if m.Metadata != "" {
			if err := json.Unmarshal([]byte(m.Metadata), &metadata); err != nil {
				metadata = m.Metadata
			}
		}

		doc := MongoMovieDoc{
			ID:       m.ID,
			Title:    m.Title,
			Year:     m.Year,
			Director: director,
			Metadata: metadata,
			Genres:   movieGenres[m.ID],
			Actors:   movieActors[m.ID],
			Awards:   movieAwards[m.ID],
		}
		if doc.Genres == nil {
			doc.Genres = []MongoGenreRef{}
		}
		if doc.Actors == nil {
			doc.Actors = []MongoActorRef{}
		}
		if doc.Awards == nil {
			doc.Awards = []MongoAwardRef{}
		}

		batch = append(batch, doc)
		if len(batch) >= mongoBatchSize {
			totalMovies += insertMongoBatch(ctx, moviesColl, batch, errLog, "mongo/movies")
			batch = batch[:0]
		}
	})
	if err != nil {
		return fmt.Errorf("movies stream: %w", err)
	}
	if len(batch) > 0 {
		totalMovies += insertMongoBatch(ctx, moviesColl, batch, errLog, "mongo/movies")
	}
	log.Printf("[mongo] films: %d en %v", totalMovies, time.Since(start))

	movieGenres = nil
	movieActors = nil
	movieAwards = nil

	// bouts rattachés aux utilisateurs

	userFavGenres := make(map[string][]MongoGenreRef)
	err = streamJSON(filepath.Join(jsonDir, "favorite_genres.json"), errLog, func(fg FavoriteGenre) {
		if fg.UserID == "" || fg.GenreID == "" {
			return
		}
		name := genreMap[fg.GenreID]
		userFavGenres[fg.UserID] = append(userFavGenres[fg.UserID], MongoGenreRef{ID: fg.GenreID, Name: name})
	})
	if err != nil {
		return fmt.Errorf("favorite_genres: %w", err)
	}

	userWatchHistory := make(map[string][]MongoWatchEntry)
	err = streamJSON(filepath.Join(jsonDir, "watch_history.json"), errLog, func(wh WatchHistory) {
		if wh.UserID == "" || wh.MovieID == "" {
			return
		}
		userWatchHistory[wh.UserID] = append(userWatchHistory[wh.UserID], MongoWatchEntry{MovieID: wh.MovieID, WatchedOn: wh.WatchedOn})
	})
	if err != nil {
		return fmt.Errorf("watch_history: %w", err)
	}

	makeRatingsIdx := func(ctx context.Context, coll *mongo.Collection) error {
		_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "user_id", Value: 1}, {Key: "movie_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
		if err != nil {
			return fmt.Errorf("ratings user/movie index: %w", err)
		}
		return nil
	}

	ratingsColl := db.Collection("ratings")
	if err := makeRatingsIdx(ctx, ratingsColl); err != nil {
		return err
	}
	start = time.Now()
	ratingsBatch := make([]interface{}, 0, mongoBatchSize)
	err = streamJSON(filepath.Join(jsonDir, "ratings.json"), errLog, func(r Rating) {
		if r.UserID == "" || r.MovieID == "" {
			return
		}
		rating, err := r.Rating.Float64()
		if err != nil {
			errLog.Log("mongo/ratings.json", r.UserID, fmt.Sprintf("note invalide: %v", err))
			return
		}
		if rating < 0 || rating > 5 {
			errLog.Log("mongo/ratings.json", r.UserID, fmt.Sprintf("note hors plage: %v", rating))
			return
		}
		review := ""
		if r.Review != nil {
			review = *r.Review
		}
		ratingsBatch = append(ratingsBatch, MongoRatingDoc{UserID: r.UserID, MovieID: r.MovieID, Rating: rating, Review: review})
		if len(ratingsBatch) >= mongoBatchSize {
			if err := insertRatingsBatch(ctx, ratingsColl, ratingsBatch, errLog); err != nil {
				errLog.Log("mongo/ratings", "", fmt.Sprintf("lot ratings: %v", err))
			}
			ratingsBatch = ratingsBatch[:0]
		}
	})
	if err != nil {
		return fmt.Errorf("ratings: %w", err)
	}
	if len(ratingsBatch) > 0 {
		if err := insertRatingsBatch(ctx, ratingsColl, ratingsBatch, errLog); err != nil {
			errLog.Log("mongo/ratings", "", fmt.Sprintf("lot ratings: %v", err))
		}
	}
	totalRatings, err := ratingsColl.CountDocuments(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("compte mongo ratings: %w", err)
	}
	log.Printf("[mongo] notes: %d en %v", totalRatings, time.Since(start))

	// utilisateurs
	usersColl := db.Collection("users")
	start = time.Now()

	batch = batch[:0]
	totalUsers := 0

	err = streamJSON(filepath.Join(jsonDir, "users.json"), errLog, func(u User) {
		if u.ID == "" {
			errLog.Log("mongo/users.json", u.Name, "id manquant")
			return
		}
		doc := MongoUserDoc{
			ID:             u.ID,
			Name:           u.Name,
			FavoriteGenres: userFavGenres[u.ID],
			WatchHistory:   userWatchHistory[u.ID],
		}
		if doc.FavoriteGenres == nil {
			doc.FavoriteGenres = []MongoGenreRef{}
		}
		if doc.WatchHistory == nil {
			doc.WatchHistory = []MongoWatchEntry{}
		}
		batch = append(batch, doc)
		if len(batch) >= mongoBatchSize {
			totalUsers += insertMongoBatch(ctx, usersColl, batch, errLog, "mongo/users")
			batch = batch[:0]
		}
	})
	if err != nil {
		return fmt.Errorf("users stream: %w", err)
	}
	if len(batch) > 0 {
		totalUsers += insertMongoBatch(ctx, usersColl, batch, errLog, "mongo/users")
	}
	log.Printf("[mongo] utilisateurs: %d en %v", totalUsers, time.Since(start))

	return nil
}

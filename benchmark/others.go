package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// s1 s2 s3

type mongoS1RatingAverage struct {
	MovieID   string  `bson:"_id"`
	AvgRating float64 `bson:"avg_rating"`
}

type mongoS1GenreRef struct {
	ID   string `bson:"id"`
	Name string `bson:"name"`
}

type mongoS1Movie struct {
	ID     string            `bson:"_id"`
	Title  string            `bson:"title"`
	Genres []mongoS1GenreRef `bson:"genres"`
}

type mongoS1TopMovie struct {
	GenreID   string
	Genre     string
	MovieID   string
	Title     string
	AvgRating float64
	Rank      int
}

func mongoS1TopLess(left, right mongoS1TopMovie) bool {
	if left.AvgRating != right.AvgRating {
		return left.AvgRating > right.AvgRating
	}
	return left.MovieID < right.MovieID
}

func addMongoS1Candidate(topMovies []mongoS1TopMovie, candidate mongoS1TopMovie) []mongoS1TopMovie {
	topMovies = append(topMovies, candidate)
	sort.Slice(topMovies, func(leftIndex, rightIndex int) bool {
		return mongoS1TopLess(topMovies[leftIndex], topMovies[rightIndex])
	})
	if len(topMovies) > 3 {
		return topMovies[:3]
	}
	return topMovies
}

func s1PG(ctx context.Context, pool *pgxpool.Pool, runs int) (BenchResult, error) {
	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		rows, err := pool.Query(queryCtx, `
			WITH movie_ratings AS (
				SELECT movie_id, AVG(rating) AS avg_rating
				FROM ratings
				GROUP BY movie_id
			), ranked AS (
				SELECT g.name AS genre, m.title, movie_ratings.avg_rating,
					ROW_NUMBER() OVER (
						PARTITION BY g.id
						ORDER BY movie_ratings.avg_rating DESC, m.id ASC
					) AS rank
				FROM movie_ratings
				JOIN movies m ON m.id = movie_ratings.movie_id
				JOIN movies_genres mg ON mg.movie_id = m.id
				JOIN genres g ON g.id = mg.genre_id
			)
			SELECT genre, title, avg_rating, rank
			FROM ranked
			WHERE rank <= 3
			ORDER BY genre ASC, rank ASC, title ASC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var genre, title string
			var avg float64
			var rank int
			if err := rows.Scan(&genre, &title, &avg, &rank); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL S1 warmup: %w", err)
	}
	return benchSame("PostgreSQL", "S1", runs, run)
}

func s1MG(ctx context.Context, db *mongo.Database, runs int) (BenchResult, error) {
	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		ratingsPipeline := bson.A{
			bson.D{{Key: "$sort", Value: bson.D{{Key: "movie_id", Value: 1}, {Key: "rating", Value: 1}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$movie_id"}, {Key: "avg_rating", Value: bson.D{{Key: "$avg", Value: "$rating"}}}}}},
		}

		ratingsCur, err := db.Collection("ratings").Aggregate(queryCtx, ratingsPipeline, options.Aggregate().SetAllowDiskUse(true).SetHint(bson.D{{Key: "movie_id", Value: 1}, {Key: "rating", Value: 1}}))
		if err != nil {
			return err
		}
		defer ratingsCur.Close(queryCtx)

		avgByMovieID := make(map[string]float64)
		for ratingsCur.Next(queryCtx) {
			var ratingAverage mongoS1RatingAverage
			if err := ratingsCur.Decode(&ratingAverage); err != nil {
				return err
			}
			avgByMovieID[ratingAverage.MovieID] = ratingAverage.AvgRating
		}
		if err := ratingsCur.Err(); err != nil {
			return err
		}

		moviesCur, err := db.Collection("movies").Find(queryCtx, bson.D{}, options.Find().SetProjection(bson.D{
			{Key: "_id", Value: 1},
			{Key: "title", Value: 1},
			{Key: "genres", Value: 1},
		}))
		if err != nil {
			return err
		}
		defer moviesCur.Close(queryCtx)

		topByGenreID := make(map[string][]mongoS1TopMovie)
		for moviesCur.Next(queryCtx) {
			var movie mongoS1Movie
			if err := moviesCur.Decode(&movie); err != nil {
				return err
			}

			avgRating, ok := avgByMovieID[movie.ID]
			if !ok {
				continue
			}

			for _, genre := range movie.Genres {
				candidate := mongoS1TopMovie{
					GenreID:   genre.ID,
					Genre:     genre.Name,
					MovieID:   movie.ID,
					Title:     movie.Title,
					AvgRating: avgRating,
				}
				topByGenreID[genre.ID] = addMongoS1Candidate(topByGenreID[genre.ID], candidate)
			}
		}
		if err := moviesCur.Err(); err != nil {
			return err
		}

		rows := make([]mongoS1TopMovie, 0, len(topByGenreID)*3)
		for _, topMovies := range topByGenreID {
			for rankIndex, topMovie := range topMovies {
				topMovie.Rank = rankIndex + 1
				rows = append(rows, topMovie)
			}
		}
		sort.Slice(rows, func(leftIndex, rightIndex int) bool {
			left := rows[leftIndex]
			right := rows[rightIndex]
			if left.Genre != right.Genre {
				return left.Genre < right.Genre
			}
			if left.Rank != right.Rank {
				return left.Rank < right.Rank
			}
			return left.Title < right.Title
		})
		for range rows {
		}
		return queryCtx.Err()
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB S1 warmup: %w", err)
	}
	return benchSame("MongoDB", "S1", runs, run)
}

func s1N4(ctx context.Context, d neo4j.DriverWithContext, runs int) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			res, err := tx.Run(queryCtx, `
				MATCH (u:User)-[r:RATED]->(m:Movie)-[:HAS_GENRE]->(g:Genre)
				WITH g, m, AVG(r.rating) AS avg_rating
				ORDER BY g.name ASC, avg_rating DESC, m.id ASC
				WITH g, collect({title: m.title, avg_rating: avg_rating})[0..3] AS top_movies
				UNWIND range(0, size(top_movies) - 1) AS idx
				WITH g, idx, top_movies[idx] AS top_movie
				RETURN g.name AS genre, top_movie.title AS title, top_movie.avg_rating AS avg_rating, idx + 1 AS rank
				ORDER BY genre ASC, rank ASC, title ASC`, nil)
			if err != nil {
				return nil, err
			}
			return nil, consumeNeo4jResult(queryCtx, res, func(record *neo4j.Record) error {
				if _, ok := record.Get("genre"); !ok {
					return fmt.Errorf("champ genre absent du résultat")
				}
				if _, ok := record.Get("title"); !ok {
					return fmt.Errorf("champ title absent du résultat")
				}
				if _, ok := record.Get("avg_rating"); !ok {
					return fmt.Errorf("champ avg_rating absent du résultat")
				}
				if _, ok := record.Get("rank"); !ok {
					return fmt.Errorf("champ rank absent du résultat")
				}
				return nil
			})
		})
		return err
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j S1 warmup: %w", err)
	}
	return benchSame("Neo4j", "S1", runs, run)
}

func s2PG(ctx context.Context, pool *pgxpool.Pool, warmupActorID string, actorIDs []string) (BenchResult, error) {
	runOne := func(actorID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		rows, err := pool.Query(queryCtx, `
			SELECT m.id, m.title, COALESCE(m.year, 0) AS year, COALESCE(ma.role, '') AS role
			FROM movies_actors ma
			JOIN movies m ON m.id = ma.movie_id
			WHERE ma.actor_id = $1
			ORDER BY year ASC, m.title ASC, m.id ASC`, actorID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var id, title, role string
			var year int
			if err := rows.Scan(&id, &title, &year, &role); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupActorID); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL S2 warmup: %w", err)
	}
	return benchN("PostgreSQL", "S2", len(actorIDs), func(i int) error { return runOne(actorIDs[i]) })
}

func s2MG(ctx context.Context, db *mongo.Database, warmupActorID string, actorIDs []string) (BenchResult, error) {
	runOne := func(actorID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		pipeline := bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "actors.id", Value: actorID}}}},
			bson.D{{Key: "$unwind", Value: "$actors"}},
			bson.D{{Key: "$match", Value: bson.D{{Key: "actors.id", Value: actorID}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "id", Value: "$_id"}, {Key: "title", Value: 1}, {Key: "year", Value: 1}, {Key: "role", Value: "$actors.role"}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "year", Value: 1}, {Key: "title", Value: 1}, {Key: "id", Value: 1}}}},
		}

		cur, err := db.Collection("movies").Aggregate(queryCtx, pipeline)
		if err != nil {
			return err
		}
		if err := consumeMongoCursor(queryCtx, cur); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupActorID); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB S2 warmup: %w", err)
	}
	return benchN("MongoDB", "S2", len(actorIDs), func(i int) error { return runOne(actorIDs[i]) })
}

func s2N4(ctx context.Context, d neo4j.DriverWithContext, warmupActorID string, actorIDs []string) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	runOne := func(actorID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			res, err := tx.Run(queryCtx, `
				MATCH (a:Person {id: $aid})-[r:ACTED_IN]->(m:Movie)
				RETURN m.id AS id, m.title AS title, m.year AS year, r.role AS role
				ORDER BY year ASC, title ASC, id ASC`, map[string]interface{}{"aid": actorID})
			if err != nil {
				return nil, err
			}
			return nil, consumeNeo4jResult(queryCtx, res, func(record *neo4j.Record) error {
				if _, ok := record.Get("id"); !ok {
					return fmt.Errorf("champ id absent du résultat")
				}
				if _, ok := record.Get("title"); !ok {
					return fmt.Errorf("champ title absent du résultat")
				}
				if _, ok := record.Get("year"); !ok {
					return fmt.Errorf("champ year absent du résultat")
				}
				if _, ok := record.Get("role"); !ok {
					return fmt.Errorf("champ role absent du résultat")
				}
				return nil
			})
		})
		return err
	}

	if err := runOne(warmupActorID); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j S2 warmup: %w", err)
	}
	return benchN("Neo4j", "S2", len(actorIDs), func(i int) error { return runOne(actorIDs[i]) })
}

func s3PG(ctx context.Context, pool *pgxpool.Pool, warmupUserID string, userIDs []string) (BenchResult, error) {
	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		rows, err := pool.Query(queryCtx, `
			WITH fav AS (
				SELECT genre_id
				FROM favorite_genres
				WHERE user_id = $1
			), seen AS (
				SELECT movie_id
				FROM watch_history
				WHERE user_id = $1
			)
			SELECT m.id, m.title, COALESCE(m.year, 0) AS year,
				COUNT(DISTINCT mg.genre_id) AS matching_genres
			FROM fav
			JOIN movies_genres mg ON mg.genre_id = fav.genre_id
			JOIN movies m ON m.id = mg.movie_id
			LEFT JOIN seen ON seen.movie_id = m.id
			WHERE seen.movie_id IS NULL
			GROUP BY m.id, m.title, m.year
			ORDER BY matching_genres DESC, m.title ASC, m.id ASC
			LIMIT 10`, userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var id, title string
			var year int
			var matchingGenres int64
			if err := rows.Scan(&id, &title, &year, &matchingGenres); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL S3 warmup: %w", err)
	}
	return benchN("PostgreSQL", "S3", len(userIDs), func(i int) error { return runOne(userIDs[i]) })
}

func s3MG(ctx context.Context, db *mongo.Database, warmupUserID string, userIDs []string) (BenchResult, error) {
	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		var user struct {
			FavoriteGenres []struct {
				ID string `bson:"id"`
			} `bson:"favorite_genres"`
			WatchHistory []struct {
				MovieID string `bson:"movie_id"`
			} `bson:"watch_history"`
		}
		err := db.Collection("users").FindOne(queryCtx, bson.M{"_id": userID}).Decode(&user)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		}

		genreIDs := make([]string, 0, len(user.FavoriteGenres))
		for _, genre := range user.FavoriteGenres {
			genreIDs = append(genreIDs, genre.ID)
		}
		if len(genreIDs) == 0 {
			return nil
		}

		watchedIDs := make([]string, 0, len(user.WatchHistory))
		for _, watched := range user.WatchHistory {
			watchedIDs = append(watchedIDs, watched.MovieID)
		}

		pipeline := bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "genres.id", Value: bson.D{{Key: "$in", Value: genreIDs}}}, {Key: "_id", Value: bson.D{{Key: "$nin", Value: watchedIDs}}}}}},
			bson.D{{Key: "$unwind", Value: "$genres"}},
			bson.D{{Key: "$match", Value: bson.D{{Key: "genres.id", Value: bson.D{{Key: "$in", Value: genreIDs}}}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "id", Value: "$_id"}, {Key: "title", Value: "$title"}, {Key: "year", Value: "$year"}}}, {Key: "genre_ids", Value: bson.D{{Key: "$addToSet", Value: "$genres.id"}}}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "id", Value: "$_id.id"}, {Key: "title", Value: "$_id.title"}, {Key: "year", Value: "$_id.year"}, {Key: "matching_genres", Value: bson.D{{Key: "$size", Value: "$genre_ids"}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "matching_genres", Value: -1}, {Key: "title", Value: 1}, {Key: "id", Value: 1}}}},
			bson.D{{Key: "$limit", Value: 10}},
		}

		cur, err := db.Collection("movies").Aggregate(queryCtx, pipeline, options.Aggregate().SetAllowDiskUse(true))
		if err != nil {
			return err
		}
		if err := consumeMongoCursor(queryCtx, cur); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB S3 warmup: %w", err)
	}
	return benchN("MongoDB", "S3", len(userIDs), func(i int) error { return runOne(userIDs[i]) })
}

func s3N4(ctx context.Context, d neo4j.DriverWithContext, warmupUserID string, userIDs []string) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			res, err := tx.Run(queryCtx, `
				MATCH (u:User {id: $uid})-[:PREFERS]->(g:Genre)<-[:HAS_GENRE]-(m:Movie)
				WHERE NOT (u)-[:WATCHED]->(m)
				RETURN m.id AS id, m.title AS title, m.year AS year,
					count(DISTINCT g) AS matching_genres
				ORDER BY matching_genres DESC, title ASC, id ASC
				LIMIT 10`, map[string]interface{}{"uid": userID})
			if err != nil {
				return nil, err
			}
			return nil, consumeNeo4jResult(queryCtx, res, func(record *neo4j.Record) error {
				if _, ok := record.Get("id"); !ok {
					return fmt.Errorf("champ id absent du résultat")
				}
				if _, ok := record.Get("title"); !ok {
					return fmt.Errorf("champ title absent du résultat")
				}
				if _, ok := record.Get("year"); !ok {
					return fmt.Errorf("champ year absent du résultat")
				}
				if _, ok := record.Get("matching_genres"); !ok {
					return fmt.Errorf("champ matching_genres absent du résultat")
				}
				return nil
			})
		})
		return err
	}

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j S3 warmup: %w", err)
	}
	return benchN("Neo4j", "S3", len(userIDs), func(i int) error { return runOne(userIDs[i]) })
}

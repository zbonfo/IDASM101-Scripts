package main

import "encoding/json"

// Structs lues depuis les fichiers JSON.

type Worker struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Genre struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Award struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Category string `json:"category"`
}

type Movie struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Year       int    `json:"year"`
	DirectorID string `json:"director_id"`
	Metadata   string `json:"metadata"`
}

type MovieGenre struct {
	MovieID string `json:"movie_id"`
	GenreID string `json:"genre_id"`
}

type MovieActor struct {
	ID      string `json:"id"`
	MovieID string `json:"movie_id"`
	ActorID string `json:"actor_id"`
	Role    string `json:"role"`
}

type MovieAward struct {
	MovieID string `json:"movie_id"`
	AwardID string `json:"award_id"`
	Year    int    `json:"year"`
}

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type FavoriteGenre struct {
	UserID  string `json:"user_id"`
	GenreID string `json:"genre_id"`
}

type WatchHistory struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	MovieID   string `json:"movie_id"`
	WatchedOn string `json:"watched_on"`
}

type Rating struct {
	UserID  string      `json:"user_id"`
	MovieID string      `json:"movie_id"`
	Rating  json.Number `json:"rating"`
	Review  *string     `json:"review"`
}

/////////////////////////////////////////////////////

// MongoMovieDoc
// pour mongo on fait un seul document dénormalisé par film, avec des sous-documents pour les références (worker, genres, etc.).
// on ne met pas les ratings à cause de la limite de taille des documents sinon ça plante pour le large dataset.
type MongoMovieDoc struct {
	ID       string          `bson:"_id"`
	Title    string          `bson:"title"`
	Year     int             `bson:"year"`
	Director *MongoWorkerRef `bson:"director,omitempty"`
	Metadata interface{}     `bson:"metadata,omitempty"`
	Genres   []MongoGenreRef `bson:"genres"`
	Actors   []MongoActorRef `bson:"actors"`
	Awards   []MongoAwardRef `bson:"awards"`
}

type MongoWorkerRef struct {
	ID   string `bson:"id"`
	Name string `bson:"name"`
}

type MongoGenreRef struct {
	ID   string `bson:"id"`
	Name string `bson:"name"`
}

type MongoActorRef struct {
	ID   string `bson:"id"`
	Name string `bson:"name"`
	Role string `bson:"role"`
}

type MongoAwardRef struct {
	ID       string `bson:"id"`
	Name     string `bson:"name"`
	Category string `bson:"category"`
	Year     int    `bson:"year"`
}

type MongoUserDoc struct {
	ID             string            `bson:"_id"`
	Name           string            `bson:"name"`
	FavoriteGenres []MongoGenreRef   `bson:"favorite_genres"`
	WatchHistory   []MongoWatchEntry `bson:"watch_history"`
}

type MongoWatchEntry struct {
	MovieID   string `bson:"movie_id"`
	WatchedOn string `bson:"watched_on"`
}

type MongoRatingEntry struct {
	MovieID string  `bson:"movie_id"`
	Rating  float64 `bson:"rating"`
	Review  string  `bson:"review,omitempty"`
}

type MongoRatingDoc struct {
	UserID  string  `bson:"user_id"`
	MovieID string  `bson:"movie_id"`
	Rating  float64 `bson:"rating"`
	Review  string  `bson:"review,omitempty"`
}

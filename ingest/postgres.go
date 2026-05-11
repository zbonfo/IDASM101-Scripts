package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgBatchSize = 5000

func createPostgresSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := `
	DROP TABLE IF EXISTS ratings CASCADE;
	DROP TABLE IF EXISTS watch_history CASCADE;
	DROP TABLE IF EXISTS favorite_genres CASCADE;
	DROP TABLE IF EXISTS movies_awards CASCADE;
	DROP TABLE IF EXISTS movies_genres CASCADE;
	DROP TABLE IF EXISTS movies_actors CASCADE;
	DROP TABLE IF EXISTS movies CASCADE;
	DROP TABLE IF EXISTS users CASCADE;
	DROP TABLE IF EXISTS awards CASCADE;
	DROP TABLE IF EXISTS genres CASCADE;
	DROP TABLE IF EXISTS workers CASCADE;

	CREATE TABLE workers (
		id UUID PRIMARY KEY,
		name TEXT NOT NULL
	);

	CREATE TABLE genres (
		id UUID PRIMARY KEY,
		name TEXT UNIQUE NOT NULL
	);

	CREATE TABLE awards (
		id UUID PRIMARY KEY,
		name TEXT NOT NULL,
		category TEXT NOT NULL
	);

	CREATE TABLE movies (
		id UUID PRIMARY KEY,
		title TEXT NOT NULL,
		year INT,
		director_id UUID REFERENCES workers(id),
		metadata JSONB
	);

	CREATE TABLE movies_genres (
		movie_id UUID REFERENCES movies(id),
		genre_id UUID REFERENCES genres(id),
		PRIMARY KEY (movie_id, genre_id)
	);

	CREATE TABLE movies_actors (
		id UUID PRIMARY KEY,
		movie_id UUID REFERENCES movies(id),
		actor_id UUID REFERENCES workers(id),
		role TEXT
	);

	CREATE TABLE movies_awards (
		movie_id UUID REFERENCES movies(id),
		award_id UUID REFERENCES awards(id),
		year INT,
		PRIMARY KEY (movie_id, award_id)
	);

	CREATE TABLE users (
		id UUID PRIMARY KEY,
		name TEXT NOT NULL
	);

	CREATE TABLE favorite_genres (
		user_id UUID REFERENCES users(id),
		genre_id UUID REFERENCES genres(id),
		PRIMARY KEY (user_id, genre_id)
	);

	CREATE TABLE watch_history (
		id UUID PRIMARY KEY,
		user_id UUID REFERENCES users(id),
		movie_id UUID REFERENCES movies(id),
		watched_on DATE
	);

	CREATE TABLE ratings (
		user_id UUID REFERENCES users(id),
		movie_id UUID REFERENCES movies(id),
		rating NUMERIC CHECK (rating >= 0 AND rating <= 5),
		review TEXT,
		PRIMARY KEY (user_id, movie_id)
	);
	`
	_, err := pool.Exec(ctx, ddl)
	return err
}

func createPostgresIndexes(ctx context.Context, pool *pgxpool.Pool) error {
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title)",
		"CREATE INDEX IF NOT EXISTS idx_ratings_rating ON ratings(rating)",
		"CREATE INDEX IF NOT EXISTS idx_ratings_movie_id ON ratings(movie_id)",
		"CREATE INDEX IF NOT EXISTS idx_watch_history_user_movie ON watch_history(user_id, movie_id)",
		"CREATE INDEX IF NOT EXISTS idx_movies_actors_movie_actor ON movies_actors(movie_id, actor_id)",
		// Utile pour Q1 et S1
		"CREATE INDEX IF NOT EXISTS idx_ratings_movie_rating ON ratings(movie_id, rating)",
		// Utile pour Q3
		"CREATE INDEX IF NOT EXISTS idx_movies_actors_actor_movie ON movies_actors(actor_id, movie_id)",
		"CREATE INDEX IF NOT EXISTS idx_movies_genres_movie ON movies_genres(movie_id, genre_id)",
		"CREATE INDEX IF NOT EXISTS idx_movies_genres_genre ON movies_genres(genre_id)",
		// Utile pour Q2
		"CREATE INDEX IF NOT EXISTS idx_movies_awards_movie ON movies_awards(movie_id)",
	}
	for _, ddl := range indexes {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("index %q: %w", ddl, err)
		}
	}
	return nil
}

// petit wrapper autour de COPY
func copyPg(ctx context.Context, pool *pgxpool.Pool, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	copyRows := make([][]interface{}, len(rows))
	copy(copyRows, rows)

	_, err := pool.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromRows(copyRows))
	return err
}

// charge les csv dans postgres
// si un lot casse, on repasse ligne par ligne pour savoir quoi logguer
func loadPgCSV(ctx context.Context, pool *pgxpool.Pool, csvDir string, errLog *ErrorLogger) error {
	type tableSpec struct {
		file    string
		table   string
		columns []string
		parse   func([]string) ([]interface{}, error)
	}

	// fichier -> table
	// le header donne les colonnes, parse s'occupe des lignes
	specs := []tableSpec{
		{
			file: "workers.csv", table: "workers", columns: []string{"id", "name"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 2 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1]}, nil
			},
		},
		{
			file: "genres.csv", table: "genres", columns: []string{"id", "name"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 2 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1]}, nil
			},
		},
		{
			file: "awards.csv", table: "awards", columns: []string{"id", "name", "category"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 3 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1], r[2]}, nil
			},
		},
		{
			file: "users.csv", table: "users", columns: []string{"id", "name"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 2 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1]}, nil
			},
		},
		{
			file: "movies.csv", table: "movies", columns: []string{"id", "title", "year", "director_id", "metadata"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 4 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				year, err := strconv.Atoi(r[2])
				if err != nil {
					return nil, fmt.Errorf("année invalide: %v", err)
				}
				var metadata interface{}
				if len(r) >= 5 && r[4] != "" {
					metadata = r[4]
				}
				directorID := r[3]
				if directorID == "" {
					return nil, fmt.Errorf("director_id manquant")
				}
				return []interface{}{r[0], r[1], year, directorID, metadata}, nil
			},
		},
		{
			file: "movies_genres.csv", table: "movies_genres", columns: []string{"movie_id", "genre_id"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 2 || r[0] == "" || r[1] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1]}, nil
			},
		},
		{
			file: "movies_actors.csv", table: "movies_actors", columns: []string{"id", "movie_id", "actor_id", "role"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 4 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1], r[2], r[3]}, nil
			},
		},
		{
			file: "movies_awards.csv", table: "movies_awards", columns: []string{"movie_id", "award_id", "year"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 3 || r[0] == "" || r[1] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				year, err := strconv.Atoi(r[2])
				if err != nil {
					return nil, fmt.Errorf("année invalide: %v", err)
				}
				return []interface{}{r[0], r[1], year}, nil
			},
		},
		{
			file: "favorite_genres.csv", table: "favorite_genres", columns: []string{"user_id", "genre_id"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 2 || r[0] == "" || r[1] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1]}, nil
			},
		},
		{
			file: "watch_history.csv", table: "watch_history", columns: []string{"id", "user_id", "movie_id", "watched_on"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 4 || r[0] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				return []interface{}{r[0], r[1], r[2], r[3]}, nil
			},
		},
		{
			file: "ratings.csv", table: "ratings", columns: []string{"user_id", "movie_id", "rating", "review"},
			parse: func(r []string) ([]interface{}, error) {
				if len(r) < 3 || r[0] == "" || r[1] == "" {
					return nil, fmt.Errorf("champs manquants")
				}
				rating, err := strconv.ParseFloat(r[2], 64)
				if err != nil {
					return nil, fmt.Errorf("note invalide: %v", err)
				}
				if rating < 0 || rating > 5 {
					return nil, fmt.Errorf("note hors plage: %v", rating)
				}
				var review interface{}
				if len(r) >= 4 && r[3] != "" {
					review = r[3]
				}
				return []interface{}{r[0], r[1], rating, review}, nil
			},
		},
	}

	for _, spec := range specs {
		path := filepath.Join(csvDir, spec.file)
		start := time.Now()

		reader, f, err := openCSV(path)
		if err != nil {
			return err
		}

		// on saute l'entête
		if _, err := reader.Read(); err != nil {
			f.Close()
			return fmt.Errorf("lecture entête %s: %w", spec.file, err)
		}

		batch := make([][]interface{}, 0, pgBatchSize)
		total := 0

		for {
			record, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				errLog.Log("pg/"+spec.file, "", fmt.Sprintf("lecture csv: %v", err))
				continue
			}

			row, parseErr := spec.parse(record)
			if parseErr != nil {
				errLog.Log("pg/"+spec.file, strings.Join(record, ","), parseErr.Error())
				continue
			}

			batch = append(batch, row)
			if len(batch) >= pgBatchSize {
				if err := copyPg(ctx, pool, spec.table, spec.columns, batch); err != nil {
					// si COPY casse, on refait ligne par ligne
					for _, singleRow := range batch {
						if err2 := copyPg(ctx, pool, spec.table, spec.columns, [][]interface{}{singleRow}); err2 != nil {
							errLog.Log("pg/"+spec.file, fmt.Sprintf("%v", singleRow), err2.Error())
						} else {
							total++
						}
					}
				} else {
					total += len(batch)
				}
				batch = batch[:0]
			}
		}
		// on envoie le dernier bout
		if len(batch) > 0 {
			if err := copyPg(ctx, pool, spec.table, spec.columns, batch); err != nil {
				for _, singleRow := range batch {
					if err2 := copyPg(ctx, pool, spec.table, spec.columns, [][]interface{}{singleRow}); err2 != nil {
						errLog.Log("pg/"+spec.file, fmt.Sprintf("%v", singleRow), err2.Error())
					} else {
						total++
					}
				}
			} else {
				total += len(batch)
			}
		}

		f.Close()
		log.Printf("[pg] %s: %d lignes en %v", spec.table, total, time.Since(start))
	}
	return nil
}

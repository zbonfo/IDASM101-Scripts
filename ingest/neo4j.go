package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4J Go driver
// https://neo4j.com/docs/go-manual/current/

const neo4jBatchSize = 5000

//	index simples pour les recherches courantes
//
// ref: https://neo4j.com/docs/cypher-manual/current/indexes/search-performance-indexes/create-indexes/#create-a-single-property-range-index-for-nodes
func createNeo4jIndexes(ctx context.Context, driver neo4j.DriverWithContext) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	indexes := []string{
		"CREATE INDEX IF NOT EXISTS FOR (m:Movie) ON (m.title)", // pour la recherche par titre
		"CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.name)", // pour la recherche par nom
	}
	for _, cypher := range indexes {
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			_, err := tx.Run(ctx, cypher, nil)
			return nil, err
		})
		if err != nil {
			return fmt.Errorf("index %q: %w", cypher, err)
		}
	}
	return nil
}

func runNeoBatch(ctx context.Context, session neo4j.SessionWithContext, cypher string, paramsList []map[string]interface{}) error {
	if len(paramsList) == 0 {
		return nil
	}
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(ctx, cypher, map[string]interface{}{"batch": paramsList})
		return nil, err
	})
	return err
}

func loadNeo(ctx context.Context, driver neo4j.DriverWithContext, csvDir string, errLog *ErrorLogger) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	// on vide la base au départ
	log.Println("[neo] reset base")
	session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		tx.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
		return nil, nil
	})

	// contraintes pour éviter les doublons avec MERGE
	constraints := []string{
		"CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (a:Award) REQUIRE a.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
	}
	for _, c := range constraints {
		session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			_, err := tx.Run(ctx, c, nil)
			return nil, err
		})
	}

	// workers.csv -> noeuds Person
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "workers.csv"), errLog,
		"UNWIND $batch AS row MERGE (p:Person {id: row.id}) SET p.name = row.name",
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 2 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"id": record[0], "name": record[1]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j workers: %w", err)
	}

	// genres.csv -> noeuds Genre
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "genres.csv"), errLog,
		"UNWIND $batch AS row MERGE (g:Genre {id: row.id}) SET g.name = row.name",
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 2 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"id": record[0], "name": record[1]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j genres: %w", err)
	}

	// awards.csv -> noeuds Award
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "awards.csv"), errLog,
		"UNWIND $batch AS row MERGE (a:Award {id: row.id}) SET a.name = row.name, a.category = row.category",
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 3 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"id": record[0], "name": record[1], "category": record[2]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j awards: %w", err)
	}

	// users.csv -> noeuds User
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "users.csv"), errLog,
		"UNWIND $batch AS row MERGE (u:User {id: row.id}) SET u.name = row.name",
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 2 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"id": record[0], "name": record[1]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j users: %w", err)
	}

	// movies.csv -> films + relation DIRECTED
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "movies.csv"), errLog,
		`UNWIND $batch AS row
		 MERGE (m:Movie {id: row.id})
		 SET m.title = row.title, m.year = row.year
		 WITH m, row
		 MATCH (p:Person {id: row.director_id})
		 MERGE (p)-[:DIRECTED]->(m)`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 4 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			year, err := strconv.Atoi(record[2])
			if err != nil {
				return nil, fmt.Errorf("année invalide: %v", err)
			}
			return map[string]interface{}{
				"id": record[0], "title": record[1], "year": year, "director_id": record[3],
			}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j movies: %w", err)
	}

	// movies_genres.csv -> relations HAS_GENRE
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "movies_genres.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (m:Movie {id: row.movie_id})
		 MATCH (g:Genre {id: row.genre_id})
		 MERGE (m)-[:HAS_GENRE]->(g)`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 2 || record[0] == "" || record[1] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"movie_id": record[0], "genre_id": record[1]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j movies_genres: %w", err)
	}

	// movies_actors.csv -> relations ACTED_IN
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "movies_actors.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (p:Person {id: row.actor_id})
		 MATCH (m:Movie {id: row.movie_id})
		 MERGE (p)-[:ACTED_IN {role: row.role}]->(m)`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 4 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{
				"movie_id": record[1], "actor_id": record[2], "role": record[3],
			}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j movies_actors: %w", err)
	}

	// movies_awards.csv -> relations WON_AWARD
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "movies_awards.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (m:Movie {id: row.movie_id})
		 MATCH (a:Award {id: row.award_id})
		 MERGE (m)-[:WON_AWARD {year: row.year}]->(a)`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 3 || record[0] == "" || record[1] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			year, err := strconv.Atoi(record[2])
			if err != nil {
				return nil, fmt.Errorf("année invalide: %v", err)
			}
			return map[string]interface{}{"movie_id": record[0], "award_id": record[1], "year": year}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j movies_awards: %w", err)
	}

	// favorite_genres.csv -> relations PREFERS
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "favorite_genres.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (u:User {id: row.user_id})
		 MATCH (g:Genre {id: row.genre_id})
		 MERGE (u)-[:PREFERS]->(g)`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 2 || record[0] == "" || record[1] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{"user_id": record[0], "genre_id": record[1]}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j favorite_genres: %w", err)
	}

	// watch_history.csv -> relations WATCHED
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "watch_history.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (u:User {id: row.user_id})
		 MATCH (m:Movie {id: row.movie_id})
		 MERGE (u)-[w:WATCHED {id: row.id}]->(m)
		 SET w.watched_on = row.watched_on`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 4 || record[0] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			return map[string]interface{}{
				"id": record[0], "user_id": record[1], "movie_id": record[2], "watched_on": record[3],
			}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j watch_history: %w", err)
	}

	// ratings.csv -> relations RATED
	if err := loadNeoCSV(ctx, session, filepath.Join(csvDir, "ratings.csv"), errLog,
		`UNWIND $batch AS row
		 MATCH (u:User {id: row.user_id})
		 MATCH (m:Movie {id: row.movie_id})
		 MERGE (u)-[r:RATED]->(m)
		 ON CREATE SET r.rating = row.rating, r.review = row.review`,
		func(record []string) (map[string]interface{}, error) {
			if len(record) < 3 || record[0] == "" || record[1] == "" {
				return nil, fmt.Errorf("champs manquants")
			}
			rating, err := strconv.ParseFloat(record[2], 64)
			if err != nil {
				return nil, fmt.Errorf("note invalide: %v", err)
			}
			if rating < 0 || rating > 5 {
				return nil, fmt.Errorf("note hors plage: %v", rating)
			}
			review := ""
			if len(record) >= 4 {
				review = record[3]
			}
			return map[string]interface{}{
				"user_id": record[0], "movie_id": record[1], "rating": rating, "review": review,
			}, nil
		},
	); err != nil {
		return fmt.Errorf("neo4j ratings: %w", err)
	}

	return nil
}

// charge un csv dans neo4j avec UNWIND par lots
func loadNeoCSV(ctx context.Context, session neo4j.SessionWithContext, path string, errLog *ErrorLogger,
	cypher string, parse func([]string) (map[string]interface{}, error)) error {

	reader, f, err := openCSV(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// on saute l'entête
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("lecture entête: %w", err)
	}

	batch := make([]map[string]interface{}, 0, neo4jBatchSize)
	total := 0
	fileName := filepath.Base(path)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errLog.Log("neo4j/"+fileName, "", fmt.Sprintf("lecture csv: %v", err))
			continue
		}

		params, parseErr := parse(record)
		if parseErr != nil {
			errLog.Log("neo4j/"+fileName, strings.Join(record, ","), parseErr.Error())
			continue
		}

		batch = append(batch, params)
		if len(batch) >= neo4jBatchSize {
			if err := runNeoBatch(ctx, session, cypher, batch); err != nil {
				errLog.Log("neo4j/"+fileName, "", fmt.Sprintf("lot neo4j: %v", err))
			} else {
				total += len(batch)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := runNeoBatch(ctx, session, cypher, batch); err != nil {
			errLog.Log("neo4j/"+fileName, "", fmt.Sprintf("lot neo4j: %v", err))
		} else {
			total += len(batch)
		}
	}

	log.Printf("[neo] %s: %d lignes", fileName, total)
	return nil
}

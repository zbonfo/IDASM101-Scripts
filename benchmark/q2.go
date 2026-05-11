package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Q2 : fiche film complète sur plusieurs films

func q2PG(ctx context.Context, pool *pgxpool.Pool, warmupMovieID string, movieIDs []string) (BenchResult, error) {
	runOne := func(movieID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		/*language=sql*/
		row := pool.QueryRow(queryCtx, `
			SELECT m.id, m.title, m.year,
			  CASE
			    WHEN w.id IS NULL THEN NULL
			    ELSE jsonb_build_object('id', w.id, 'name', w.name)
			  END AS director,
			  COALESCE(actor_data.actors, '[]'::jsonb) AS actors,
			  COALESCE(award_data.awards, '[]'::jsonb) AS awards
			FROM movies m
			LEFT JOIN workers w ON w.id = m.director_id
			LEFT JOIN LATERAL (
				SELECT jsonb_agg(
					jsonb_build_object('id', wa.id, 'name', wa.name, 'role', ma.role)
					ORDER BY wa.id
				) AS actors
				FROM movies_actors ma
				JOIN workers wa ON wa.id = ma.actor_id
				WHERE ma.movie_id = m.id
			) actor_data ON true
			LEFT JOIN LATERAL (
				SELECT jsonb_agg(
					jsonb_build_object('id', a.id, 'name', a.name, 'category', a.category, 'year', maw.year)
					ORDER BY a.id, maw.year
				) AS awards
				FROM movies_awards maw
				JOIN awards a ON a.id = maw.award_id
				WHERE maw.movie_id = m.id
			) award_data ON true
			WHERE m.id = $1`, movieID)

		var id, title string
		var year int
		var director, actors, awards interface{}
		if err := row.Scan(&id, &title, &year, &director, &actors, &awards); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("film %s introuvable", movieID)
			}
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupMovieID); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL Q2 warmup: %w", err)
	}
	return benchN("PostgreSQL", "Q2", len(movieIDs), func(i int) error { return runOne(movieIDs[i]) })
}

func q2MG(ctx context.Context, db *mongo.Database, warmupMovieID string, movieIDs []string) (BenchResult, error) {
	runOne := func(movieID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		var result bson.M
		err := db.Collection("movies").FindOne(
			queryCtx,
			bson.D{{Key: "_id", Value: movieID}},
			options.FindOne().SetProjection(bson.D{
				{Key: "_id", Value: 1},
				{Key: "title", Value: 1},
				{Key: "year", Value: 1},
				{Key: "director", Value: 1},
				{Key: "actors", Value: 1},
				{Key: "awards", Value: 1},
			}),
		).Decode(&result)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return fmt.Errorf("film %s introuvable", movieID)
			}
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupMovieID); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB Q2 warmup: %w", err)
	}
	return benchN("MongoDB", "Q2", len(movieIDs), func(i int) error { return runOne(movieIDs[i]) })
}

func q2N4(ctx context.Context, d neo4j.DriverWithContext, warmupMovieID string, movieIDs []string) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	runOne := func(movieID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			res, err := tx.Run(queryCtx, `
				MATCH (m:Movie {id: $id})
				OPTIONAL MATCH (dir:Person)-[:DIRECTED]->(m)
				WITH m, dir
				OPTIONAL MATCH (act:Person)-[r:ACTED_IN]->(m)
				WITH m, dir,
				  collect(DISTINCT CASE WHEN act IS NULL THEN NULL ELSE {id: act.id, name: act.name, role: r.role} END) AS raw_actors
				OPTIONAL MATCH (m)-[wa:WON_AWARD]->(aw:Award)
				WITH m, dir, raw_actors,
				  collect(DISTINCT CASE WHEN aw IS NULL THEN NULL ELSE {id: aw.id, name: aw.name, category: aw.category, year: wa.year} END) AS raw_awards
				RETURN m.id AS id, m.title AS title, m.year AS year,
				  CASE WHEN dir IS NULL THEN NULL ELSE {id: dir.id, name: dir.name} END AS director,
				  [actor IN raw_actors WHERE actor IS NOT NULL] AS actors,
				  [award IN raw_awards WHERE award IS NOT NULL] AS awards`,
				map[string]interface{}{"id": movieID})
			if err != nil {
				return nil, err
			}

			found := false
			err = consumeNeo4jResult(queryCtx, res, func(record *neo4j.Record) error {
				found = true
				if _, ok := record.Get("id"); !ok {
					return fmt.Errorf("champ id absent")
				}
				if _, ok := record.Get("title"); !ok {
					return fmt.Errorf("champ title absent")
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("film %s introuvable", movieID)
			}
			return nil, nil
		})
		return err
	}

	if err := runOne(warmupMovieID); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j Q2 warmup: %w", err)
	}
	return benchN("Neo4j", "Q2", len(movieIDs), func(i int) error { return runOne(movieIDs[i]) })
}

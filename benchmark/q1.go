package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Q1 : films avec une moyenne > 4.5

func q1PG(ctx context.Context, pool *pgxpool.Pool, runs int) (BenchResult, error) {
	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		/*language=sql*/
		rows, err := pool.Query(queryCtx, `
		  SELECT m.id, m.title, AVG(r.rating) AS avg_rating
		  FROM movies m
		  JOIN ratings r ON m.id = r.movie_id
		  GROUP BY m.id, m.title, m.year
		  HAVING AVG(r.rating) > 4.5
		  ORDER BY avg_rating DESC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, title string
			var avg float64
			if err := rows.Scan(&id, &title, &avg); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL Q1 warmup: %w", err)
	}
	return benchSame("PostgreSQL", "Q1", runs, run)
}

func q1MG(ctx context.Context, db *mongo.Database, runs int) (BenchResult, error) {
	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		cur, err := db.Collection("ratings").Aggregate(queryCtx, bson.A{
			bson.D{{Key: "$sort", Value: bson.D{{Key: "movie_id", Value: 1}, {Key: "rating", Value: 1}}}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$movie_id"}, {Key: "avg_rating", Value: bson.D{{Key: "$avg", Value: "$rating"}}}}}},
			bson.D{{Key: "$match", Value: bson.D{{Key: "avg_rating", Value: bson.D{{Key: "$gt", Value: 4.5}}}}}},
			bson.D{{Key: "$lookup", Value: bson.D{{Key: "from", Value: "movies"}, {Key: "localField", Value: "_id"}, {Key: "foreignField", Value: "_id"}, {Key: "as", Value: "movie"}}}},
			bson.D{{Key: "$unwind", Value: "$movie"}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "id", Value: "$movie._id"}, {Key: "title", Value: "$movie.title"}, {Key: "avg_rating", Value: 1}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "avg_rating", Value: -1}}}},
		}, options.Aggregate().SetAllowDiskUse(true).SetHint(bson.D{{Key: "movie_id", Value: 1}, {Key: "rating", Value: 1}}))
		if err != nil {
			return err
		}
		if err := consumeMongoCursor(queryCtx, cur); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB Q1 warmup: %w", err)
	}
	return benchSame("MongoDB", "Q1", runs, run)
}

func q1N4(ctx context.Context, d neo4j.DriverWithContext, runs int) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	run := func() error {
		queryCtx, cancel := context.WithTimeout(ctx, globalQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			res, err := tx.Run(queryCtx, `
				MATCH (u:User)-[r:RATED]->(m:Movie)
				WITH m, AVG(r.rating) AS avg_rating WHERE avg_rating > 4.5
				RETURN m.id AS id, m.title AS title, avg_rating
				ORDER BY avg_rating DESC`, nil)
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
				if _, ok := record.Get("avg_rating"); !ok {
					return fmt.Errorf("champ avg_rating absent du résultat")
				}
				return nil
			})
		})
		return err
	}

	if err := run(); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j Q1 warmup: %w", err)
	}
	return benchSame("Neo4j", "Q1", runs, run)
}

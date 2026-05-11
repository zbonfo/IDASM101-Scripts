package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Q3 : films pas vus par un utilisateur mais avec au moins un acteur déjà vu

func q3PG(ctx context.Context, pool *pgxpool.Pool, warmupUserID string, uids []string) (BenchResult, error) {
	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		/*language=sql*/
		rows, err := pool.Query(queryCtx, `
			SELECT m2.id, m2.title
			FROM watch_history wh
			JOIN movies_actors ma1 ON wh.movie_id = ma1.movie_id
			JOIN movies_actors ma2 ON ma1.actor_id = ma2.actor_id
			JOIN movies m2 ON ma2.movie_id = m2.id
			WHERE wh.user_id = $1
			  AND NOT EXISTS (
				  SELECT 1
				  FROM watch_history wh2
				  WHERE wh2.user_id = wh.user_id
				    AND wh2.movie_id = m2.id
			  )
			GROUP BY m2.id, m2.title, m2.year
			ORDER BY m2.title ASC`, userID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, title string
			if err := rows.Scan(&id, &title); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return queryCtx.Err()
	}

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("PostgreSQL Q3 warmup: %w", err)
	}
	return benchN("PostgreSQL", "Q3", len(uids), func(i int) error { return runOne(uids[i]) })
}

func q3MG(ctx context.Context, db *mongo.Database, warmupUserID string, uids []string) (BenchResult, error) {
	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		var user struct {
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

		watchedIds := make([]string, 0, len(user.WatchHistory))
		for _, w := range user.WatchHistory {
			watchedIds = append(watchedIds, w.MovieID)
		}

		if len(watchedIds) == 0 {
			return nil
		}

		var actorsRes []string
		err = db.Collection("movies").Distinct(queryCtx, "actors.id", bson.M{"_id": bson.M{"$in": watchedIds}}).Decode(&actorsRes)
		if err != nil {
			return err
		}
		if len(actorsRes) == 0 {
			return nil
		}

		pipeline := bson.A{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "_id", Value: bson.D{{Key: "$nin", Value: watchedIds}}},
				{Key: "actors.id", Value: bson.D{{Key: "$in", Value: actorsRes}}},
			}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "title", Value: 1}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "id", Value: "$_id"}, {Key: "title", Value: 1}}}},
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

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("MongoDB Q3 warmup: %w", err)
	}
	return benchN("MongoDB", "Q3", len(uids), func(i int) error { return runOne(uids[i]) })
}

func q3N4(ctx context.Context, d neo4j.DriverWithContext, warmupUserID string, uids []string) (BenchResult, error) {
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	runOne := func(userID string) error {
		queryCtx, cancel := context.WithTimeout(ctx, entityQueryTimeout)
		defer cancel()

		_, err := session.ExecuteRead(queryCtx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			/*language=cypher*/
			res, err := tx.Run(queryCtx, `
					MATCH (u:User {id:$uid})-[:WATCHED]->(:Movie)<-[:ACTED_IN]-(a:Person)
					WITH DISTINCT u, a
					MATCH (a)-[:ACTED_IN]->(rec:Movie)
					WHERE NOT (u)-[:WATCHED]->(rec)
					RETURN DISTINCT rec.id AS id, rec.title AS title
					ORDER BY title ASC`, map[string]interface{}{"uid": userID})
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
				return nil
			})
		})
		return err
	}

	if err := runOne(warmupUserID); err != nil {
		return BenchResult{}, fmt.Errorf("Neo4j Q3 warmup: %w", err)
	}
	return benchN("Neo4j", "Q3", len(uids), func(i int) error { return runOne(uids[i]) })
}

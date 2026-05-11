package main

import (
	"bigdata-ingestion/internal/dbcheck"
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ErrorLogger struct {
	file   *os.File
	writer *bufio.Writer
	count  int
}

// log à part pour les lignes qui ne passent pas
func newErrLogger(path string) (*ErrorLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &ErrorLogger{file: f, writer: bufio.NewWriter(f)}, nil
}

func (el *ErrorLogger) Log(source, line, reason string) {
	el.count++
	fmt.Fprintf(el.writer, "[%s] %s | raison: %s\n", source, line, reason)
}

func (el *ErrorLogger) Close() {
	el.writer.Flush()
	el.file.Close()
}

// connexions db

func openPg(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := "postgres://admin:password@localhost:5432/mydatabase?sslmode=disable"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

func openMongo(ctx context.Context) (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	return client, nil
}

func openNeo() (neo4j.DriverWithContext, error) {
	driver, err := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "supersecretpassword", ""))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		return nil, fmt.Errorf("connexion neo4j: %w", err)
	}
	return driver, nil
}

// vérifier à la fin pour voir si les compteurs collent
func checkLoaded(ctx context.Context, includePg, includeMongo, includeNeo4j bool) error {
	counts := make(map[string]dbcheck.Counts)

	if includePg {
		pool, err := openPg(ctx)
		if err != nil {
			return fmt.Errorf("connexion postgres pour la vérif: %w", err)
		}
		defer pool.Close()

		count, err := dbcheck.FetchPostgresCounts(ctx, pool)
		if err != nil {
			return err
		}
		counts["PostgreSQL"] = count
	}

	if includeMongo {
		client, err := openMongo(ctx)
		if err != nil {
			return fmt.Errorf("connexion mongo pour la vérif: %w", err)
		}
		defer client.Disconnect(ctx)

		count, err := dbcheck.FetchMongoCounts(ctx, client)
		if err != nil {
			return err
		}
		counts["MongoDB"] = count
	}

	if includeNeo4j {
		driver, err := openNeo()
		if err != nil {
			return fmt.Errorf("connexion neo4j pour la vérif: %w", err)
		}
		defer driver.Close(ctx)

		count, err := dbcheck.FetchNeo4jCounts(ctx, driver)
		if err != nil {
			return err
		}
		counts["Neo4j"] = count
	}

	if err := dbcheck.CompareAll(counts); err != nil {
		return err
	}
	if len(counts) == 1 {
		log.Println("Vérif OK")
	}
	if len(counts) > 1 {
		log.Println("Vérif OK entre les bases choisies")
	}

	return nil
}

// Point d'entrée

func main() {
	size := flag.String("size", "small", "taille du dataset: small, medium ou large")
	skipPg := flag.Bool("skip-pg", false, "saute l'import PostgreSQL")
	skipMongo := flag.Bool("skip-mongo", false, "saute l'import MongoDB")
	skipNeo4j := flag.Bool("skip-neo4j", false, "saute l'import Neo4j")
	indexOnly := flag.Bool("index-only", false, "fait juste les index")
	validateOnly := flag.Bool("validate-only", false, "fait juste la vérif des compteurs")
	flag.Parse()

	if *size != "small" && *size != "medium" && *size != "large" {
		log.Fatalf("taille %q invalide: small, medium ou large", *size)
	}

	ctx := context.Background()
	totalStart := time.Now()

	if *indexOnly && *validateOnly {
		log.Fatalf("prends soit -index-only soit -validate-only")
	}

	if *validateOnly {
		if err := checkLoaded(ctx, !*skipPg, !*skipMongo, !*skipNeo4j); err != nil {
			log.Fatalf("vérif: %v", err)
		}
		log.Printf("Vérif finie en %v", time.Since(totalStart))
		return
	}

	// juste les index, puis on s'arrête
	if *indexOnly {
		log.Println("Mode index seulement")
		if !*skipPg {
			pool, err := openPg(ctx)
			if err != nil {
				log.Fatalf("connexion pg: %v", err)
			}
			defer pool.Close()
			if err := createPostgresIndexes(ctx, pool); err != nil {
				log.Fatalf("index pg: %v", err)
			}
			log.Println("[pg] index ok")
		}
		if !*skipMongo {
			mc, err := openMongo(ctx)
			if err != nil {
				log.Fatalf("connexion mongo: %v", err)
			}
			defer mc.Disconnect(ctx)
			if err := createMongoIndexes(ctx, mc); err != nil {
				log.Fatalf("index mongo: %v", err)
			}
			log.Println("[mongo] index ok")
		}
		if !*skipNeo4j {
			nd, err := openNeo()
			if err != nil {
				log.Fatalf("connexion neo4j: %v", err)
			}
			defer nd.Close(ctx)
			if err := createNeo4jIndexes(ctx, nd); err != nil {
				log.Fatalf("index neo4j: %v", err)
			}
			log.Println("[neo] index ok")
		}
		log.Printf("Index faits en %v", time.Since(totalStart))
		return
	}

	baseDir := filepath.Join("..", "generated_data", *size)
	csvDir := filepath.Join(baseDir, "postgresql&neo4j")
	jsonDir := filepath.Join(baseDir, "mongodb")

	// vérifie que les dossiers sont là
	if _, err := os.Stat(csvDir); os.IsNotExist(err) {
		log.Fatalf("dossier csv introuvable: %s", csvDir)
	}
	if _, err := os.Stat(jsonDir); os.IsNotExist(err) {
		log.Fatalf("dossier json introuvable: %s", jsonDir)
	}

	// fichier à part pour les erreurs d'import
	errLogPath := fmt.Sprintf("errors_%s.log", *size)
	errLog, err := newErrLogger(errLogPath)
	if err != nil {
		log.Fatalf("création du log d'erreurs: %v", err)
	}
	defer func() {
		errLog.Close()
		if errLog.count > 0 {
			log.Printf("Erreurs dans %s (%d)", errLogPath, errLog.count)
		}
	}()

	log.Printf("Import du dataset %s", *size)

	// PostgreSQL
	if !*skipPg {
		log.Println("[pg] import")
		pool, err := openPg(ctx)
		if err != nil {
			log.Fatalf("connexion pg: %v", err)
		}
		defer pool.Close()

		if err := createPostgresSchema(ctx, pool); err != nil {
			log.Fatalf("schema pg: %v", err)
		}

		pgStart := time.Now()
		if err := loadPgCSV(ctx, pool, csvDir, errLog); err != nil {
			log.Fatalf("import pg: %v", err)
		}
		if err := createPostgresIndexes(ctx, pool); err != nil {
			log.Fatalf("index pg: %v", err)
		}
		log.Printf("[pg] fini en %v", time.Since(pgStart))
	}

	// MongoDB
	if !*skipMongo {
		log.Println("[mongo] import")
		mongoClient, err := openMongo(ctx)
		if err != nil {
			log.Fatalf("connexion mongo: %v", err)
		}
		defer mongoClient.Disconnect(ctx)

		mongoStart := time.Now()
		if err := loadMongo(ctx, mongoClient, jsonDir, errLog); err != nil {
			log.Fatalf("import mongo: %v", err)
		}
		if err := createMongoIndexes(ctx, mongoClient); err != nil {
			log.Fatalf("index mongo: %v", err)
		}
		log.Printf("[mongo] fini en %v", time.Since(mongoStart))
	}

	// Neo4j
	if !*skipNeo4j {
		log.Println("[neo] import")
		neo4jDriver, err := openNeo()
		if err != nil {
			log.Fatalf("connexion neo4j: %v", err)
		}
		defer neo4jDriver.Close(ctx)

		neo4jStart := time.Now()
		if err := loadNeo(ctx, neo4jDriver, csvDir, errLog); err != nil {
			log.Fatalf("import neo4j: %v", err)
		}
		if err := createNeo4jIndexes(ctx, neo4jDriver); err != nil {
			log.Fatalf("index neo4j: %v", err)
		}
		log.Printf("[neo] fini en %v", time.Since(neo4jStart))
	}

	if err := checkLoaded(ctx, !*skipPg, !*skipMongo, !*skipNeo4j); err != nil {
		log.Fatalf("vérif après import: %v", err)
	}

	log.Printf("Terminé en %v", time.Since(totalStart))
}

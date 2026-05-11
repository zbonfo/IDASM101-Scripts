package main

import (
	"bigdata-ingestion/internal/dbcheck"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// on fait un passage à vide avant chaque bench

const (
	defaultSampleSize = 100
	defaultNbRuns     = 2
)

const (
	globalQueryTimeout = 3 * time.Minute
	entityQueryTimeout = 30 * time.Second
)

// connexions aux bases

func connectPG(ctx context.Context) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, "postgres://admin:password@localhost:5432/mydatabase?sslmode=disable")
	if err != nil {
		log.Fatalf("connexion pg: %v", err)
	}
	return pool
}

func connectMG(ctx context.Context) *mongo.Client {
	c, err := mongo.Connect(options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		log.Fatalf("connexion mongo: %v", err)
	}
	return c
}

func connectN4J() neo4j.DriverWithContext {
	d, err := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "supersecretpassword", ""))
	if err != nil {
		log.Fatalf("connexion neo4j: %v", err)
	}
	return d
}

// échantillonnage et vérif

// prend les n premières strings renvoyées par une requête
func takeStrings(ctx context.Context, pool *pgxpool.Pool, query string, n int) []string {
	rows, err := pool.Query(ctx, query, n)
	if err != nil {
		log.Fatalf("requête d'échantillon: %v", err)
	}
	defer rows.Close()

	out := make([]string, 0, n)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			log.Fatalf("scan échantillon: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("parcours échantillon: %v", err)
	}
	if len(out) == 0 {
		log.Fatalf("la requête d'échantillon n'a rien renvoyé: %s", query)
	}
	return out
}

// prend une valeur pour le passage à vide et garde le reste pour le bench
func takeWarmAndRest(ctx context.Context, pool *pgxpool.Pool, query string, n int) (string, []string) {
	samples := takeStrings(ctx, pool, query, n+1)
	if len(samples) < n+1 {
		log.Fatalf("échantillon trop petit: %d lignes, il en faut au moins %d", len(samples), n+1)
	}
	return samples[0], samples[1 : n+1]
}

// vérifier avant de lancer les benchmakrs
func checkBenchData(ctx context.Context, pool *pgxpool.Pool, mg *mongo.Client, n4j neo4j.DriverWithContext) error {
	counts := make(map[string]dbcheck.Counts)

	pgCounts, err := dbcheck.FetchPostgresCounts(ctx, pool)
	if err != nil {
		return err
	}
	counts["PostgreSQL"] = pgCounts

	mongoCounts, err := dbcheck.FetchMongoCounts(ctx, mg)
	if err != nil {
		return err
	}
	counts["MongoDB"] = mongoCounts

	neo4jCounts, err := dbcheck.FetchNeo4jCounts(ctx, n4j)
	if err != nil {
		return err
	}
	counts["Neo4j"] = neo4jCounts

	for _, name := range []string{"PostgreSQL", "MongoDB", "Neo4j"} {
		fmt.Printf("[vérif] %-10s %s\n", name, counts[name].String())
	}

	if err := dbcheck.CompareAll(counts); err != nil {
		return err
	}
	return nil
}

// outils pour les benchmarks

type BenchResult struct {
	DB      string
	Query   string
	N       int           // nombre d'exécutions
	Average time.Duration // moyenne
	Median  time.Duration // mediane
	P5      time.Duration // 5e centile
	P95     time.Duration // 95e centile
}

type benchUnit struct {
	Name string                      // nom de la db
	Run  func() (BenchResult, error) // fonction de benchmark
}

// calcul des stats
func stats(durations []time.Duration) (avg, median, p5, p95 time.Duration) {
	n := len(durations)
	if n == 0 {
		return 0, 0, 0, 0
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	avg = sum / time.Duration(n)
	if n%2 == 0 {
		median = (durations[n/2-1] + durations[n/2]) / 2
	} else {
		median = durations[n/2]
	}
	p5 = durations[int(float64(n-1)*0.05)]
	p95 = durations[int(float64(n-1)*0.95)]
	return
}

// lance un bench n fois et sort les stats
// fn reçoit l'index pour pouvoir varier les données si besoin
func benchN(db, query string, n int, fn func(i int) error) (BenchResult, error) {
	if n <= 0 {
		return BenchResult{}, fmt.Errorf("%s %s: il faut au moins une exécution", db, query)
	}

	durations := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := fn(i); err != nil {
			return BenchResult{}, fmt.Errorf("%s %s essai %d/%d: %w", db, query, i+1, n, err)
		}
		durations[i] = time.Since(start)
	}
	avg, med, p5, p95 := stats(durations)
	return BenchResult{DB: db, Query: query, N: n, Average: avg, Median: med, P5: p5, P95: p95}, nil
}

// version simple quand on rejoue toujours la même requête
func benchSame(db, query string, n int, fn func() error) (BenchResult, error) {
	return benchN(db, query, n, func(_ int) error { return fn() })
}

// même benchs mais dans un ordre mélangé
func runMixed(benches []benchUnit) (map[string]BenchResult, error) {
	order := append([]benchUnit(nil), benches...)
	rand.New(rand.NewSource(time.Now().UnixNano())).Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})

	results := make(map[string]BenchResult, len(order))
	for _, bench := range order {
		log.Printf("[bench] %s...", bench.Name)
		started := time.Now()
		result, err := bench.Run()
		if err != nil {
			return nil, err
		}
		log.Printf("[bench] %s fini en %v", bench.Name, time.Since(started).Round(time.Millisecond))
		results[bench.Name] = result
	}
	return results, nil
}

func runDirect(benches []benchUnit) (map[string]BenchResult, error) {
	results := make(map[string]BenchResult, len(benches))
	for _, bench := range benches {
		log.Printf("[bench] %s...", bench.Name)
		started := time.Now()
		result, err := bench.Run()
		if err != nil {
			return nil, err
		}
		log.Printf("[bench] %s fini en %v", bench.Name, time.Since(started).Round(time.Millisecond))
		results[bench.Name] = result
	}
	return results, nil
}

func runBenches(benches []benchUnit, shuffle bool) (map[string]BenchResult, error) {
	if shuffle {
		return runMixed(benches)
	}
	return runDirect(benches)
}

func showBenchRes(results map[string]BenchResult, order []string) {
	for _, name := range order {
		if result, ok := results[name]; ok {
			result.Print()
		}
	}
}

func (r BenchResult) Print() {
	fmt.Printf("  %-12s %-6s  n=%-4d  moy=%-14s  med=%-14s  p5=%-14s  p95=%s\n",
		r.DB, r.Query, r.N,
		r.Average.Round(time.Microsecond),
		r.Median.Round(time.Microsecond),
		r.P5.Round(time.Microsecond),
		r.P95.Round(time.Microsecond))
}

// on vide le curseur pour compter aussi le temps de parcours
func consumeMongoCursor(ctx context.Context, cur *mongo.Cursor) error {
	defer cur.Close(ctx)
	var rows []bson.M // le contenu des docs n'est pas utilisé
	if err := cur.All(ctx, &rows); err != nil {
		return err
	}
	return nil
}

// pareil pour neo4j, avec une petite vérif sur chaque ligne
func consumeNeo4jResult(ctx context.Context, res neo4j.ResultWithContext, validate func(*neo4j.Record) error) error {
	for res.Next(ctx) {
		if err := validate(res.Record()); err != nil {
			return err
		}
	}
	return res.Err()
}

// Point d'entrée

func main() {
	sampleSize := flag.Int("sample-size", defaultSampleSize, "nombre d'utilisateurs, films et acteurs tirés pour les requêtes avec données")
	nbRuns := flag.Int("runs", defaultNbRuns, "nombre de passages pour les requêtes globales")
	shuffle := flag.Bool("shuffle", true, "mélange l'ordre des bases pour chaque requête")
	flag.Parse()

	if *sampleSize <= 0 {
		log.Fatalf("sample-size doit être positif")
	}
	if *nbRuns <= 0 {
		log.Fatalf("runs doit être positif")
	}

	ctx := context.Background()

	// on se connecte aux dbs
	pool := connectPG(ctx)
	defer pool.Close()
	mg := connectMG(ctx)
	defer mg.Disconnect(ctx)
	n4j := connectN4J()
	defer n4j.Close(ctx)
	mgDB := mg.Database("streaming")

	// vérif avant de commencer
	if err := checkBenchData(ctx, pool, mg, n4j); err != nil {
		log.Fatalf("vérif bench: %v", err)
	}
	fmt.Println()

	// on tire quelques ids pour les requêtes qui en ont besoin
	log.Printf("Tirage de %d utilisateurs, films et acteurs...", *sampleSize)
	userWarmupID, userIDs := takeWarmAndRest(ctx, pool, "SELECT id FROM users ORDER BY random() LIMIT $1", *sampleSize)
	movieWarmupID, movieIDs := takeWarmAndRest(ctx, pool, "SELECT id FROM movies ORDER BY random() LIMIT $1", *sampleSize)
	actorWarmupID, actorIDs := takeWarmAndRest(ctx, pool, "SELECT actor_id FROM (SELECT DISTINCT actor_id FROM movies_actors) t ORDER BY random() LIMIT $1", *sampleSize)

	fmt.Println("================================================================")
	fmt.Println(" BENCHMARKS - 6 requêtes x 3 bases")
	fmt.Printf(" Q1:             %d passages\n", *nbRuns)
	fmt.Printf(" Q2 (par film):  %d films\n", len(movieIDs))
	fmt.Printf(" Q3 (par utilisateur): %d utilisateurs\n", len(userIDs))
	fmt.Printf(" S1:             %d passages\n", *nbRuns)
	fmt.Printf(" S2 (par acteur): %d acteurs\n", len(actorIDs))
	fmt.Printf(" S3 (par utilisateur): %d utilisateurs\n", len(userIDs))
	fmt.Println("================================================================")
	fmt.Println()

	fmt.Printf("- Q1: films avec moyenne > 4.5 (%d passages) :\n", *nbRuns)
	q1Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return q1PG(ctx, pool, *nbRuns) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return q1MG(ctx, mgDB, *nbRuns) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return q1N4(ctx, n4j, *nbRuns) }},
	}
	q1Results, err := runBenches(q1Benches, *shuffle)
	if err != nil {
		log.Fatalf("Q1: %v", err)
	}
	showBenchRes(q1Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Printf("- Q2: fiche film complète (%d films) :\n", len(movieIDs))
	q2Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return q2PG(ctx, pool, movieWarmupID, movieIDs) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return q2MG(ctx, mgDB, movieWarmupID, movieIDs) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return q2N4(ctx, n4j, movieWarmupID, movieIDs) }},
	}
	q2Results, err := runBenches(q2Benches, *shuffle)
	if err != nil {
		log.Fatalf("Q2: %v", err)
	}
	showBenchRes(q2Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Printf("- Q3: films pas vus avec acteurs en commun (%d utilisateurs) :\n", len(userIDs))
	q3Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return q3PG(ctx, pool, userWarmupID, userIDs) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return q3MG(ctx, mgDB, userWarmupID, userIDs) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return q3N4(ctx, n4j, userWarmupID, userIDs) }},
	}
	q3Results, err := runBenches(q3Benches, *shuffle)
	if err != nil {
		log.Fatalf("Q3: %v", err)
	}
	showBenchRes(q3Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Println("================================================================")
	fmt.Println(" REQUÊTES BONUS")
	fmt.Println("================================================================")
	fmt.Println()

	fmt.Printf("- S1: 3 meilleurs films par genre (%d passages) :\n", *nbRuns)
	s1Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return s1PG(ctx, pool, *nbRuns) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return s1MG(ctx, mgDB, *nbRuns) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return s1N4(ctx, n4j, *nbRuns) }},
	}
	s1Results, err := runBenches(s1Benches, *shuffle)
	if err != nil {
		log.Fatalf("S1: %v", err)
	}
	showBenchRes(s1Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Printf("- S2: tous les films d'un acteur (%d acteurs) :\n", len(actorIDs))
	s2Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return s2PG(ctx, pool, actorWarmupID, actorIDs) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return s2MG(ctx, mgDB, actorWarmupID, actorIDs) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return s2N4(ctx, n4j, actorWarmupID, actorIDs) }},
	}
	s2Results, err := runBenches(s2Benches, *shuffle)
	if err != nil {
		log.Fatalf("S2: %v", err)
	}
	showBenchRes(s2Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Printf("- S3: recommandations par genres favoris (%d utilisateurs) :\n", len(userIDs))
	s3Benches := []benchUnit{
		{Name: "PostgreSQL", Run: func() (BenchResult, error) { return s3PG(ctx, pool, userWarmupID, userIDs) }},
		{Name: "MongoDB", Run: func() (BenchResult, error) { return s3MG(ctx, mgDB, userWarmupID, userIDs) }},
		{Name: "Neo4j", Run: func() (BenchResult, error) { return s3N4(ctx, n4j, userWarmupID, userIDs) }},
	}
	s3Results, err := runBenches(s3Benches, *shuffle)
	if err != nil {
		log.Fatalf("S3: %v", err)
	}
	showBenchRes(s3Results, []string{"PostgreSQL", "MongoDB", "Neo4j"})
	fmt.Println()

	fmt.Println("================================================================")
	fmt.Println(" FIN DES BENCHS")
	fmt.Println("================================================================")
}

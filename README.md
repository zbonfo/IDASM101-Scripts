# Projet Big Data - UNamur

Comparaison de performances entre PostgreSQL, MongoDB et Neo4j sur un dataset de streaming.

## Prérequis

- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- [Go 1.22+](https://go.dev/dl/)

## Démarrage

**1. Lancer les containers**

```bash
docker compose up -d
```

**2. Charger un dataset** (trois tailles disponibles : `small`, `medium`, `large`)

```bash
go run ./ingest -size small
```

**3. Lancer les benchmarks**

```bash
go run ./benchmark -sample-size 100 -runs 4 -shuffle=true
```

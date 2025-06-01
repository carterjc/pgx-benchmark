package main

import (
	"context"
	"log"
	"os"
	"testing"

	"pgx_benchmark/db"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type MyRow struct {
	ID    int32
	Value string
}

func generateRows(n int) []MyRow {
	rows := make([]MyRow, n)
	for i := range rows {
		rows[i] = MyRow{ID: int32(i), Value: "val"}
	}
	return rows
}

func setupDB(ctx context.Context, conn *pgxpool.Pool) {
	conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_test")
	conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_staging")
	conn.Exec(ctx, "CREATE TABLE benchmark_test (id INTEGER PRIMARY KEY, value TEXT)")
	conn.Exec(ctx, "CREATE TABLE benchmark_staging (id INTEGER PRIMARY KEY, value TEXT)")
}

func sqlcUpserts(ctx context.Context, q *db.Queries, rows []MyRow) error {
	for _, row := range rows {
		err := q.UpsertBenchmarkRow(ctx, db.UpsertBenchmarkRowParams{
			ID:    row.ID,
			Value: pgtype.Text{String: row.Value, Valid: true},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// stagingCopyUpsert does:
//  1. COPY FROM -> benchmark_staging
//  2. INSERT … ON CONFLICT DO NOTHING FROM benchmark_staging -> benchmark_test
//  3. TRUNCATE benchmark_staging
func stagingCopyUpsert(ctx context.Context, pool *pgxpool.Pool, q *db.Queries, rows []MyRow) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	copyRows := make([]db.BulkInsertBenchmarkStagingParams, len(rows))
	for i, r := range rows {
		copyRows[i] = db.BulkInsertBenchmarkStagingParams{ID: r.ID, Value: pgtype.Text{String: r.Value, Valid: true}}
	}

	if _, err := q.WithTx(tx).BulkInsertBenchmarkStaging(ctx, copyRows); err != nil {
		return err
	}

	if err := q.WithTx(tx).MergeAndTruncateBenchmarkStaging(ctx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func init() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("no .env")
	}
}

func BenchmarkSqlcUpsert(b *testing.B) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("no DATABASE_URL")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	setupDB(ctx, pool)
	q := db.New(pool)
	rows := generateRows(1000000)

	// 1) Benchmark plain sqlc Upsert
	b.Run("DirectUpsert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := sqlcUpserts(ctx, q, rows); err != nil {
				b.Fatal(err)
			}
		}
	})

	// 2) Benchmark temp table + COPY + merge
	b.Run("StagingCopyUpsert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := stagingCopyUpsert(ctx, pool, q, rows); err != nil {
				b.Fatal(err)
			}
		}
	})
}

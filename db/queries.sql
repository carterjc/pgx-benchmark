-- name: UpsertBenchmarkRow :exec
INSERT INTO benchmark_test (id, value)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
SET value = EXCLUDED.value;

-- name: BulkInsertBenchmarkStaging :copyfrom
INSERT INTO benchmark_staging (id, value)
VALUES ($1, $2);

-- name: MergeAndTruncateBenchmarkStaging :exec
DO $$
BEGIN
  INSERT INTO benchmark_test (id, value)
    SELECT id, value
      FROM benchmark_staging
      ON CONFLICT DO NOTHING;

  TRUNCATE benchmark_staging;
END $$;

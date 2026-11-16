package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestJobRecordsWithoutADatabaseReportNothing(t *testing.T) {
	r, err := NewJobRecords(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Statuses(t.Context(), []string{"x"})
	if err != nil || rows != nil {
		t.Errorf("Statuses = %v, %v; want nothing without PGHOST", rows, err)
	}
	runs, ckpts, err := r.History(t.Context(), "x")
	if err != nil || runs != nil || ckpts != nil {
		t.Errorf("History = %v, %v, %v; want nothing without PGHOST", runs, ckpts, err)
	}
}

// TestJobRecordsReadTheDataSchema runs against a PostgreSQL that has the Data repo db/ schema applied
// (001 · 003). Set BACKEND_TEST_PG=1 and the PG* variables to run it; CI skips it.
func TestJobRecordsReadTheDataSchema(t *testing.T) {
	if os.Getenv("BACKEND_TEST_PG") == "" {
		t.Skip("BACKEND_TEST_PG not set")
	}
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	t0 := time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)
	for _, q := range []string{
		`DELETE FROM data.checkpoints WHERE job_id = 'rt000001'`,
		`DELETE FROM data.job_progress WHERE job_id = 'rt000001'`,
		`DELETE FROM data.job_runs WHERE job_id = 'rt000001'`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(`INSERT INTO data.job_runs (job_id, generation, jobset_name, cluster, queue, gpus, strategy, submitted_at, started_at, ended_at, end_reason)
		VALUES ('rt000001', 1, 'job-rt000001-g0001', 'eks-a', 'queue-light', 2, 'fsdp', $1, $2, $3, 'fenced'),
		       ('rt000001', 2, 'job-rt000001-g0002', 'eks-b', 'queue-recovery', 2, 'fsdp', NULL, $4, NULL, NULL)`,
		t0, t0.Add(time.Minute), t0.Add(10*time.Minute), t0.Add(12*time.Minute))
	mustExec(`INSERT INTO data.checkpoints (job_id, generation, step, format, world_size, manifest_key, file_count, size_bytes, config_sha256, saved_at, approved_at)
		VALUES ('rt000001', 1, 694, 'fsdp-sharded', 2, 'k', 2, 10, 'c', $1, $1)`, t0.Add(5*time.Minute))
	mustExec(`INSERT INTO data.job_progress (job_id, generation, status, step, total_steps, loss, event_id, updated_at)
		VALUES ('rt000001', 2, 'RUNNING', 800, 2080, 1.5, 'e', $1)`, t0.Add(13*time.Minute))

	r, err := NewJobRecords(ctx, os.Getenv("PGHOST"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	rows, err := r.Statuses(ctx, []string{"rt000001", "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	s := rows[0]
	if s.Generation != 2 || s.State != "RUNNING" || *s.Cluster != "eks-b" || s.RecoveryCount != 1 ||
		*s.Step != 800 || *s.TotalSteps != 2080 || *s.LastApprovedStep != 694 {
		t.Errorf("row = %+v", s)
	}

	runs, ckpts, err := r.History(ctx, "rt000001")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Generation != 1 || *runs[0].EndReason != "fenced" || runs[1].SubmittedAt != nil {
		t.Errorf("runs = %+v", runs)
	}
	if len(ckpts) != 1 || ckpts[0].Step != 694 {
		t.Errorf("checkpoints = %+v", ckpts)
	}
}

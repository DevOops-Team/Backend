package repository

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/DevOops-Team/Backend/internal/model"
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
	if gone, err := r.Gone(t.Context(), nil); err != nil || gone != nil {
		t.Errorf("Gone = %v, %v; want nothing without PGHOST", gone, err)
	}
	final, err := r.FinalCheckpoint(t.Context(), "x")
	if err != nil || final != nil {
		t.Errorf("FinalCheckpoint = %v, %v; want nothing without PGHOST", final, err)
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
		VALUES ('rt000001', 1, 300, 'fsdp-sharded', 2, 'k300', 2, 9, 'c', $1, $1),
		       ('rt000001', 1, 694, 'fsdp-sharded', 2, 'k', 2, 10, 'c', $2, $2)`, t0.Add(3*time.Minute), t0.Add(5*time.Minute))
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

	// GPU-204: a job without a JobSet that has not ended is listed from the database, with its first submission.
	gone, err := r.Gone(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(gone, func(g model.StatusRow) bool { return g.JobID == "rt000001" })
	if i < 0 || gone[i].FirstSubmittedAt == nil || !gone[i].FirstSubmittedAt.Equal(t0) || gone[i].State != "RUNNING" {
		t.Errorf("Gone(nil) = %+v, want rt000001 first submitted at %s", gone, t0)
	}
	if gone, err := r.Gone(ctx, []string{"rt000001"}); err != nil ||
		slices.ContainsFunc(gone, func(g model.StatusRow) bool { return g.JobID == "rt000001" }) {
		t.Errorf("Gone(listed) = %+v, %v; want rt000001 left out", gone, err)
	}

	runs, ckpts, err := r.History(ctx, "rt000001")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Generation != 1 || *runs[0].EndReason != "fenced" || runs[1].SubmittedAt != nil {
		t.Errorf("runs = %+v", runs)
	}
	if len(ckpts) != 2 || ckpts[0].Step != 300 || ckpts[1].Step != 694 {
		t.Errorf("checkpoints = %+v", ckpts)
	}

	final, err := r.FinalCheckpoint(ctx, "rt000001")
	if err != nil {
		t.Fatal(err)
	}
	if final == nil || final.Generation != 1 || final.Step != 694 || final.WorldSize != 2 || final.ManifestKey != "k" ||
		final.SizeBytes != 10 {
		t.Errorf("final checkpoint = %+v, want generation 1 step 694", final)
	}
	if none, err := r.FinalCheckpoint(ctx, "nope"); err != nil || none != nil {
		t.Errorf("FinalCheckpoint(nope) = %+v, %v; want nil", none, err)
	}
}

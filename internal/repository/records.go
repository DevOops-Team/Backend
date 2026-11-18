package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DevOops-Team/Backend/internal/model"
)

// queryTimeout keeps a slow database from holding a screen poll (backend_ro also has statement_timeout 5s).
const queryTimeout = 3 * time.Second

// JobRecords reads the data database (PostgreSQL gpuf_data, Data repo db/) as backend_ro: data.v_job_status
// for the screens and data.job_runs · data.checkpoints for the timeline. The collector writes them from
// Kafka job.lifecycle (GPU-141 · GPU-185).
type JobRecords struct {
	pool *pgxpool.Pool // nil = not configured: reads return nothing
}

// NewJobRecords connects with the standard libpq environment (PGHOST · PGPORT · PGDATABASE · PGUSER ·
// PGPASSWORD · PGSSLMODE). Without PGHOST it returns records that report nothing (local runs), so the
// screens show the management cluster's state only. Connections open lazily: a database that is down
// at start does not stop the server.
func NewJobRecords(ctx context.Context, host string) (*JobRecords, error) {
	if host == "" {
		return &JobRecords{}, nil
	}
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("data database config: %w", err)
	}
	cfg.MaxConns = 4 // backend_ro CONNECTION LIMIT 10 across 3 replicas
	cfg.ConnConfig.RuntimeParams["application_name"] = "backend"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("data database pool: %w", err)
	}
	return &JobRecords{pool: pool}, nil
}

func (r *JobRecords) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// Statuses returns the v_job_status rows of the given jobs (jobs without a row are left out).
func (r *JobRecords) Statuses(ctx context.Context, ids []string) ([]model.StatusRow, error) {
	if r.pool == nil || len(ids) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `
		SELECT job_id, generation, exp_id, cluster, state, submitted_at, started_at, ended_at, recovery_count,
		       progress_status, step, total_steps, loss, progress_updated_at, last_approved_step, last_approved_at
		FROM data.v_job_status WHERE job_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("query v_job_status: %w", err)
	}
	defer rows.Close()
	var out []model.StatusRow
	for rows.Next() {
		var s model.StatusRow
		if err := rows.Scan(&s.JobID, &s.Generation, &s.ExpID, &s.Cluster, &s.State, &s.SubmittedAt, &s.StartedAt,
			&s.EndedAt, &s.RecoveryCount, &s.ProgressStatus, &s.Step, &s.TotalSteps, &s.Loss, &s.ProgressUpdatedAt,
			&s.LastApprovedStep, &s.LastApprovedAt); err != nil {
			return nil, fmt.Errorf("scan v_job_status: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Gone returns the v_job_status rows of jobs whose JobSets are not among ids but that the list still shows
// (GPU-204): stopped or cancelled, fenced (the relocator has not created the next generation yet) or not
// ended (a stop whose end event has not arrived). Completed and failed jobs have no reason to lose their
// JobSet except a delete, so they stay out.
func (r *JobRecords) Gone(ctx context.Context, ids []string) ([]model.StatusRow, error) {
	if r.pool == nil {
		return nil, nil
	}
	if ids == nil {
		ids = []string{} // a NULL array would make NOT (... = ANY) NULL and hide every row
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `
		SELECT s.job_id, s.generation, s.exp_id, s.cluster, s.state, s.submitted_at, s.started_at, s.ended_at,
		       s.recovery_count, s.progress_status, s.step, s.total_steps, s.loss, s.progress_updated_at,
		       s.last_approved_step, s.last_approved_at,
		       (SELECT min(r.submitted_at) FROM data.job_runs r WHERE r.job_id = s.job_id)
		FROM data.v_job_status s
		WHERE NOT (s.job_id = ANY($1)) AND s.state NOT IN ('COMPLETED', 'FAILED')`, ids)
	if err != nil {
		return nil, fmt.Errorf("query v_job_status: %w", err)
	}
	defer rows.Close()
	var out []model.StatusRow
	for rows.Next() {
		var s model.StatusRow
		if err := rows.Scan(&s.JobID, &s.Generation, &s.ExpID, &s.Cluster, &s.State, &s.SubmittedAt, &s.StartedAt,
			&s.EndedAt, &s.RecoveryCount, &s.ProgressStatus, &s.Step, &s.TotalSteps, &s.Loss, &s.ProgressUpdatedAt,
			&s.LastApprovedStep, &s.LastApprovedAt, &s.FirstSubmittedAt); err != nil {
			return nil, fmt.Errorf("scan v_job_status: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// History returns the job's generations and approved checkpoints, oldest first.
func (r *JobRecords) History(ctx context.Context, jobID string) ([]model.RunRecord, []model.CheckpointRecord, error) {
	if r.pool == nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `
		SELECT generation, cluster, queue, submitted_at, started_at, ended_at, end_reason
		FROM data.job_runs WHERE job_id = $1 ORDER BY generation`, jobID)
	if err != nil {
		return nil, nil, fmt.Errorf("query job_runs: %w", err)
	}
	var runs []model.RunRecord
	for rows.Next() {
		var run model.RunRecord
		if err := rows.Scan(&run.Generation, &run.Cluster, &run.Queue, &run.SubmittedAt, &run.StartedAt,
			&run.EndedAt, &run.EndReason); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan job_runs: %w", err)
		}
		runs = append(runs, run)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rows, err = r.pool.Query(ctx, `
		SELECT generation, step, approved_at FROM data.checkpoints WHERE job_id = $1 ORDER BY generation, step`, jobID)
	if err != nil {
		return nil, nil, fmt.Errorf("query checkpoints: %w", err)
	}
	defer rows.Close()
	var ckpts []model.CheckpointRecord
	for rows.Next() {
		var c model.CheckpointRecord
		if err := rows.Scan(&c.Generation, &c.Step, &c.ApprovedAt); err != nil {
			return nil, nil, fmt.Errorf("scan checkpoints: %w", err)
		}
		ckpts = append(ckpts, c)
	}
	return runs, ckpts, rows.Err()
}

// FinalCheckpoint returns the job's newest approved checkpoint (highest generation, then step), or nil when
// it has none. Only approved checkpoints reach data.checkpoints (checkpoint.approved).
func (r *JobRecords) FinalCheckpoint(ctx context.Context, jobID string) (*model.FinalCheckpoint, error) {
	if r.pool == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	var c model.FinalCheckpoint
	var worldSize int16
	err := r.pool.QueryRow(ctx, `
		SELECT generation, step, format, world_size, manifest_key, size_bytes, approved_at
		FROM data.checkpoints WHERE job_id = $1 ORDER BY generation DESC, step DESC LIMIT 1`, jobID).
		Scan(&c.Generation, &c.Step, &c.Format, &worldSize, &c.ManifestKey, &c.SizeBytes, &c.ApprovedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query checkpoints: %w", err)
	}
	c.WorldSize = int(worldSize)
	return &c, nil
}

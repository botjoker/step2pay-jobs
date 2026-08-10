package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type JobRepo struct {
	pool *pgxpool.Pool
}

func NewJobRepo(pool *pgxpool.Pool) *JobRepo {
	return &JobRepo{pool: pool}
}

func (r *JobRepo) LoadDueJobs(ctx context.Context) ([]models.SchedulerJob, error) {
	rows, err := r.pool.Query(ctx, `
		WITH due AS (
			SELECT id
			FROM scheduler_jobs
			WHERE is_active = true
			  AND (next_run IS NULL OR next_run <= now())
			  AND (status <> 'running' OR updated_at < now() - interval '30 minutes')
			ORDER BY next_run ASC NULLS FIRST
			FOR UPDATE SKIP LOCKED
		)
		UPDATE scheduler_jobs AS jobs
		SET status = 'running', updated_at = now()
		FROM due
		WHERE jobs.id = due.id
		RETURNING jobs.id, jobs.profile_id, jobs.name, jobs.is_active,
		          jobs.trigger_type, jobs.trigger_config, jobs.action_type,
		          jobs.action_config, jobs.last_run, jobs.next_run, jobs.status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.SchedulerJob
	for rows.Next() {
		var j models.SchedulerJob
		if err := rows.Scan(
			&j.ID, &j.ProfileID, &j.Name, &j.IsActive,
			&j.TriggerType, &j.TriggerConfig,
			&j.ActionType, &j.ActionConfig,
			&j.LastRun, &j.NextRun, &j.Status,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (r *JobRepo) UpdateAfterRun(ctx context.Context, jobID uuid.UUID, nextRun *time.Time, status, errMsg string, deactivate bool) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_jobs
		SET last_run = now(), next_run = $1, status = $2, error_msg = $3,
		    is_active = CASE WHEN $4 THEN false ELSE is_active END,
		    updated_at = now()
		WHERE id = $5
	`, nextRun, status, errMsg, deactivate, jobID)
	return err
}

func (r *JobRepo) UpdateNextRun(ctx context.Context, jobID uuid.UUID, nextRun *time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_jobs
		SET next_run = $1, status = 'idle', updated_at = now()
		WHERE id = $2
	`, nextRun, jobID)
	return err
}

func (r *JobRepo) WriteLog(ctx context.Context, jobID uuid.UUID, status, message string, count int) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO scheduler_job_logs (job_id, status, message, affected_count)
		VALUES ($1, $2, $3, $4)
	`, jobID, status, message, count)
	return err
}

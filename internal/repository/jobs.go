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
		SELECT id, profile_id, name, is_active, trigger_type, trigger_config,
		       action_type, action_config, last_run, next_run, status
		FROM scheduler_jobs
		WHERE is_active = true
		  AND (next_run IS NULL OR next_run <= now())
		ORDER BY next_run ASC NULLS FIRST
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

func (r *JobRepo) UpdateAfterRun(ctx context.Context, jobID uuid.UUID, nextRun *time.Time, status, errMsg string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_jobs
		SET last_run = now(), next_run = $1, status = $2, error_msg = $3, updated_at = now()
		WHERE id = $4
	`, nextRun, status, errMsg, jobID)
	return err
}

func (r *JobRepo) UpdateNextRun(ctx context.Context, jobID uuid.UUID, nextRun *time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_jobs SET next_run = $1, updated_at = now() WHERE id = $2
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

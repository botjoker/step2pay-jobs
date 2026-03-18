package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type EventTrigger struct{}

type eventConfig struct {
	Type string `json:"type"`
}

func (t *EventTrigger) ShouldRun(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (bool, error) {
	var cfg eventConfig
	if err := json.Unmarshal(job.TriggerConfig, &cfg); err != nil {
		return false, fmt.Errorf("invalid event config: %w", err)
	}

	since := time.Now().Add(-15 * time.Minute)
	if job.LastRun != nil {
		since = *job.LastRun
	}

	switch cfg.Type {
	case "new_client":
		return checkNewRecords(ctx, pool, "clients", job.ProfileID.String(), since)
	case "new_payment":
		return checkNewRecords(ctx, pool, "payments", job.ProfileID.String(), since)
	default:
		return false, fmt.Errorf("unknown event type: %s", cfg.Type)
	}
}

func checkNewRecords(ctx context.Context, pool *pgxpool.Pool, table, profileID string, since time.Time) (bool, error) {
	var count int
	err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE profile_id = $1 AND created_at > $2", table),
		profileID, since,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

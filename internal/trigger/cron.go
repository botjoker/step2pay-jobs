package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/sambacrm/scheduler/internal/models"
)

type CronTrigger struct{}

type cronConfig struct {
	Cron string `json:"cron"`
}

func (t *CronTrigger) ShouldRun(_ context.Context, _ models.SchedulerJob, _ *pgxpool.Pool) (bool, error) {
	return true, nil
}

func NextCronRun(triggerConfig json.RawMessage) (*time.Time, error) {
	var cfg cronConfig
	if err := json.Unmarshal(triggerConfig, &cfg); err != nil {
		return nil, fmt.Errorf("invalid cron config: %w", err)
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cfg.Cron)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", cfg.Cron, err)
	}
	// Cron expressions are always interpreted in Moscow time (UTC+3, no DST since 2014).
	msk := time.FixedZone("MSK", 3*60*60)
	next := schedule.Next(time.Now().In(msk))
	return &next, nil
}

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
	next := schedule.Next(time.Now())
	return &next, nil
}

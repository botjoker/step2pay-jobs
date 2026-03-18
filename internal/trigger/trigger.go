package trigger

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type Trigger interface {
	ShouldRun(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (bool, error)
}

var Registry = map[string]Trigger{
	"cron":      &CronTrigger{},
	"condition": &ConditionTrigger{},
	"event":     &EventTrigger{},
}

func NextRunForJob(job models.SchedulerJob, pollInterval time.Duration) (*time.Time, error) {
	if job.TriggerType == "cron" {
		return NextCronRun(json.RawMessage(job.TriggerConfig))
	}
	t := time.Now().Add(pollInterval)
	return &t, nil
}

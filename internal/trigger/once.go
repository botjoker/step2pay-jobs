package trigger

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type OnceTrigger struct{}

func (t *OnceTrigger) ShouldRun(_ context.Context, _ models.SchedulerJob, _ *pgxpool.Pool) (bool, error) {
	return true, nil
}

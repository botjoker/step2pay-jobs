package action

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/config"
	"github.com/sambacrm/scheduler/internal/models"
)

type Action interface {
	Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, error)
}

func BuildRegistry(cfg *config.Config) map[string]Action {
	return map[string]Action{
		"send_notification": &NotificationAction{
			rustBaseURL: cfg.RustAPIBaseURL,
			internalKey: cfg.InternalAPIKey,
		},
	}
}

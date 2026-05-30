package action

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/config"
	"github.com/sambacrm/scheduler/internal/models"
)

// Execute returns (sent count, warning message, error).
// warning is non-empty when some recipients succeeded but others failed — not a hard error.
type Action interface {
	Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, string, error)
}

func BuildRegistry(cfg *config.Config) map[string]Action {
	return map[string]Action{
		"send_notification": &NotificationAction{
			rustBaseURL: cfg.RustAPIBaseURL,
			internalKey: cfg.InternalAPIKey,
		},
		"retry_notifications": &RetryNotificationsAction{
			rustBaseURL: cfg.RustAPIBaseURL,
			internalKey: cfg.InternalAPIKey,
		},
	}
}

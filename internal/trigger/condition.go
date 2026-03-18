package trigger

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

// ConditionTrigger проверяет выполнение условия перед запуском action.
// Будущие условия (истечение абонементов и т.д.) добавляются как новые case.
//
// trigger_config: {"type": "always"}
type ConditionTrigger struct{}

type conditionConfig struct {
	Type string `json:"type"`
}

func (t *ConditionTrigger) ShouldRun(_ context.Context, job models.SchedulerJob, _ *pgxpool.Pool) (bool, error) {
	var cfg conditionConfig
	if err := json.Unmarshal(job.TriggerConfig, &cfg); err != nil {
		return false, fmt.Errorf("invalid condition config: %w", err)
	}

	switch cfg.Type {
	case "always", "":
		return true, nil
	default:
		return false, fmt.Errorf("unknown condition type: %s", cfg.Type)
	}
}

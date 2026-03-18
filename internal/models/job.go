package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type SchedulerJob struct {
	ID            uuid.UUID       `json:"id"`
	ProfileID     uuid.UUID       `json:"profile_id"`
	Name          string          `json:"name"`
	IsActive      bool            `json:"is_active"`
	TriggerType   string          `json:"trigger_type"`
	TriggerConfig json.RawMessage `json:"trigger_config"`
	ActionType    string          `json:"action_type"`
	ActionConfig  json.RawMessage `json:"action_config"`
	LastRun       *time.Time      `json:"last_run"`
	NextRun       *time.Time      `json:"next_run"`
	Status        string          `json:"status"`
}

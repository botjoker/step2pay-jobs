package action

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type RetryNotificationsAction struct {
	rustBaseURL string
	internalKey string
}

type retryQueueEntry struct {
	ID string `db:"id"`
}

func (a *RetryNotificationsAction) Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, string, error) {
	// 1. Fetch up to 100 pending entries due for retry
	rows, err := pool.Query(ctx,
		`SELECT id::text FROM notification_retry_queue
		 WHERE status = 'pending' AND next_attempt_at <= now()
		 ORDER BY next_attempt_at
		 LIMIT 100`,
	)
	if err != nil {
		return 0, "", fmt.Errorf("query retry queue: %w", err)
	}
	defer rows.Close()

	var queueIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, "", fmt.Errorf("scan queue id: %w", err)
		}
		queueIDs = append(queueIDs, id)
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf("iterate retry queue: %w", err)
	}

	if len(queueIDs) == 0 {
		return 0, "", nil
	}

	// 2. Call backend retry endpoint
	payload := map[string][]string{"queue_ids": queueIDs}
	body, _ := json.Marshal(payload)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.rustBaseURL+"/internal/notifications/retry",
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, "", fmt.Errorf("build retry request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", a.internalKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("call retry endpoint: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("retry endpoint status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Succeeded int `json:"succeeded"`
		Failed    int `json:"failed"`
		Exhausted int `json:"exhausted"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, "", fmt.Errorf("parse retry response: %w", err)
	}

	sent := result.Succeeded
	var warn string
	if result.Failed > 0 || result.Exhausted > 0 {
		warn = fmt.Sprintf("failed=%d exhausted=%d", result.Failed, result.Exhausted)
	}

	return sent, warn, nil
}

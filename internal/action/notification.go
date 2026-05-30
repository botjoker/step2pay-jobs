package action

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

type profileRateLimiter struct {
	mu       sync.Mutex
	lastCall time.Time
	interval time.Duration
}

func (r *profileRateLimiter) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if elapsed := now.Sub(r.lastCall); elapsed < r.interval {
		time.Sleep(r.interval - elapsed)
	}
	r.lastCall = time.Now()
}

var tgLimiters sync.Map // profileID -> *profileRateLimiter
var vkLimiters sync.Map // profileID -> *profileRateLimiter

func getTGLimiter(profileID string) *profileRateLimiter {
	v, _ := tgLimiters.LoadOrStore(profileID, &profileRateLimiter{interval: 40 * time.Millisecond})
	return v.(*profileRateLimiter)
}

func getVKLimiter(profileID string) *profileRateLimiter {
	v, _ := vkLimiters.LoadOrStore(profileID, &profileRateLimiter{interval: 55 * time.Millisecond})
	return v.(*profileRateLimiter)
}

// NotificationAction отправляет сообщение клиенту или всей группе
// через Telegram, VK, или оба канала.
//
// action_config:
//
//	{
//	  "channel": "telegram",       // "telegram" | "vk" | "both"
//	  "recipient_type": "client",  // "client" | "group" | "debtors" | etc.
//	  "recipient_id": "uuid",      // client_id или group_id
//	  "message": "Привет, {{name}}! Занятие завтра в 18:00."
//	}
//
// Плейсхолдеры в message:
//
//	{{name}}      → имя клиента (firstname + lastname)
//	{{firstname}} → только имя
//	{{lastname}}  → только фамилия
//	+ любые ключи из recipient.TemplateVars
type NotificationAction struct {
	rustBaseURL string
	internalKey string
}

type notificationConfig struct {
	Channel       string `json:"channel"`        // "telegram" | "vk" | "both"
	RecipientType string `json:"recipient_type"` // "client" | "group" | ...
	RecipientID   string `json:"recipient_id"`
	Message       string `json:"message"`
}

type notificationSettings struct {
	TelegramEnabled  bool
	TelegramBotToken string
	VKEnabled        bool
	VKCommunityToken string
}

type audienceRequest struct {
	ProfileID    string          `json:"profile_id"`
	AudienceType string          `json:"audience_type"`
	Params       json.RawMessage `json:"params"`
}

type recipientInfo struct {
	ClientID       string            `json:"client_id"`
	Firstname      string            `json:"firstname"`
	Lastname       string            `json:"lastname"`
	TelegramChatID *string           `json:"telegram_chat_id"`
	VkUserID       *string           `json:"vk_user_id"`
	TemplateVars   map[string]string `json:"template_vars"`
	SubscriptionID *string           `json:"subscription_id,omitempty"`
}

func (a *NotificationAction) Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, string, error) {
	var cfg notificationConfig
	if err := json.Unmarshal(job.ActionConfig, &cfg); err != nil {
		return 0, "", fmt.Errorf("invalid action_config: %w", err)
	}
	if cfg.Message == "" {
		return 0, "", fmt.Errorf("message is required")
	}
	if cfg.Channel == "" {
		cfg.Channel = "telegram"
	}
	if cfg.RecipientType == "" {
		cfg.RecipientType = "client"
	}

	settings, err := loadNotificationSettings(ctx, pool, job.ProfileID.String())
	if err != nil {
		return 0, "", fmt.Errorf("load notification settings: %w", err)
	}

	if cfg.Channel == "telegram" && !settings.TelegramEnabled {
		return 0, "", fmt.Errorf("telegram notifications disabled for this profile")
	}
	if cfg.Channel == "vk" && !settings.VKEnabled {
		return 0, "", fmt.Errorf("vk notifications disabled for this profile")
	}
	if cfg.Channel == "both" && !settings.TelegramEnabled && !settings.VKEnabled {
		return 0, "", fmt.Errorf("both channels disabled for this profile")
	}

	params, err := buildAudienceParams(cfg, job.ActionConfig)
	if err != nil {
		return 0, "", fmt.Errorf("build audience params: %w", err)
	}
	recipients, err := a.fetchAudience(ctx, job.ProfileID.String(), cfg.RecipientType, params)
	if err != nil {
		return 0, "", fmt.Errorf("fetch audience: %w", err)
	}

	if len(recipients) == 0 {
		return 0, "", nil
	}

	// Init Telegram bot once — avoids a getMe call per recipient.
	// Use a 10s timeout so a network blip fails fast instead of hanging per-recipient.
	var tgBot *tgbotapi.BotAPI
	needTG := (cfg.Channel == "telegram" || cfg.Channel == "both") && settings.TelegramEnabled
	if needTG {
		tgHTTPClient := &http.Client{Timeout: 10 * time.Second}
		var botErr error
		tgBot, botErr = tgbotapi.NewBotAPIWithClient(settings.TelegramBotToken, tgbotapi.APIEndpoint, tgHTTPClient)
		if botErr != nil {
			if cfg.Channel == "telegram" {
				return 0, "", fmt.Errorf("init telegram bot: %w", botErr)
			}
			// for "both": proceed with VK only
			tgBot = nil
		}
	}

	sent := 0
	var errs []string
	noTGID, noVKID := 0, 0
	var sentSubscriptionIDs []string

	for _, r := range recipients {
		text := renderMessage(cfg.Message, r)
		var anySent bool
		switch cfg.Channel {
		case "telegram":
			if r.TelegramChatID == nil {
				noTGID++
				continue
			}
			getTGLimiter(job.ProfileID.String()).Wait()
			if err := sendTelegramMsg(tgBot, *r.TelegramChatID, text); err != nil {
				errs = append(errs, fmt.Sprintf("tg(%s): %v", *r.TelegramChatID, err))
				continue
			}
			sent++
			anySent = true
		case "vk":
			if r.VkUserID == nil {
				noVKID++
				continue
			}
			getVKLimiter(job.ProfileID.String()).Wait()
			if err := sendVK(settings.VKCommunityToken, *r.VkUserID, text); err != nil {
				errs = append(errs, fmt.Sprintf("vk(%s): %v", *r.VkUserID, err))
				continue
			}
			sent++
			anySent = true
		case "both":
			if tgBot != nil {
				if r.TelegramChatID != nil {
					getTGLimiter(job.ProfileID.String()).Wait()
					if err := sendTelegramMsg(tgBot, *r.TelegramChatID, text); err != nil {
						errs = append(errs, fmt.Sprintf("tg(%s): %v", *r.TelegramChatID, err))
					} else {
						sent++
						anySent = true
					}
				} else {
					noTGID++
				}
			}
			if settings.VKEnabled {
				if r.VkUserID != nil {
					getVKLimiter(job.ProfileID.String()).Wait()
					if err := sendVK(settings.VKCommunityToken, *r.VkUserID, text); err != nil {
						errs = append(errs, fmt.Sprintf("vk(%s): %v", *r.VkUserID, err))
					} else {
						sent++
						anySent = true
					}
				} else {
					noVKID++
				}
			}
		}
		if anySent && cfg.RecipientType == "low_subscription" && r.SubscriptionID != nil {
			sentSubscriptionIDs = append(sentSubscriptionIDs, *r.SubscriptionID)
		}
	}

	// Двухфазный коммит: подтверждаем доставку для low_subscription (#1)
	if len(sentSubscriptionIDs) > 0 {
		if err := a.confirmDelivery(ctx, "low_subscription", sentSubscriptionIDs); err != nil {
			// Не фатально: lease сам истечёт через 15 мин, просто лог
			errs = append(errs, fmt.Sprintf("confirm delivery: %v", err))
		}
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("sent %d", sent))
	if noTGID > 0 {
		parts = append(parts, fmt.Sprintf("no telegram id: %d", noTGID))
	}
	if noVKID > 0 {
		parts = append(parts, fmt.Sprintf("no vk id: %d", noVKID))
	}
	if len(errs) > 0 {
		parts = append(parts, fmt.Sprintf("failed: %s", strings.Join(errs, "; ")))
	}
	msg := strings.Join(parts, " | ")

	if sent == 0 && len(errs) > 0 {
		return 0, "", fmt.Errorf("all failed: %s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 || noTGID > 0 || noVKID > 0 {
		return sent, msg, nil
	}
	return sent, "", nil
}

type confirmDeliveryRequest struct {
	RecipientType   string   `json:"recipient_type"`
	SubscriptionIDs []string `json:"subscription_ids"`
}

func (a *NotificationAction) confirmDelivery(ctx context.Context, recipientType string, subscriptionIDs []string) error {
	body := confirmDeliveryRequest{
		RecipientType:   recipientType,
		SubscriptionIDs: subscriptionIDs,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.rustBaseURL+"/internal/scheduler/audience/confirm",
		bytes.NewReader(bodyBytes),
	)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", a.internalKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// buildAudienceParams converts action_config into params expected by the backend audience endpoint.
// For client/group the backend expects client_id/group_id; for other types pass config as-is.
func buildAudienceParams(cfg notificationConfig, rawConfig json.RawMessage) (json.RawMessage, error) {
	switch cfg.RecipientType {
	case "client":
		p, err := json.Marshal(map[string]string{"client_id": cfg.RecipientID})
		return p, err
	case "group":
		p, err := json.Marshal(map[string]string{"group_id": cfg.RecipientID})
		return p, err
	default:
		return rawConfig, nil
	}
}

func (a *NotificationAction) fetchAudience(
	ctx context.Context,
	profileID string,
	audienceType string,
	params json.RawMessage,
) ([]recipientInfo, error) {
	reqBody := audienceRequest{
		ProfileID:    profileID,
		AudienceType: audienceType,
		Params:       params,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("fetchAudience marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.rustBaseURL+"/internal/scheduler/audience",
		bytes.NewReader(bodyBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("fetchAudience new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", a.internalKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetchAudience do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetchAudience status %d: %s", resp.StatusCode, string(body))
	}

	var recipients []recipientInfo
	if err := json.NewDecoder(resp.Body).Decode(&recipients); err != nil {
		return nil, fmt.Errorf("fetchAudience decode: %w", err)
	}
	return recipients, nil
}

func loadNotificationSettings(ctx context.Context, pool *pgxpool.Pool, profileID string) (notificationSettings, error) {
	var s notificationSettings
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(telegram_enabled, false),
			COALESCE(telegram_bot_token, ''),
			COALESCE(vk_enabled, false),
			COALESCE(vk_community_token, '')
		FROM notification_settings
		WHERE profile_id = $1
	`, profileID).Scan(&s.TelegramEnabled, &s.TelegramBotToken, &s.VKEnabled, &s.VKCommunityToken)
	if err != nil {
		return s, fmt.Errorf("profile %s: %w", profileID, err)
	}
	return s, nil
}

var unresolvedPlaceholder = regexp.MustCompile(`\{\{[^}]+\}\}`)

func renderMessage(tmpl string, r recipientInfo) string {
	fullName := strings.TrimSpace(r.Firstname + " " + r.Lastname)
	s := strings.ReplaceAll(tmpl, "{{name}}", fullName)
	s = strings.ReplaceAll(s, "{{firstname}}", r.Firstname)
	s = strings.ReplaceAll(s, "{{lastname}}", r.Lastname)
	for k, v := range r.TemplateVars {
		s = strings.ReplaceAll(s, "{{"+k+"}}", v)
	}
	s = unresolvedPlaceholder.ReplaceAllString(s, "")
	return s
}

func sendTelegramMsg(bot *tgbotapi.BotAPI, chatID, message string) error {
	var chatIDInt int64
	if _, err := fmt.Sscanf(chatID, "%d", &chatIDInt); err != nil {
		return fmt.Errorf("invalid chat_id %q: %w", chatID, err)
	}
	msg := tgbotapi.NewMessage(chatIDInt, message)
	_, err := bot.Send(msg)
	return err
}

func sendVK(communityToken, vkUserID, message string) error {
	params := url.Values{
		"user_id":      {vkUserID},
		"message":      {message},
		"random_id":    {fmt.Sprintf("%d", rand.Int63())},
		"access_token": {communityToken},
		"v":            {"5.199"},
	}
	resp, err := http.PostForm("https://api.vk.com/method/messages.send", params)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Error *struct {
			ErrorMsg string `json:"error_msg"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if result.Error != nil {
		return fmt.Errorf("vk api: %s", result.Error.ErrorMsg)
	}
	return nil
}

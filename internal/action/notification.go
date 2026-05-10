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
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sambacrm/scheduler/internal/models"
)

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
}

func (a *NotificationAction) Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, error) {
	var cfg notificationConfig
	if err := json.Unmarshal(job.ActionConfig, &cfg); err != nil {
		return 0, fmt.Errorf("invalid action_config: %w", err)
	}
	if cfg.Message == "" {
		return 0, fmt.Errorf("message is required")
	}
	if cfg.Channel == "" {
		cfg.Channel = "telegram"
	}
	if cfg.RecipientType == "" {
		cfg.RecipientType = "client"
	}

	settings, err := loadNotificationSettings(ctx, pool, job.ProfileID.String())
	if err != nil {
		return 0, fmt.Errorf("load notification settings: %w", err)
	}

	if cfg.Channel == "telegram" && !settings.TelegramEnabled {
		return 0, fmt.Errorf("telegram notifications disabled for this profile")
	}
	if cfg.Channel == "vk" && !settings.VKEnabled {
		return 0, fmt.Errorf("vk notifications disabled for this profile")
	}

	// Pass the full action_config as params so Rust can read all fields
	// (recipient_id, group_id, min_debt, sessions_threshold, etc.)
	recipients, err := a.fetchAudience(ctx, job.ProfileID.String(), cfg.RecipientType, job.ActionConfig)
	if err != nil {
		return 0, fmt.Errorf("fetch audience: %w", err)
	}

	if len(recipients) == 0 {
		return 0, nil
	}

	sent := 0
	var errs []string
	for _, r := range recipients {
		text := renderMessage(cfg.Message, r)
		switch cfg.Channel {
		case "telegram":
			if r.TelegramChatID == nil {
				continue
			}
			if err := sendTelegram(settings.TelegramBotToken, *r.TelegramChatID, text); err != nil {
				errs = append(errs, fmt.Sprintf("tg(%s): %v", *r.TelegramChatID, err))
				continue
			}
			sent++
		case "vk":
			if r.VkUserID == nil {
				continue
			}
			if err := sendVK(settings.VKCommunityToken, *r.VkUserID, text); err != nil {
				errs = append(errs, fmt.Sprintf("vk(%s): %v", *r.VkUserID, err))
				continue
			}
			sent++
		case "both":
			if r.TelegramChatID != nil {
				if err := sendTelegram(settings.TelegramBotToken, *r.TelegramChatID, text); err != nil {
					errs = append(errs, fmt.Sprintf("tg(%s): %v", *r.TelegramChatID, err))
				} else {
					sent++
				}
			}
			if r.VkUserID != nil {
				if err := sendVK(settings.VKCommunityToken, *r.VkUserID, text); err != nil {
					errs = append(errs, fmt.Sprintf("vk(%s): %v", *r.VkUserID, err))
				} else {
					sent++
				}
			}
		}
	}

	if len(errs) > 0 {
		return sent, fmt.Errorf("partial failures (%d sent): %s", sent, strings.Join(errs, "; "))
	}
	return sent, nil
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

func renderMessage(tmpl string, r recipientInfo) string {
	fullName := strings.TrimSpace(r.Firstname + " " + r.Lastname)
	s := strings.ReplaceAll(tmpl, "{{name}}", fullName)
	s = strings.ReplaceAll(s, "{{firstname}}", r.Firstname)
	s = strings.ReplaceAll(s, "{{lastname}}", r.Lastname)
	for k, v := range r.TemplateVars {
		s = strings.ReplaceAll(s, "{{"+k+"}}", v)
	}
	return s
}

func sendTelegram(botToken, chatID, message string) error {
	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		return fmt.Errorf("init bot: %w", err)
	}
	var chatIDInt int64
	if _, err := fmt.Sscanf(chatID, "%d", &chatIDInt); err != nil {
		return fmt.Errorf("invalid chat_id %q: %w", chatID, err)
	}
	msg := tgbotapi.NewMessage(chatIDInt, message)
	_, err = bot.Send(msg)
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

package action

import (
	"context"
	"encoding/json"
	"fmt"
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
//	  "recipient_type": "client",  // "client" | "group"
//	  "recipient_id": "uuid",      // client_id или group_id
//	  "message": "Привет, {{name}}! Занятие завтра в 18:00."
//	}
//
// Плейсхолдеры в message:
//
//	{{name}}      → имя клиента (firstname + lastname)
//	{{firstname}} → только имя
//	{{lastname}}  → только фамилия
type NotificationAction struct{}

type notificationConfig struct {
	Channel       string `json:"channel"`        // "telegram" | "vk" | "both"
	RecipientType string `json:"recipient_type"` // "client" | "group"
	RecipientID   string `json:"recipient_id"`
	Message       string `json:"message"`
}

type notificationSettings struct {
	TelegramEnabled  bool
	TelegramBotToken string
	VKEnabled        bool
	VKCommunityToken string
}

type clientContact struct {
	Firstname      string
	Lastname       string
	TelegramChatID *string
	VKUserID       *string
}

func (a *NotificationAction) Execute(ctx context.Context, job models.SchedulerJob, pool *pgxpool.Pool) (int, error) {
	var cfg notificationConfig
	if err := json.Unmarshal(job.ActionConfig, &cfg); err != nil {
		return 0, fmt.Errorf("invalid action_config: %w", err)
	}
	if cfg.RecipientID == "" {
		return 0, fmt.Errorf("recipient_id is required")
	}
	if cfg.Message == "" {
		return 0, fmt.Errorf("message is required")
	}
	if cfg.Channel == "" {
		cfg.Channel = "telegram"
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

	var contacts []clientContact
	switch cfg.RecipientType {
	case "client":
		c, err := loadClient(ctx, pool, cfg.RecipientID, job.ProfileID.String())
		if err != nil {
			return 0, fmt.Errorf("load client: %w", err)
		}
		contacts = []clientContact{c}
	case "group":
		contacts, err = loadGroupMembers(ctx, pool, cfg.RecipientID, job.ProfileID.String())
		if err != nil {
			return 0, fmt.Errorf("load group members: %w", err)
		}
	default:
		return 0, fmt.Errorf("unknown recipient_type: %s (use 'client' or 'group')", cfg.RecipientType)
	}

	if len(contacts) == 0 {
		return 0, nil
	}

	sent := 0
	var errs []string
	for _, contact := range contacts {
		text := renderMessage(cfg.Message, contact)
		switch cfg.Channel {
		case "telegram":
			if contact.TelegramChatID == nil {
				continue
			}
			if err := sendTelegram(settings.TelegramBotToken, *contact.TelegramChatID, text); err != nil {
				errs = append(errs, fmt.Sprintf("tg(%s): %v", *contact.TelegramChatID, err))
				continue
			}
			sent++
		case "vk":
			if contact.VKUserID == nil {
				continue
			}
			if err := sendVK(settings.VKCommunityToken, *contact.VKUserID, text); err != nil {
				errs = append(errs, fmt.Sprintf("vk(%s): %v", *contact.VKUserID, err))
				continue
			}
			sent++
		case "both":
			if contact.TelegramChatID != nil {
				if err := sendTelegram(settings.TelegramBotToken, *contact.TelegramChatID, text); err != nil {
					errs = append(errs, fmt.Sprintf("tg(%s): %v", *contact.TelegramChatID, err))
				} else {
					sent++
				}
			}
			if contact.VKUserID != nil {
				if err := sendVK(settings.VKCommunityToken, *contact.VKUserID, text); err != nil {
					errs = append(errs, fmt.Sprintf("vk(%s): %v", *contact.VKUserID, err))
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

func loadClient(ctx context.Context, pool *pgxpool.Pool, clientID, profileID string) (clientContact, error) {
	var c clientContact
	err := pool.QueryRow(ctx, `
		SELECT firstname, lastname, telegram_chat_id, vk_user_id
		FROM clients
		WHERE id = $1 AND profile_id = $2 AND archived = false
	`, clientID, profileID).Scan(&c.Firstname, &c.Lastname, &c.TelegramChatID, &c.VKUserID)
	return c, err
}

func loadGroupMembers(ctx context.Context, pool *pgxpool.Pool, groupID, profileID string) ([]clientContact, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.firstname, c.lastname, c.telegram_chat_id, c.vk_user_id
		FROM client_group_links l
		JOIN clients c ON c.id = l.client_id
		WHERE l.group_id = $1
		  AND l.profile_id = $2
		  AND l.archived = false
		  AND c.archived = false
	`, groupID, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []clientContact
	for rows.Next() {
		var c clientContact
		if err := rows.Scan(&c.Firstname, &c.Lastname, &c.TelegramChatID, &c.VKUserID); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func renderMessage(tmpl string, c clientContact) string {
	s := strings.ReplaceAll(tmpl, "{{name}}", strings.TrimSpace(c.Firstname+" "+c.Lastname))
	s = strings.ReplaceAll(s, "{{firstname}}", c.Firstname)
	s = strings.ReplaceAll(s, "{{lastname}}", c.Lastname)
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

# CLAUDE.md — step2pay-scheduler

Этот файл содержит инструкции для Claude Code, когда он работает в директории `step2pay-scheduler/`.

## Роль сервиса

`step2pay-scheduler` — фоновый Go-микросервис для отложенных и повторяющихся задач в SambaCRM. Основное назначение: рассылка уведомлений (Telegram, VK) клиентам образовательного центра — задолжникам, клиентам с истекающими абонементами, по конкретным группам и т.д.

Работает как **polling-loop**: не выставляет HTTP-эндпоинтов, не подписан на RabbitMQ, не имеет метрик и health-check'ов. Единственная активность — регулярно опрашивать таблицу `scheduler_jobs` и запускать те, у которых `next_run <= now()`.

CRUD джобов выполняется через админку → `step2pay-back` (`/admin/scheduler/jobs`), scheduler только их исполняет.

## Стек

- **Go 1.24.2**
- `jackc/pgx/v5` + `pgxpool` — PostgreSQL
- `robfig/cron/v3` — парсер cron-выражений (5-полевой формат)
- `go-telegram-bot-api/telegram-bot-api/v5` — Telegram Bot API
- `joho/godotenv` — загрузка `.env`
- `google/uuid` — UUID-типы

VK API дергается напрямую через `net/http` (POST на `messages.send`), без SDK.

## Обзор структуры

```
step2pay-scheduler/
├── cmd/main.go               # entrypoint: config → db → scheduler.Run(ctx)
├── internal/
│   ├── config/config.go      # загрузка env + значения по умолчанию
│   ├── db/db.go              # pgxpool.New(ctx, dsn)
│   ├── models/job.go         # SchedulerJob struct (mirror таблицы scheduler_jobs)
│   ├── repository/jobs.go    # LoadDueJobs, UpdateAfterRun, UpdateNextRun, WriteLog
│   ├── scheduler/scheduler.go# главный цикл: ticker → tick → processJob → finish
│   ├── trigger/
│   │   ├── trigger.go        # интерфейс Trigger + Registry + NextRunForJob
│   │   ├── cron.go           # CronTrigger (robfig/cron, MSK)
│   │   ├── event.go          # EventTrigger (new_client, new_payment)
│   │   └── condition.go      # ConditionTrigger (only "always")
│   └── action/
│       ├── action.go         # интерфейс Action + BuildRegistry
│       ├── notification.go   # NotificationAction (Telegram/VK, rate-limiters, audience via backend)
│       └── retry_notifications.go # RetryNotificationsAction (обход notification_retry_queue)
├── .env / .env.example       # локальная конфигурация
├── Dockerfile
└── go.mod
```

## Как работает цикл

1. `main.go` грузит `.env`, поднимает `pgxpool`, создаёт `scheduler.New(pool, cfg)` и вызывает `Run(ctx)`.
2. `Scheduler.Run` запускает `time.Ticker(cfg.PollInterval)`, первый `tick` — сразу.
3. Каждый `tick`:
   - `repo.LoadDueJobs()` → `SELECT ... FROM scheduler_jobs WHERE is_active = true AND (next_run IS NULL OR next_run <= now()) ORDER BY next_run ASC NULLS FIRST`
   - Для каждого job запускается **отдельная горутина** с `processJob`.
4. `processJob`:
   - Особый случай: новый cron-job с `NextRun == nil` — только считает `NextCronRun` и вызывает `UpdateNextRun`, действие **не выполняется** до следующего тика.
   - Иначе: `trigger.Registry[job.TriggerType].ShouldRun(ctx, job, pool)`.
     - Если `error` → `finish(status="error")`.
     - Если `false` → `finish(status="skipped")`.
     - Если `true` → `actions[job.ActionType].Execute(ctx, job, pool)`.
5. `finish`:
   - Считает следующий `next_run` через `trigger.NextRunForJob`.
   - `repo.UpdateAfterRun(id, next_run, status, err_msg)` — обновляет `last_run`, `next_run`, `status`, `error_msg`, `updated_at`.
   - `repo.WriteLog(id, status, message, affected_count)` — пишет запись в `scheduler_job_logs`.

## Триггеры

Реестр в `internal/trigger/trigger.go`:

```go
var Registry = map[string]Trigger{
    "cron":      &CronTrigger{},
    "condition": &ConditionTrigger{},
    "event":     &EventTrigger{},
}
```

| `trigger_type` | `trigger_config` | Поведение |
|----------------|------------------|-----------|
| `cron` | `{"cron": "0 9 * * 1"}` | `ShouldRun` **всегда true**. Таймизация полностью через `next_run` в БД. `NextCronRun` парсит выражение (5 полей: min/hour/dom/month/dow) в зоне **MSK (UTC+3, без DST)**. |
| `condition` | `{"type": "always"}` | Только `always` реализован. Другие типы → `error: unknown condition type`. |
| `event` | `{"type": "new_client"}` или `{"type": "new_payment"}` | `SELECT COUNT(*) FROM clients/payments WHERE profile_id = $1 AND created_at > $2`, где `$2 = last_run` или `now() - 15m` если `last_run IS NULL`. Возвращает `true`, если хотя бы одна новая запись. |

Функция `NextRunForJob`:
- Для `cron` — через `NextCronRun` (реально считает следующий запуск).
- Для остальных типов — просто `time.Now().Add(pollInterval)`. То есть event/condition-джобы будут дергаться каждые `POLL_INTERVAL_SECONDS`.

## Действия

Реестр строится в `internal/action/action.go` через `BuildRegistry(cfg)`:

```go
map[string]Action{
    "send_notification":  &NotificationAction{rustBaseURL, internalKey},
    "retry_notifications": &RetryNotificationsAction{rustBaseURL, internalKey},
}
```

### `send_notification`

Основное действие. `action_config`:

```json
{
  "channel": "telegram",       // "telegram" | "vk" | "both" (default: telegram)
  "recipient_type": "client",  // client | group | debtors | low_subscription | expiring_soon | ...
  "recipient_id": "uuid",      // client_id или group_id (для type client/group)
  "message": "Привет, {{name}}! Задолженность {{debt_amount}} руб."
}
```

Шаги:
1. Читает `notification_settings` для `profile_id`: токены Telegram/VK и флаги `*_enabled`.
2. Если нужный канал выключен для профиля — ошибка. Для `both` — если оба выключены, ошибка.
3. Собирает params для audience: для `client`/`group` — `{"client_id"|"group_id": recipient_id}`; для остальных — весь `action_config` как есть.
4. **HTTP POST на backend**: `POST {RUST_API_BASE_URL}/internal/scheduler/audience` с заголовком `X-Internal-Key: {INTERNAL_API_KEY}` и телом `{profile_id, audience_type, params}`. Возвращает `[]recipientInfo` (client_id, firstname, lastname, telegram_chat_id, vk_user_id, template_vars, subscription_id).
5. Инициализирует Telegram Bot **один раз** (без getMe-запроса на каждого получателя) с 10s HTTP-таймаутом.
6. Для каждого получателя: рендерит шаблон (`{{name}}`, `{{firstname}}`, `{{lastname}}` + любые ключи из `TemplateVars`), нерезолвнутые плейсхолдеры вырезаются регэкспом.
7. Отправляет через **per-profile rate limiter** (`sync.Map`):
   - Telegram: `40ms` между вызовами
   - VK: `55ms`
   Лимитер живёт в памяти процесса — при рестарте состояние теряется.
8. **Двухфазный коммит**: после успешной отправки — `POST /internal/scheduler/audience/confirm` с `subscription_ids` (для `low_subscription`/`expiring_soon`) и `client_ids`. Если backend не ответил — не фатально, lease истечёт через 15 мин на стороне бэкенда.

Итог: возвращает `(sent_count, warning, error)`. Ошибка возвращается только если **все** отправки провалились. Иначе — количество успешных + предупреждение с деталями.

### `retry_notifications`

Обход очереди повторных попыток:

1. `SELECT id FROM notification_retry_queue WHERE status = 'pending' AND next_attempt_at <= now() LIMIT 100`.
2. Если пусто — возвращает `(0, "", nil)`.
3. `POST {RUST_API_BASE_URL}/internal/notifications/retry` с `{"queue_ids": [...]}` и `X-Internal-Key`.
4. Ожидает ответ `{succeeded, failed, exhausted}`, возвращает `succeeded` как sent_count и предупреждение при `failed + exhausted > 0`.

Собственно повторную отправку выполняет backend — scheduler только триггерит.

## Переменные окружения

| Var | Обязателен | Default | Назначение |
|-----|-----------|---------|-----------|
| `DATABASE_URL` | да | — | Строка подключения к PostgreSQL. Без неё `log.Fatal`. |
| `POLL_INTERVAL_SECONDS` | нет | `900` | Интервал опроса БД в секундах. При `<= 0` подставляется 900. |
| `RUST_API_BASE_URL` | нет | `http://localhost:8080` | URL бэкенда для audience/confirm/retry-запросов. |
| `INTERNAL_API_KEY` | нет | `""` | Значение заголовка `X-Internal-Key`. Должен совпадать с конфигом бэкенда, иначе 401/403. |

Загружается через `godotenv.Load()` — файл `.env` в корне сервиса.

## CRUD джобов (через backend, не через scheduler)

Scheduler джобы **не создаёт**. Всё управление — через админ-API `step2pay-back`:

| Метод | Путь | Что делает |
|-------|------|-----------|
| GET | `/admin/scheduler/jobs` | Список |
| POST | `/admin/scheduler/jobs` | Создать |
| POST | `/admin/scheduler/jobs/:id` | Обновить |
| DELETE | `/admin/scheduler/jobs/:id` | Удалить |
| GET | `/admin/scheduler/jobs/:id/logs` | Логи последних запусков |

Реализация: `step2pay-back/src/admin/scheduler.rs`. Аудитории: `step2pay-back/src/internal/scheduler_audience.rs`.

## Известные ограничения

- **Минимальная гранулярность = `POLL_INTERVAL_SECONDS`**. Cron `*/5 * * * *` при `POLL_INTERVAL_SECONDS=900` будет срабатывать примерно раз в 15 минут, а не каждые 5. В prod values.yaml сейчас стоит `POLL_INTERVAL_SECONDS=60`.
- **Первый tick нового cron-job — no-op**: только инициализирует `next_run`. Действие сработает не раньше, чем на **следующем** тике после того как настанет `next_run`.
- **Event-триггер при первом запуске** (без `last_run`) смотрит назад на **15 минут** от `now()`. Более старые события не подхватываются.
- **Cron только в MSK** (`time.FixedZone("MSK", 3*60*60)`). Часовой пояс не настраивается через config.
- **Rate limiters — in-memory**. Каждый рестарт pod'а сбрасывает `sync.Map` → первые 40-55 мс после старта возможны burst-вызовы к Telegram/VK.
- **`RUST_API_BASE_URL` и `INTERNAL_API_KEY`** должны совпадать с конфигом backend'а. Иначе audience-запрос вернёт ошибку и job получит `status=error`.
- **Telegram/VK токены живут в таблице `notification_settings`** (per-profile). Если строка отсутствует или флаги выключены — action падает с понятной ошибкой.
- **Таблицы `clients`/`payments`** должны содержать `profile_id` и `created_at` — иначе event-триггер сломается.
- **Условие `condition`** реализовано только для `type=always`. Любое другое значение возвращает ошибку.

## Пробелы (что не сделано и стоило бы)

- **Синхронный `Execute` блокирует горутину** на всё время рассылки. Если у джоба много получателей и медленный Telegram — горутина висит N минут. Нет back-pressure и cancel по context'у внутри цикла отправки.
- **Нет batch/chunking** аудитории: 10 000 получателей → 10 000 последовательных вызовов Telegram API.
- **Нет circuit breaker** для Telegram/VK API — при массовых ошибках scheduler будет колотиться в упавший API до конца списка.
- **Нет Prometheus/OpenTelemetry-метрик** (jobs_run_total, notifications_sent_total, api_errors_total). Единственная наблюдаемость — `log.Printf` в stdout и `scheduler_job_logs`.
- **Нет health-check эндпоинта** — Kubernetes не сможет проверить живость процесса иначе как по TCP-порту (которого нет).
- **`condition` фактически не используется** — расширения (истечение абонементов, порог задолженности) не написаны; вместо этого условия зашиты в `audience_type` на бэкенде.
- **`event`-триггер полагается на sync между `last_run` scheduler'а и реальными `created_at`** — при остановке scheduler'а > 15 мин часть новых записей может быть пропущена (backlook только 15 мин).
- **Нет retry для fetchAudience/confirmDelivery** на уровне scheduler'а. Один сетевой сбой → job = error.

## Команды

Пользователь сам билдит бинарник и деплоит через `step2pay-infra/charts/scheduler`. Агент не запускает `docker build`, `helm upgrade`, `kubectl` и т.п.

Локальный запуск для проверки:

```bash
cd step2pay-scheduler
go mod tidy
go run ./cmd/main.go
go build ./cmd/main.go     # проверка компиляции
```

## Правила проекта

- Не создавать `_test.go` файлы, не писать `#[test]/#[cfg(test)]` (см. auto-memory `no-unit-tests`).
- Не делать `git add / commit / push` — коммиты только руками пользователя.
- Не редактировать существующие миграции; SQL-схема таблиц `scheduler_jobs`, `scheduler_job_logs`, `notification_settings`, `notification_retry_queue`, `clients`, `payments` живёт в `step2pay-back/migrations/`. Если нужно поле — создать новую миграцию **на бэкенде**.
- Билдить проект (`go build`, `docker build`) и деплоить — пользователь; агент может только читать код и править `.go`-файлы.

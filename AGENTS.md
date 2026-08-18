# SambaCRM scheduler

Go polling-сервис для отложенных уведомлений. CRUD и SQL-схема принадлежат
`step2pay-back`; scheduler только исполняет задания.

- Общайся с пользователем на русском.
- Не добавляй `_test.go` и не выполняй `git add`, commit, push или deploy.
- Не запускай `go build` и `go run`.
- Не создавай миграции здесь; новые схемы добавляй новой миграцией backend.
- Не меняй audience/confirm/retry-контракты без проверки backend и admin.
- Для продуктового OPSX-изменения читай артефакты в
  `../step2pay-back/openspec/changes/<change>/`.

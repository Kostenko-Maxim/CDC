# Публикация в GitHub как портфолио

Репозиторий готов к загрузке: `.env`, generated artifacts, локальные настройки
и исходный PDF исключены через `.gitignore`. Перед публикацией проверьте
`git status --short` и список подготовленных файлов.

1. Создайте в GitHub пустой репозиторий, например `postgres-kafka-s3-cdc`.
2. Добавьте описание: `PostgreSQL CDC pipeline with Debezium, Kafka, S3/MinIO, operational labs and end-to-end verification`.
3. Выполните из корня проекта, подставив свой URL:

```sh
git add .
git diff --cached --stat
git commit -m "Build local PostgreSQL Kafka S3 CDC portfolio project"
git remote add origin https://github.com/YOUR_USERNAME/postgres-kafka-s3-cdc.git
git push -u origin main
```

Если Git просит имя/почту, настройте свой реальный author identity перед commit.
Проект не прописывает выдуманного автора и не создаёт remote автоматически.

Рекомендуемые topics: `data-engineering`, `cdc`, `postgresql`, `debezium`,
`apache-kafka`, `kafka-connect`, `s3`, `docker-compose`, `python`.

Для демонстрации результата добавьте собственные скриншоты Kafka UI,
структуры бакета и успешно завершённого replay. Можно опубликовать небольшой
обезличенный JSON-отчёт проверки в `docs/`, просмотрев его содержимое.
Не включайте полный `.env`, access keys, worker logs с credentials или
заимствованный PDF в публичный репозиторий.

На интервью объясните четыре решения: зачем нужен replication slot,
почему RUNNING не доказывает доставку, как tombstone отличается от delete
event и почему schemas.enable нельзя бездумно переключать в заполненном топике.
Покажите `replay --compare` как проверку корректности конвейера.

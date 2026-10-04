# Проверка локальной реализации

Реальная локальная проверка выполнена **4 октября 2026 года** в Docker Desktop
с Linux containers (4 vCPU, 8 ГБ RAM) и Python 3.12 на Windows.

- Unit-тесты replay: **10/10 пройдены**.
- Компиляция Python: пройдена.
- `docker compose config --quiet`: пройдено.
- Сборка всех образов и запуск инфраструктуры: пройдены.
- Kafka UI и MinIO console: HTTP 200.
- Сквозная доставка: `c → u → d → tombstone` в Kafka и JSONL в S3; before image проверен.
- Snapshot: **20 124** события `r` доставлены в S3; генератор успел изменить исходные 20 000 заказов до snapshot.
- ALTER TABLE: новое поле `channel` обнаружено в `before` и `after` живых JSONL-событий.
- Parquet: файл прочитан через PyArrow, **652** строки; схема содержит `channel`.
- Replay: **21 132** события восстановили **20 635** текущих заказов.
  Множества ключей и проверяемые бизнес-поля совпали с PostgreSQL: **0 расхождений**.
- Heartbeat: `confirmed_flush_lsn` дополнительного idle source продвинулся после action query.
- Compaction: из журнала stock с offsets `0…3134` sink выгрузил **101 запись
  на 100 уникальных ключей**. Kafka сохранила пробелы offsets.
- Orphan slot: после удаления products source наблюдался неактивный слот с `wal_status=reserved`.
- Cleanup: дополнительные коннекторы, слоты и публикации удалены; основной конвейер сохранён.

Небольшие отчёты включены в репозиторий как примеры фактического запуска:
[delivery](verification-example.json), [replay](replay-example.json),
[Parquet](parquet-example.json), [heartbeat](heartbeat-example.json),
[compaction](compaction-example.json). Это результаты одного локального запуска;
при повторном запуске counts и offsets будут другими.

Машиночитаемые результаты сохраняются локально в игнорируемом `artifacts/`.
Документ не заявляет измерений throughput, production SLA или exactly-once.

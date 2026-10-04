# Практические эксперименты

Начните с `python cdc.py up` и `python cdc.py verify`.
Все команды выполняются из корня репозитория. Изменения конфигураций
можно применять без пересборки tools: манифесты смонтированы read-only.
`apply` и `register` также снимают паузу и запрашивают restart упавших задач.

## 1. Snapshot, лог и offsets

Откройте Kafka UI → `cdc.shop.orders` → Messages. Первые события имеют
`op=r`; затем идут `c`, `u`, `d` и null-value. Сравните количество событий
с `SELECT count(*) FROM shop.orders`: это разные показатели.

```sh
python cdc.py sql "SELECT count(*) FROM shop.orders"
python cdc.py sql "SHOW wal_level"
python cdc.py status
python cdc.py inspect --limit 1
```

В Kafka Connect UI проверьте отдельно connector state и task state.
Офсет sink-группы `connect-orders-s3-sink` сохраняется независимо от source.

## 2. Доставка insert/update/delete

```sh
python cdc.py verify
```

Проверка создаёт заказ с уникальной меткой, обновляет и удаляет его.
Сравните `artifacts/verification.json` с сообщениями Kafka и файлами S3.
Delete event имеет `before`, `after=null`, `op=d`. Следующая запись с тем же
ключом и `value=null` — tombstone. Она не является JSON-строкой `"null"`.

## 3. Backfill и маленькие файлы

Первые файлы содержат до 1 000 snapshot-записей. Текущий поток заполняет
намного меньшие файлы раз в ~10 секунд. Сравните records и bytes:

```sh
python cdc.py inspect --limit 3
```

Меняя `file.max.records` в `connectors/sink-s3.json`, вы меняете верхнюю
границу файла, но не интервал flush worker. Применение:

```sh
python cdc.py apply sink-s3.json
```

## 4. Пауза sink и восстановление

```sh
python cdc.py scenario pause-sink
python cdc.py status
```

Подождите ~30 секунд и откройте Consumers в Kafka UI. End offset растёт,
committed offset sink стоит, lag увеличивается. Source продолжает читать WAL.

```sh
python cdc.py scenario resume-sink
python cdc.py verify
```

Sink догоняет накопленную историю; уже подтверждённые offsets не сбрасываются.

## 5. Пауза source и WAL

```sh
python cdc.py scenario pause-source
python cdc.py status
```

Повторите status через ~30 секунд: слот становится неактивным, удержанный
WAL растёт при продолжающемся генераторе. Сравните с паузой sink.

```sh
python cdc.py scenario resume-source
python cdc.py verify
```

## 6. Idle publication и heartbeat

```sh
python cdc.py scenario heartbeat
```

Отдельный source захватывает неизменяемый справочник products.
Генератор при этом пишет за пределами его publication. Сценарий сначала
наблюдает позицию, затем включает захватываемую heartbeat-таблицу и action query.
Через ~35 секунд результат появляется в `artifacts/heartbeat.json`.
Смотрите также `retained_wal_bytes` и `confirmed_flush_lsn` в каталоге PostgreSQL:

```sh
python cdc.py sql "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_wal_bytes FROM pg_replication_slots"
python cdc.py cleanup
```

## 7. Осиротевший слот

```sh
python cdc.py scenario orphan
python cdc.py status
```

Дополнительный source регистрируется и удаляется. Его слот
`cdc_products_slot` и публикация остаются. Коннектора уже нет,
но PostgreSQL продолжает удерживать WAL.

```sh
python cdc.py cleanup
python cdc.py status
```

Cleanup удаляет дополнительные слоты только после остановки коннекторов
и проверки `active=false`. Не удаляйте слот работающего источника.

## 8. ALTER TABLE и новая форма файлов

```sh
python cdc.py scenario schema
python cdc.py verify
python cdc.py inspect --limit 1
```

Source продолжает работать. В новых `after` появляется `channel`;
старые файлы не переписываются. Новый контрольный заказ имеет `channel=mobile`.
Добавление поля проходит иначе, чем добавление таблицы: таблицу нужно
явно включить в `table.include.list` и применить source-манифест.
Автоматически включать новые таблицы проект не пытается.

## 9. JSON → Parquet и схема Kafka Connect

```sh
python cdc.py scenario parquet
```

Создаётся отдельная пара с topic prefix `cdc-pq`, schema/payload включены
с первого события. Sink выполняет unwrap и пишет Parquet.
Сценарий читает файл через PyArrow и сохраняет schema/rows в
`artifacts/parquet.json`. Основной JSONL-конвейер продолжает работать.

Почему не переключать основной sink на Parquet: в старом топике находятся
schemaless JSON-сообщения. Включение схемы у источника сегодня не меняет
сообщения, записанные вчера. Смешанный wire format ломает converter при backfill.
Для Parquet нужны схемы и стратегия изменения типов. В этом эксперименте
удаления отбрасываются; для analytics среза нужен самостоятельный merge-процесс.

```sh
python cdc.py cleanup
```

## 10. Compaction

```sh
python cdc.py scenario compaction
```

Сценарий занимает около 70 секунд: создаёт `shop.stock`, отдельный source
и компактируемый топик, пишет 100 ключей и 3 000 обновлений, затем
продолжает редкие записи для закрытия сегментов.

В Kafka UI сравните Clean Up Policy, segment size, границы offsets и
фактически доступные Messages. Разница offsets после compaction может
остаться большой: номера записей не перенумеровываются. Чистильщик работает
асинхронно; точное число записей в конкретную секунду не фиксировано.

Это отдельный топик, история заказов сохраняется. Настройки
`topic.creation.default.*` влияют только на вновь создаваемые топики.
После обновлений отдельный stock sink читает доступную историю с начала
и выгружает её в S3. `artifacts/compaction.json` содержит число фактически
прочитанных записей, 100 уникальных ключей и последний наблюдаемый offset.

```sh
python cdc.py cleanup
```

## 11. Replay и контроль состояния

```sh
python cdc.py replay --compare
```

Генератор останавливается. Проверка дожидается прохождения контрольного
tombstone до S3, после чего replay читает JSONL по партиции и offset,
применяет изменения и сравнивает заказы с базой. При несовпадении команда
возвращает ненулевой exit code и сохраняет различия в отчёте.

```sh
python cdc.py generator start
```

## 12. Task FAILED и восстановление

В Kafka UI изучите tasks коннектора и поле `trace`, не ограничивайтесь
верхней строкой connector state. Для намеренного эксперимента создайте
копию sink-манифеста с **новым именем**, неверным endpoint и тем же
топиком. Он не влияет на группу основного sink. Примените через `apply`.

Посмотрите REST status, worker logs и consumer lag. Некоторые ошибки
вызывают retries без FAILED: статус RUNNING не доказывает свежесть данных.
Чтобы удалить созданный вручную тестовый connector, используйте REST
`DELETE /connectors/<test-name>`. Встроенный cleanup знает только имена
сценариев проекта и не удаляет произвольные коннекторы.

## 13. Изменение состава таблиц

Новая таблица требует прав владельца, primary key и изменения
`table.include.list`; примените обновлённый source JSON через `apply`.
У already-running source автоматический initial snapshot добавленной
таблицы не гарантирован: для backfill предусмотрите incremental snapshot
или отдельный источник. DROP TABLE прекращает события таблицы, даже
если connector state продолжает показывать RUNNING.

## 14. Завершение

```sh
python cdc.py cleanup
python cdc.py down
```

Топики и файлы сохраняются для повторного анализа. Полный возврат к
исходному набору таблиц, offsets и файлов: `python cdc.py reset --yes`, затем `up`.

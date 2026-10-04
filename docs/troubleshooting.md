# Диагностика

| Симптом | Проверка | Действие |
|---|---|---|
| Нет соединения с Docker daemon | Docker Desktop → Linux containers | Запустить Docker Desktop |
| Порт занят | `.env`, `docker compose ps` | Заменить соответствующий host port |
| CONNECT RUNNING, задача FAILED | `/connectors/<name>/status`, `trace` | Исправить причину; применить конфигурацию или restart task |
| Оба RUNNING, S3 не обновляется | Consumer lag, topic name, `logs connect` | Проверить подписку, endpoint и разрешения; искать retries |
| Source RUNNING, слот inactive | `pg_replication_slots`, worker logs | Проверить подключение, публикацию и retries |
| WAL растёт при простое таблиц | `restart_lsn`, публикация, heartbeat query | Включить транзакцию heartbeat в захватываемой публикации |
| Слот inactive после удаления source | REST список коннекторов, слоты | Убедиться, что source удалён; удалить слот и публикацию |
| Verify timeout | `status`, `logs connect`, Kafka UI | Снять паузу; проверить задачи и доставку файлов |
| Parquet converter error | Формат сообщения и converter schemas | Использовать отдельный топик с единым wire format |
| Replay mismatch | Генератор, пауза sink, исходная полнота журнала | Остановить генератор, снять паузы и проверить доставку |
| MinIO не скачивается как готовый image | Dockerfile MinIO | В проекте используется сборка официальных исходников |
| Compaction ещё не видна | Закрытые сегменты, cleaner logs | Продолжить редкую запись и дождаться cleaner |
| Новый пароль роли не работает | Сохранённый PostgreSQL volume | ALTER ROLE или явный reset стенда |

```sh
python cdc.py status
python cdc.py logs connect
python cdc.py logs postgres
python cdc.py logs minio
python cdc.py scenario restore
python cdc.py verify
```

`restore` применяет текущие основные манифесты из `connectors/` и снимает
паузы. Если вы меняли файлы руками, сначала верните необходимые значения.
Добавленные колонки и таблицы эта команда не удаляет.

При ошибке скачивания зависимостей повторите `up`: Docker использует уже
собранные слои. Для корпоративных сетей настройте proxy Docker/BuildKit;
сборке нужен доступ к Docker Hub, GHCR, GitHub, Maven Central, Apache Archive,
Go proxy, PyPI и apt-репозиториям.

Не удаляйте replication slot для исправления lag, пока источник жив.
Удаление слота может сделать пропущенный диапазон WAL недоступным,
а сохранённый Connect offset сам по себе этого диапазона не восстановит.

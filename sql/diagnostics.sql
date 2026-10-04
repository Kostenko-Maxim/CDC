SELECT slot_name, plugin, active, wal_status, restart_lsn, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_wal_bytes
FROM pg_replication_slots;
SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1, 2, 3;
SELECT count(*) AS orders FROM shop.orders;

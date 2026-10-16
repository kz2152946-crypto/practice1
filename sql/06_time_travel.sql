-- Time travel и восстановление из снапшота на silver.lakehouse.dim_customer.
-- Запуск: bash scripts/time_travel.sh (подставляет snapshot_id), либо по шагам в DBeaver.
-- ${GOOD} — id снапшота до "аварии", ${TS} — время, когда таблица была целой.

-- 0. Контрольная сумма "хорошего" состояния
SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer;

SELECT snapshot_id, parent_id, operation, committed_at
FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at;

-- 1. "Авария": случайно удалили всех клиентов из Японии и обнулили баланс у Германии
DELETE FROM silver.lakehouse.dim_customer WHERE nation_name = 'JAPAN';
UPDATE silver.lakehouse.dim_customer SET acct_balance = 0 WHERE nation_name = 'GERMANY';

SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer;

SELECT snapshot_id, parent_id, operation, committed_at, summary['deleted-records'] AS deleted_records
FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at;

-- 2. Time travel: читаем таблицу как она была до аварии
SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer FOR VERSION AS OF ${GOOD};

SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer FOR TIMESTAMP AS OF TIMESTAMP '${TS}';

-- 3а. Точечное восстановление: вернуть только пострадавшие строки из старой версии
--     (удалить испорченные + вставить строки, которых нет / которые отличаются)
DELETE FROM silver.lakehouse.dim_customer WHERE nation_name = 'GERMANY';
INSERT INTO silver.lakehouse.dim_customer
SELECT * FROM silver.lakehouse.dim_customer FOR VERSION AS OF ${GOOD}
WHERE nation_name IN ('JAPAN', 'GERMANY');

SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer;

-- проверка: текущая таблица == версия до аварии (0 и 0)
SELECT
  (SELECT count(*) FROM (SELECT * FROM silver.lakehouse.dim_customer
                         EXCEPT SELECT * FROM silver.lakehouse.dim_customer FOR VERSION AS OF ${GOOD})) AS cur_minus_good,
  (SELECT count(*) FROM (SELECT * FROM silver.lakehouse.dim_customer FOR VERSION AS OF ${GOOD}
                         EXCEPT SELECT * FROM silver.lakehouse.dim_customer)) AS good_minus_cur;

-- 3б. Полный откат: снова "ломаем" и откатываем всю таблицу на снапшот
DELETE FROM silver.lakehouse.dim_customer WHERE customer_key <= 1000;
SELECT count(*) AS rows_after_second_accident FROM silver.lakehouse.dim_customer;

ALTER TABLE silver.lakehouse.dim_customer EXECUTE rollback_to_snapshot(${GOOD});

SELECT count(*) AS rows, sum(acct_balance) AS sum_balance
FROM silver.lakehouse.dim_customer;

SELECT snapshot_id, parent_id, operation, committed_at, is_current_ancestor
FROM silver.lakehouse."dim_customer$snapshots" s
JOIN silver.lakehouse."dim_customer$history" h USING (snapshot_id)
ORDER BY committed_at;

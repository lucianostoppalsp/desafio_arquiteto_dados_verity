-- ============================================================
-- Queries para validação do pipeline orders_pipeline_dag
-- As datas da janela de tempo devem utilizar os mesmos valores usados para a execução da DAG
-- Exemplo de janela de tempo:
--   reference_date = 2026-03-20
--   lookback_days  = 7
--   start_date     = 2026-03-13
--   end_date       = 2026-03-20
-- ============================================================

-- ============================================================
-- Parâmetros da janela
-- Ajuste apenas estes valores
-- ============================================================
\set reference_date '2026-03-20'
\set lookback_days 7

SELECT
    DATE :'reference_date' AS reference_date,
    :'lookback_days'::int AS lookback_days,
    (DATE :'reference_date' - :'lookback_days'::int) AS start_date,
    DATE :'reference_date' AS end_date
\gset

\echo reference_date = :reference_date
\echo lookback_days  = :lookback_days
\echo start_date     = :start_date
\echo end_date       = :end_date


\echo =========================================================
\echo 1) Contagens básicas
\echo =========================================================
SELECT COUNT(*) AS orders_raw_count
FROM raw.orders_raw
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date';

-- Deduplica apenas cópias exatas, diferentes status para mesmo cliente e pedido permanecem na tabela
SELECT COUNT(*) AS orders_events_dedup_count
FROM refined.orders_events_dedup
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date';

-- Aqui a visão é do status mais recente de cada pedido; as linhas com status anteriores são eliminadas
-- Dados sumarizados por cliente e business date

SELECT COUNT(*) AS customer_orders_daily_count
FROM analytics.customer_orders_daily
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date';

-- Visão global por business date
SELECT COUNT(*) AS orders_daily_summary_count
FROM analytics.orders_daily_summary
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date';


\echo =========================================================
\echo 2) Comparação das linhas da Raw com a refined por business_date
\echo Serve para observar redução de volume após a deduplicação.
\echo =========================================================
SELECT
    business_date,
    COUNT(*) AS raw_rows
FROM raw.orders_raw
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY business_date
ORDER BY business_date;

SELECT
    business_date,
    COUNT(*) AS dedup_rows
FROM refined.orders_events_dedup
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY business_date
ORDER BY business_date;

\echo =========================================================
\echo 3) Na tabela da camada refined não pode haver duplicidades. Esta query serve para diagnosticar isso.
\echo Chave de deduplicação:
\echo (order_id, customer_id, status, amount, business_date)
\echo Resultado esperado: zero linhas.
\echo =========================================================
SELECT
    order_id,
    customer_id,
    status,
    amount,
    business_date,
    COUNT(*) AS qty
FROM refined.orders_events_dedup
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY
    order_id,
    customer_id,
    status,
    amount,
    business_date
HAVING COUNT(*) > 1;


\echo =========================================================
\echo 4) Lista as linhas candidatas à duplicidade na tabela raw para comparar contra a refined
\echo =========================================================
SELECT
    order_id,
    customer_id,
    status,
    amount,
    business_date,
    COUNT(*) AS qty,
    MIN(ingested_at) AS min_ingested_at,
    MAX(ingested_at) AS max_ingested_at
FROM raw.orders_raw
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY
    order_id,
    customer_id,
    status,
    amount,
    business_date
HAVING COUNT(*) > 1
ORDER BY business_date, order_id;


\echo ============================================================
\echo 5) Lista as linhas carregadas na refined na janela de tempo
\echo ============================================================
SELECT
    event_id,
    order_id,
    customer_id,
    status,
    amount,
    business_date,
    ingested_at,
    source_file,
    processing_ts
FROM refined.orders_events_dedup
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
ORDER BY business_date, order_id, customer_id, status, ingested_at;


\echo ============================================================
\echo 6) Lista as linhas carregadas na camada analytics com agrupamento por cliente/business date na janela de tempo
\echo ============================================================
SELECT
    business_date,
    customer_id,
    total_orders,
    pending_orders,
    paid_orders,
    canceled_orders,
    total_amount,
    pending_amount,
    paid_amount,
    canceled_amount,
    last_ingested_at,
    processing_ts
FROM analytics.customer_orders_daily
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
ORDER BY business_date, customer_id;


\echo ============================================================
\echo 7) Refaz a seleção das linhas para a customer_orders_daily a partir da refined
\echo Valida a lógica da captura da linha mais recente de cada pedido
\echo Comparar o resultado com a query acima; se der divergência, tem problema no processo de carga
\echo ============================================================
WITH latest_order_state AS (
    SELECT *
    FROM (
        SELECT
            event_id,
            order_id,
            customer_id,
            status,
            amount,
            business_date,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, customer_id, business_date
                ORDER BY ingested_at DESC, event_id DESC
            ) AS rn
        FROM refined.orders_events_dedup
        WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
    ) t
    WHERE rn = 1
)
SELECT
    business_date,
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_orders,
    SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
    SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
    SUM(amount) AS total_amount,
    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_amount,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status = 'canceled' THEN amount ELSE 0 END) AS canceled_amount,
    MAX(ingested_at) AS last_ingested_at
FROM latest_order_state
GROUP BY business_date, customer_id
ORDER BY business_date, customer_id;


\echo ============================================================
\echo 8) Não podemos ter duplicidade de linhas usando a chave business_date/customer_id na customer_orders_daily
\echo Resultado esperado: 0 linhas.
\echo ============================================================
SELECT
    business_date,
    customer_id,
    COUNT(*) AS qty
FROM analytics.customer_orders_daily
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY business_date, customer_id
HAVING COUNT(*) > 1;


\echo ============================================================
\echo 9) Lista linhas carregadas na daily summary na janela de tempo
\echo ============================================================
SELECT
    business_date,
    total_customers,
    total_orders,
    pending_orders,
    paid_orders,
    canceled_orders,
    total_amount,
    pending_amount,
    paid_amount,
    canceled_amount,
    last_ingested_at,
    processing_ts
FROM analytics.orders_daily_summary
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
ORDER BY business_date;


\echo ============================================================
\echo 10) Refaz a seleção das linhas para a orders_daily_summary a partir da customer_orders_daily
\echo Valida a lógica da sumarização por business_date
\echo Comparar o resultado com a query acima; se der divergência, tem problema no pipeline
\echo ============================================================
SELECT
    business_date,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(total_orders) AS total_orders,
    SUM(pending_orders) AS pending_orders,
    SUM(paid_orders) AS paid_orders,
    SUM(canceled_orders) AS canceled_orders,
    SUM(total_amount) AS total_amount,
    SUM(pending_amount) AS pending_amount,
    SUM(paid_amount) AS paid_amount,
    SUM(canceled_amount) AS canceled_amount,
    MAX(last_ingested_at) AS last_ingested_at
FROM analytics.customer_orders_daily
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY business_date
ORDER BY business_date;


\echo ============================================================
\echo 11) Não podem existir linhas duplicadas com mesma business_date em orders_daily_summary
\echo Resultado esperado: 0 linhas.
\echo ============================================================
SELECT
    business_date,
    COUNT(*) AS qty
FROM analytics.orders_daily_summary
WHERE business_date BETWEEN DATE :'start_date' AND DATE :'end_date'
GROUP BY business_date
HAVING COUNT(*) > 1;


\echo ============================================================
\echo 12) Não podemos ter a coluna processing_ts com valor null em qualquer das tabelas carregadas pelo pipeline
\echo Resultado esperado: 0 linhas em todas as 3 queries.
\echo ============================================================
SELECT COUNT(*) AS null_processing_ts_refined
FROM refined.orders_events_dedup
WHERE processing_ts IS NULL;

SELECT COUNT(*) AS null_processing_ts_customer
FROM analytics.customer_orders_daily
WHERE processing_ts IS NULL;

SELECT COUNT(*) AS null_processing_ts_summary
FROM analytics.orders_daily_summary
WHERE processing_ts IS NULL;


\echo ============================================================
\echo 13) Listagem completa da tabela final orders_daily_summary independente da janela de tempo
\echo ============================================================
SELECT
    business_date,
    total_customers,
    total_orders,
    pending_orders,
    paid_orders,
    canceled_orders,
    total_amount,
    pending_amount,
    paid_amount,
    canceled_amount
FROM analytics.orders_daily_summary
ORDER BY business_date;
TRUNCATE TABLE raw.orders_raw RESTART IDENTITY;

INSERT INTO raw.orders_raw (
    order_id,
    customer_id,
    status,
    amount,
    business_date,
    ingested_at,
    source_file
) VALUES

-- =========================
-- business_date = 2026-03-01
-- =========================

-- cliente 501 | pedido com atualização real de status
(1001, 501, 'pending', 120.00, '2026-03-01', '2026-03-01 08:00:00', 'batch_001'),
(1001, 501, 'paid',    120.00, '2026-03-01', '2026-03-01 10:15:00', 'batch_001_update'),

-- cliente 502 | pedido pago com duplicidade técnica
(1002, 502, 'paid',     80.00, '2026-03-01', '2026-03-01 08:05:00', 'batch_001'),
(1002, 502, 'paid',     80.00, '2026-03-01', '2026-03-01 08:09:00', 'batch_001_retry'),

-- cliente 505 | pedido cancelado
(1003, 505, 'canceled', 60.00, '2026-03-01', '2026-03-01 08:20:00', 'batch_001'),

-- =========================
-- business_date = 2026-03-03
-- =========================

-- cliente 502 | pedido com cancelamento posterior
(1004, 502, 'pending',  200.00, '2026-03-03', '2026-03-03 09:00:00', 'batch_002'),
(1004, 502, 'canceled', 200.00, '2026-03-03', '2026-03-03 12:10:00', 'batch_002_update'),

-- cliente 503 | registro tardio + duplicidade técnica tardia
(1005, 503, 'paid',     150.00, '2026-03-03', '2026-03-04 07:45:00', 'batch_late_002'),
(1005, 503, 'paid',     150.00, '2026-03-03', '2026-03-04 07:50:00', 'batch_late_002_retry'),

-- cliente 503 | pedido normal pending
(1006, 503, 'pending',   95.00, '2026-03-03', '2026-03-03 10:05:00', 'batch_002'),

-- =========================
-- business_date = 2026-03-05
-- =========================

-- cliente 501 | pedido pago com duplicidade técnica
(1007, 501, 'paid',     300.00, '2026-03-05', '2026-03-05 08:00:00', 'batch_003'),
(1007, 501, 'paid',     300.00, '2026-03-05', '2026-03-05 08:06:00', 'batch_003_retry'),

-- cliente 504 | pedido com atualização tardia de status
(1008, 504, 'pending',  220.00, '2026-03-05', '2026-03-05 08:15:00', 'batch_003'),
(1008, 504, 'paid',     220.00, '2026-03-05', '2026-03-06 09:30:00', 'batch_003_update_late'),

-- cliente 501 | pedido cancelado
(1009, 501, 'canceled',  40.00, '2026-03-05', '2026-03-05 11:00:00', 'batch_003'),

-- =========================
-- business_date = 2026-03-08
-- =========================

-- cliente 502 | pedido pago normal
(1010, 502, 'paid',     500.00, '2026-03-08', '2026-03-08 08:02:00', 'batch_004'),

-- cliente 504 | pedido pending com duplicidade técnica
(1011, 504, 'pending',  130.00, '2026-03-08', '2026-03-08 08:20:00', 'batch_004'),
(1011, 504, 'pending',  130.00, '2026-03-08', '2026-03-08 08:25:00', 'batch_004_retry'),

-- cliente 505 | pedido com atualização tardia para paid
(1012, 505, 'pending',   75.00, '2026-03-08', '2026-03-08 09:10:00', 'batch_004'),
(1012, 505, 'paid',      75.00, '2026-03-08', '2026-03-10 07:00:00', 'batch_004_update_late'),

-- cliente 504 | pedido cancelado
(1013, 504, 'canceled',  90.00, '2026-03-08', '2026-03-08 11:15:00', 'batch_004'),

-- =========================
-- business_date = 2026-03-11
-- =========================

-- cliente 501 | pedido com atualização real no mesmo dia
(1014, 501, 'pending',  210.00, '2026-03-11', '2026-03-11 08:00:00', 'batch_005'),
(1014, 501, 'paid',     210.00, '2026-03-11', '2026-03-11 08:40:00', 'batch_005_update'),

-- cliente 503 | pedido cancelado
(1015, 503, 'canceled', 115.00, '2026-03-11', '2026-03-11 09:00:00', 'batch_005'),

-- cliente 504 | registro tardio + duplicidade técnica tardia
(1016, 504, 'paid',     330.00, '2026-03-11', '2026-03-13 10:30:00', 'batch_late_005'),
(1016, 504, 'paid',     330.00, '2026-03-11', '2026-03-13 10:35:00', 'batch_late_005_retry'),

-- =========================
-- business_date = 2026-03-15
-- =========================

-- cliente 501 | pago com duplicidade técnica
(1017, 501, 'paid',     120.00, '2026-03-15', '2026-03-15 08:00:00', 'batch_006'),
(1017, 501, 'paid',     120.00, '2026-03-15', '2026-03-15 08:03:00', 'batch_006_retry'),

-- cliente 502 | pending -> paid com atualização tardia
(1018, 502, 'pending',  240.00, '2026-03-15', '2026-03-15 08:10:00', 'batch_006'),
(1018, 502, 'paid',     240.00, '2026-03-15', '2026-03-16 07:55:00', 'batch_006_update_late'),

-- cliente 503 | pending -> paid com atualização ainda mais tardia
(1019, 503, 'pending',  180.00, '2026-03-15', '2026-03-15 08:30:00', 'batch_006'),
(1019, 503, 'paid',     180.00, '2026-03-15', '2026-03-17 09:20:00', 'batch_006_update_late2'),

-- cliente 505 | cancelado
(1020, 505, 'canceled',  55.00, '2026-03-15', '2026-03-15 10:00:00', 'batch_006'),

-- =========================
-- business_date = 2026-03-20
-- =========================

-- cliente 501 | pending com duplicidade técnica
(1021, 501, 'pending',  260.00, '2026-03-20', '2026-03-20 08:00:00', 'batch_007'),
(1021, 501, 'pending',  260.00, '2026-03-20', '2026-03-20 08:04:00', 'batch_007_retry'),

-- cliente 502 | cancelado
(1022, 502, 'canceled', 145.00, '2026-03-20', '2026-03-20 08:15:00', 'batch_007'),

-- cliente 504 | pago tardio
(1023, 504, 'paid',     390.00, '2026-03-20', '2026-03-21 06:50:00', 'batch_late_007'),

-- cliente 505 | pending -> canceled com atualização tardia
(1024, 505, 'pending',   99.00, '2026-03-20', '2026-03-20 09:10:00', 'batch_007'),
(1024, 505, 'canceled',  99.00, '2026-03-20', '2026-03-22 11:00:00', 'batch_007_update_late');
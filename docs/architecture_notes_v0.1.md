# Notas de Arquitetura e Decisões Técnicas (v0.1)

## Decisão 1 — Containerizar o ambiente local
**Escolha:** Docker Compose com Postgres e Airflow.

**Motivação:** reduzir dependência da máquina local, facilitar reprodução e tornar a demonstração mais consistente.

**Alternativas descartadas:** instalar Postgres, Spark e Airflow diretamente no sistema operacional.

**Trade-off:** setup inicial de contêineres é um pouco mais trabalhoso, mas a operação posterior fica mais previsível.

## Decisão 2 — Manter Airflow apenas como orquestrador
**Escolha:** DAG fina, com a lógica de negócio no job PySpark.

**Motivação:** melhorar separação de responsabilidades, legibilidade e capacidade de evolução.

**Alternativas descartadas:** concentrar regras de transformação em operators Python dentro da DAG.

**Trade-off:** exige uma estrutura de projeto um pouco mais organizada, mas evita acoplamento excessivo entre agendamento e transformação.

## Decisão 3 — Usar janela móvel de reprocessamento
**Escolha:** processar uma `reference_date` com `lookback_days`.

**Motivação:** capturar registros tardios e permitir reprocessamento controlado.

**Alternativas descartadas:** processar apenas o dia corrente.

**Trade-off:** reprocessa um volume um pouco maior, porém aumenta consistência e simplifica a lógica operacional.

## Decisão 4 — Deduplicar por atributos de negócio
**Escolha:** considerar como duplicidade técnica a repetição de `order_id`, `customer_id`, `status`, `amount` e `business_date`, mantendo o registro mais recente por `ingested_at`.

**Motivação:** o enunciado sugere reenvios e retries que podem gerar múltiplas ocorrências do mesmo evento lógico.

**Alternativas descartadas:** usar apenas `event_id` como chave única de deduplicação.

**Trade-off:** exige uma interpretação semântica da origem, mas produz um resultado mais aderente ao problema de negócio.

## Decisão 5 — Consolidar a camada analítica principal por cliente e dia
**Escolha:** `analytics.customer_orders_daily` como base principal.

**Motivação:** preservar a dimensão temporal do problema e gerar uma base mais reutilizável para análises futuras.

**Alternativas descartadas:** produzir apenas um resumo diário direto.

**Trade-off:** aumenta um pouco o número de camadas, mas melhora reuso e flexibilidade analítica.

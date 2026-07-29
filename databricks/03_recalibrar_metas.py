import os, urllib.request, json, time, random

HOST = os.environ["DATABRICKS_HOST"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
SCHEMA = "workspace.bronze_bebidas"

random.seed(42)

def _call(method, path, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"https://{HOST}{path}", data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode())

def sql(stmt, max_wait=300):
    r = _call("POST", "/api/2.0/sql/statements", {"warehouse_id": WAREHOUSE, "statement": stmt, "wait_timeout": "50s"}, timeout=65)
    stmt_id = r["statement_id"]
    start = time.time()
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() - start > max_wait:
            raise RuntimeError(f"Timeout {stmt_id}")
        time.sleep(3)
        r = _call("GET", f"/api/2.0/sql/statements/{stmt_id}", timeout=30)
    if r.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(json.dumps(r.get("status"), indent=2))
    return r

REVENDAS = ["REV_G1", "REV_G2", "REV_G3", "REV_G4", "REV_M1", "REV_M2", "REV_M3", "REV_M4"]

# Um unico fator de performance por revenda (aplicado sobre Volume em caixas).
# Hectolitro e Faturamento NAO recebem fator proprio: sao derivados do Volume
# multiplicando pela razao real HL/caixa e R$/caixa daquela revenda (mix real
# de produtos vendidos) — garante coerencia entre os 3 KPIs por construcao,
# em vez de 3 sorteios independentes que podem nao bater entre si.
fator_volume_values = []
for rev in REVENDAS:
    fator = round(random.uniform(0.85, 1.15), 4)
    fator_volume_values.append(f"('{rev}', {fator})")

# Devolucoes continua com fator proprio (nao tem razao de conversao com Volume).
fator_devol_values = []
for rev in REVENDAS:
    fator = round(random.uniform(0.85, 1.15), 4)
    fator_devol_values.append(f"('{rev}', {fator})")

print("--- Criando tabelas de fatores ---")
sql(f"CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_tmp_fator_volume (cod_revenda STRING, fator DOUBLE)")
sql(f"INSERT INTO {SCHEMA}.TBCERVA_tmp_fator_volume VALUES {','.join(fator_volume_values)}")
sql(f"CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_tmp_fator_devol (cod_revenda STRING, fator DOUBLE)")
sql(f"INSERT INTO {SCHEMA}.TBCERVA_tmp_fator_devol VALUES {','.join(fator_devol_values)}")

print("--- Apagando metas antigas dos KPIs de fluxo ---")
sql(f"DELETE FROM {SCHEMA}.TBCERVA_fato_metas WHERE kpi IN ('Volume','Hectolitro','Faturamento','Devolucoes')")

print("--- Razao real HL/caixa e R$/caixa por revenda (mix real de produtos, todo o periodo) ---")
sql(f"""
    CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_tmp_razao_revenda AS
    SELECT cod_revenda,
           SUM(volume_hl) / SUM(qtd_caixas) AS hl_por_caixa,
           SUM(valor_total_reais) / SUM(qtd_caixas) AS reais_por_caixa
    FROM {SCHEMA}.TBCERVA_gold_vendas_dia
    GROUP BY cod_revenda
""")
r = sql(f"SELECT * FROM {SCHEMA}.TBCERVA_tmp_razao_revenda ORDER BY cod_revenda")
for row in r["result"]["data_array"]:
    print(row)

print("--- Inserindo metas recalibradas e coerentes ---")
sql(f"""
    INSERT INTO {SCHEMA}.TBCERVA_fato_metas
    WITH realizado_vendas AS (
      SELECT cod_revenda, ano, mes, CONCAT(ano,'-',LPAD(mes,2,'0')) AS ano_mes, SUM(qtd_caixas) AS vol_caixas
      FROM {SCHEMA}.TBCERVA_gold_vendas_dia
      GROUP BY cod_revenda, ano, mes
    ),
    realizado_devol AS (
      SELECT cod_revenda, ano, mes, SUM(valor_total_reais) AS devol_valor
      FROM {SCHEMA}.TBCERVA_gold_devolucao_dia
      GROUP BY cod_revenda, ano, mes
    ),
    dias_uteis AS (
      SELECT ano, mes, SUM(CAST(dia_util AS INT)) AS qtd_dias_uteis
      FROM {SCHEMA}.TBCERVA_dim_calendario
      GROUP BY ano, mes
    ),
    base AS (
      SELECT v.cod_revenda AS cod_revenda, v.ano_mes AS ano_mes,
             v.vol_caixas AS vol_caixas, COALESCE(d.devol_valor, 0) AS devol_valor,
             du.qtd_dias_uteis AS qtd_dias_uteis
      FROM realizado_vendas v
      LEFT JOIN realizado_devol d ON v.cod_revenda = d.cod_revenda AND v.ano = d.ano AND v.mes = d.mes
      JOIN dias_uteis du ON v.ano = du.ano AND v.mes = du.mes
    ),
    metas_volume AS (
      -- jitter mensal (+-5%) por cima do fator fixo da revenda, so pra nao dar
      -- exatamente o mesmo desvio % todo mes — HL e Faturamento herdam o mesmo
      -- jitter (via meta_volume) porque sao derivados dele, entao continuam coerentes
      SELECT base.ano_mes AS ano_mes, base.cod_revenda AS cod_revenda,
             base.vol_caixas * fv.fator * (0.95 + RAND() * 0.10) AS meta_volume,
             base.qtd_dias_uteis AS qtd_dias_uteis
      FROM base JOIN {SCHEMA}.TBCERVA_tmp_fator_volume fv ON base.cod_revenda = fv.cod_revenda
    )
    SELECT mv.ano_mes AS ano_mes, mv.cod_revenda AS cod_revenda, 'Volume' AS kpi, 'Fluxo' AS tipo_meta,
           ROUND(mv.meta_volume, 2) AS meta_mensal,
           ROUND(mv.meta_volume / mv.qtd_dias_uteis, 2) AS meta_dia_util,
           'Maior e melhor' AS direcao_meta
    FROM metas_volume mv
    UNION ALL
    SELECT mv.ano_mes, mv.cod_revenda, 'Hectolitro', 'Fluxo',
           ROUND(mv.meta_volume * rr.hl_por_caixa, 2),
           ROUND(mv.meta_volume * rr.hl_por_caixa / mv.qtd_dias_uteis, 2),
           'Maior e melhor'
    FROM metas_volume mv JOIN {SCHEMA}.TBCERVA_tmp_razao_revenda rr ON mv.cod_revenda = rr.cod_revenda
    UNION ALL
    SELECT mv.ano_mes, mv.cod_revenda, 'Faturamento', 'Fluxo',
           ROUND(mv.meta_volume * rr.reais_por_caixa, 2),
           ROUND(mv.meta_volume * rr.reais_por_caixa / mv.qtd_dias_uteis, 2),
           'Maior e melhor'
    FROM metas_volume mv JOIN {SCHEMA}.TBCERVA_tmp_razao_revenda rr ON mv.cod_revenda = rr.cod_revenda
    UNION ALL
    SELECT base.ano_mes, base.cod_revenda, 'Devolucoes', 'Fluxo',
           ROUND(base.devol_valor * fd.fator, 2),
           ROUND(base.devol_valor * fd.fator / base.qtd_dias_uteis, 2),
           'Menor e melhor'
    FROM base JOIN {SCHEMA}.TBCERVA_tmp_fator_devol fd ON base.cod_revenda = fd.cod_revenda
""")

sql(f"DROP TABLE {SCHEMA}.TBCERVA_tmp_fator_volume")
sql(f"DROP TABLE {SCHEMA}.TBCERVA_tmp_fator_devol")
sql(f"DROP TABLE {SCHEMA}.TBCERVA_tmp_razao_revenda")

print("--- Conferindo coerencia: Jan/2020, todas as revendas ---")
r = sql(f"""
    SELECT m.kpi, SUM(m.meta_mensal) AS meta_total,
           CASE WHEN m.kpi='Volume' THEN (SELECT SUM(qtd_caixas) FROM {SCHEMA}.TBCERVA_gold_vendas_dia WHERE data BETWEEN '2020-01-01' AND '2020-01-31')
                WHEN m.kpi='Hectolitro' THEN (SELECT SUM(volume_hl) FROM {SCHEMA}.TBCERVA_gold_vendas_dia WHERE data BETWEEN '2020-01-01' AND '2020-01-31')
                WHEN m.kpi='Faturamento' THEN (SELECT SUM(valor_total_reais) FROM {SCHEMA}.TBCERVA_gold_vendas_dia WHERE data BETWEEN '2020-01-01' AND '2020-01-31')
           END AS realizado_total
    FROM {SCHEMA}.TBCERVA_fato_metas m
    WHERE m.ano_mes='2020-01' AND m.kpi IN ('Volume','Hectolitro','Faturamento')
    GROUP BY m.kpi
""")
for row in r["result"]["data_array"]:
    kpi, meta, real = row
    real = float(real); meta = float(meta)
    print(f"{kpi}: realizado={real:,.0f}  meta={meta:,.0f}  desvio={((real-meta)/meta*100):+.1f}%")

print("--- Coerencia interna da META (R$/HL e HL/caixa implicitos batem com o real?) ---")
r = sql(f"""
    SELECT cod_revenda,
      SUM(CASE WHEN kpi='Faturamento' THEN meta_mensal END) / SUM(CASE WHEN kpi='Hectolitro' THEN meta_mensal END) AS reais_por_hl_meta,
      SUM(CASE WHEN kpi='Hectolitro' THEN meta_mensal END) / SUM(CASE WHEN kpi='Volume' THEN meta_mensal END) AS hl_por_caixa_meta
    FROM {SCHEMA}.TBCERVA_fato_metas WHERE ano_mes='2020-06' AND kpi IN ('Volume','Hectolitro','Faturamento')
    GROUP BY cod_revenda ORDER BY cod_revenda
""")
for row in r["result"]["data_array"]:
    print(row)

print("--- Sazonalidade preservada? (Dez vs Jul, Hectolitro, 2020) ---")
r = sql(f"""
    SELECT SUBSTR(ano_mes,6,2) AS mes, SUM(meta_mensal)
    FROM {SCHEMA}.TBCERVA_fato_metas WHERE kpi='Hectolitro' AND ano_mes IN ('2020-12','2020-07')
    GROUP BY SUBSTR(ano_mes,6,2)
""")
print(r["result"]["data_array"])

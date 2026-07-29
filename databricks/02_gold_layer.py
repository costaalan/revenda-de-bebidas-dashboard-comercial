import os, urllib.request, json, time

HOST = os.environ["DATABRICKS_HOST"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
SCHEMA = "workspace.bronze_bebidas"

def _call(method, path, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"https://{HOST}{path}",
        data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode())

def sql(stmt, max_wait=600):
    r = _call("POST", "/api/2.0/sql/statements", {
        "warehouse_id": WAREHOUSE, "statement": stmt, "wait_timeout": "50s"
    }, timeout=65)
    stmt_id = r["statement_id"]
    start = time.time()
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() - start > max_wait:
            raise RuntimeError(f"Timeout aguardando statement {stmt_id}")
        time.sleep(5)
        r = _call("GET", f"/api/2.0/sql/statements/{stmt_id}", timeout=30)
    state = r.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(json.dumps(r.get("status"), indent=2))
    return r

statements = {

"gold_vendas_dia": f"""
CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_gold_vendas_dia AS
SELECT
  v.data,
  c.ano, c.mes, c.trimestre, c.semana_ano,
  e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional,
  e.porte_revenda,
  p.cod_sku, p.marca, p.tipo_produto, p.categoria_embalagem, p.linha_produto,
  SUM(v.qtd_caixas) AS qtd_caixas,
  SUM(v.qtd_unidades) AS qtd_unidades,
  SUM(v.volume_litros) AS volume_litros,
  SUM(v.volume_hl) AS volume_hl,
  SUM(v.valor_total_reais) AS valor_total_reais
FROM {SCHEMA}.TBCERVA_fato_vendas v
JOIN {SCHEMA}.TBCERVA_dim_calendario c ON v.data = c.data
JOIN {SCHEMA}.TBCERVA_dim_estrutura_comercial e ON v.cod_vendedor = e.cod_vendedor
JOIN {SCHEMA}.TBCERVA_dim_produto p ON v.cod_sku = p.cod_sku
GROUP BY v.data, c.ano, c.mes, c.trimestre, c.semana_ano,
         e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda,
         p.cod_sku, p.marca, p.tipo_produto, p.categoria_embalagem, p.linha_produto
""",

"gold_devolucao_dia": f"""
CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_gold_devolucao_dia AS
SELECT
  d.data,
  c.ano, c.mes, c.trimestre, c.semana_ano,
  e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional,
  e.porte_revenda,
  d.cod_sku,
  SUM(d.qtd_caixas) AS qtd_caixas,
  SUM(d.volume_hl) AS volume_hl,
  SUM(d.valor_total_reais) AS valor_total_reais
FROM {SCHEMA}.TBCERVA_fato_devolucao d
JOIN {SCHEMA}.TBCERVA_dim_calendario c ON d.data = c.data
JOIN {SCHEMA}.TBCERVA_dim_estrutura_comercial e ON d.cod_vendedor = e.cod_vendedor
GROUP BY d.data, c.ano, c.mes, c.trimestre, c.semana_ano,
         e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda,
         d.cod_sku
""",

"gold_visita_dia": f"""
CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_gold_visita_dia AS
WITH venda_dia_cliente AS (
  SELECT DISTINCT data, cod_vendedor, cod_cliente
  FROM {SCHEMA}.TBCERVA_fato_vendas
)
SELECT
  fv.data,
  c.ano, c.mes, c.trimestre, c.semana_ano,
  e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional,
  e.porte_revenda,
  COUNT(*) AS visitas_programadas,
  SUM(fv.visitou) AS visitas_realizadas,
  COUNT(vdc.cod_cliente) AS clientes_com_venda_no_dia
FROM {SCHEMA}.TBCERVA_fato_visita fv
JOIN {SCHEMA}.TBCERVA_dim_calendario c ON fv.data = c.data
JOIN {SCHEMA}.TBCERVA_dim_estrutura_comercial e ON fv.cod_vendedor = e.cod_vendedor
LEFT JOIN venda_dia_cliente vdc
  ON fv.data = vdc.data AND fv.cod_vendedor = vdc.cod_vendedor AND fv.cod_cliente = vdc.cod_cliente
  AND fv.visitou = 1
GROUP BY fv.data, c.ano, c.mes, c.trimestre, c.semana_ano,
         e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda
""",

"gold_cliente_mes": f"""
CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_gold_cliente_mes AS
WITH carteira_vendedor AS (
  SELECT ct.cod_cliente, ct.cod_vendedor
  FROM {SCHEMA}.TBCERVA_dim_carteira ct
  WHERE ct.ativo = true
),
clientes_hier AS (
  SELECT cv.cod_cliente, e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda
  FROM carteira_vendedor cv
  JOIN {SCHEMA}.TBCERVA_dim_estrutura_comercial e ON cv.cod_vendedor = e.cod_vendedor
),
meses AS (
  SELECT DISTINCT ano, mes, CONCAT(ano, '-', LPAD(mes, 2, '0')) AS ano_mes FROM {SCHEMA}.TBCERVA_dim_calendario
),
compras_mes AS (
  SELECT DISTINCT cod_cliente, CONCAT(YEAR(data), '-', LPAD(MONTH(data), 2, '0')) AS ano_mes
  FROM {SCHEMA}.TBCERVA_fato_vendas
)
SELECT
  m.ano_mes, m.ano, m.mes,
  ch.cod_vendedor, ch.cod_supervisor, ch.cod_gerente, ch.cod_revenda, ch.cod_regional, ch.porte_revenda,
  COUNT(DISTINCT ch.cod_cliente) AS clientes_carteira,
  COUNT(DISTINCT cm.cod_cliente) AS clientes_com_compra
FROM meses m
JOIN clientes_hier ch ON 1 = 1
LEFT JOIN compras_mes cm ON cm.cod_cliente = ch.cod_cliente AND cm.ano_mes = m.ano_mes
GROUP BY m.ano_mes, m.ano, m.mes,
         ch.cod_vendedor, ch.cod_supervisor, ch.cod_gerente, ch.cod_revenda, ch.cod_regional, ch.porte_revenda
""",

"gold_financeiro_mes": f"""
CREATE OR REPLACE TABLE {SCHEMA}.TBCERVA_gold_financeiro_mes AS
WITH carteira_vendedor AS (
  SELECT cod_cliente, cod_vendedor FROM {SCHEMA}.TBCERVA_dim_carteira WHERE ativo = true
)
SELECT
  CONCAT(YEAR(f.data_vencimento), '-', LPAD(MONTH(f.data_vencimento), 2, '0')) AS ano_mes,
  e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda,
  SUM(f.valor_boleto) AS valor_total_boleto,
  SUM(CASE WHEN f.status = 'Vencido' THEN f.valor_boleto ELSE 0 END) AS valor_vencido,
  SUM(CASE WHEN f.status = 'Pago' THEN f.valor_boleto ELSE 0 END) AS valor_pago,
  SUM(CASE WHEN f.status = 'Em Aberto' THEN f.valor_boleto ELSE 0 END) AS valor_aberto,
  COUNT(*) AS qtd_boletos,
  SUM(CASE WHEN f.status = 'Vencido' THEN 1 ELSE 0 END) AS qtd_boletos_vencidos
FROM {SCHEMA}.TBCERVA_fato_financeiro f
JOIN carteira_vendedor cv ON f.cod_cliente = cv.cod_cliente
JOIN {SCHEMA}.TBCERVA_dim_estrutura_comercial e ON cv.cod_vendedor = e.cod_vendedor
GROUP BY CONCAT(YEAR(f.data_vencimento), '-', LPAD(MONTH(f.data_vencimento), 2, '0')),
         e.cod_vendedor, e.cod_supervisor, e.cod_gerente, e.cod_revenda, e.cod_regional, e.porte_revenda
""",

}

for name, stmt in statements.items():
    t0 = time.time()
    sql(stmt)
    print(f"{name}: OK ({time.time()-t0:.1f}s)")

print("\n=== Contagens ===")
for t in ["TBCERVA_gold_vendas_dia", "TBCERVA_gold_devolucao_dia", "TBCERVA_gold_visita_dia",
          "TBCERVA_gold_cliente_mes", "TBCERVA_gold_financeiro_mes"]:
    r = sql(f"SELECT COUNT(*) FROM {SCHEMA}.{t}")
    print(t, r["result"]["data_array"][0][0])

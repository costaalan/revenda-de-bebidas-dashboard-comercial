import os, urllib.request, json, time

HOST = os.environ["DATABRICKS_HOST"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
SCHEMA = "workspace.bronze_bebidas"

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

def sql(stmt, max_wait=180):
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

print("--- Exemplos antes da correcao ---")
r = sql(f"""
    SELECT DISTINCT nome_vendedor FROM {SCHEMA}.TBCERVA_dim_estrutura_comercial
    WHERE nome_vendedor RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
    LIMIT 10
""")
for row in r["result"]["data_array"]:
    print(row)

print("--- Corrigindo (strip de titulos Dr./Sr./Sra./Dra. no inicio do nome) ---")
for col in ["nome_vendedor", "nome_supervisor", "nome_gerente"]:
    sql(f"""
        UPDATE {SCHEMA}.TBCERVA_dim_estrutura_comercial
        SET {col} = TRIM(REGEXP_REPLACE({col}, '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?)\\\\s+', ''))
        WHERE {col} RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
    """)

print("--- dim_cliente tambem (nome_cliente usa fake.company(), nao deveria ter titulo, mas confere) ---")
r = sql(f"""
    SELECT COUNT(*) FROM {SCHEMA}.TBCERVA_dim_cliente
    WHERE nome_cliente RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
""")
print("clientes com titulo no nome:", r["result"]["data_array"])

print("--- Conferindo depois ---")
r = sql(f"""
    SELECT COUNT(*) FROM {SCHEMA}.TBCERVA_dim_estrutura_comercial
    WHERE nome_vendedor RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
       OR nome_supervisor RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
       OR nome_gerente RLIKE '^(Dr\\\\.?|Sr\\\\.?|Sra\\\\.?|Dra\\\\.?) '
""")
print("linhas restantes com titulo:", r["result"]["data_array"])

r = sql(f"SELECT nome_vendedor, nome_supervisor, nome_gerente FROM {SCHEMA}.TBCERVA_dim_estrutura_comercial LIMIT 5")
for row in r["result"]["data_array"]:
    print(row)

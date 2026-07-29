# Databricks notebook source
# MAGIC %md
# MAGIC # TBCERVA — Gerador de dados mock (distribuidora de bebidas)
# MAGIC Gera 10 tabelas Delta (5 dimensoes + 5 fatos) cobrindo 01/01/2018 a 31/12/2020.
# MAGIC
# MAGIC **Suposicoes assumidas onde o prompt original era ambiguo ou tinha conflito interno** (documentadas tambem inline):
# MAGIC 1. Carteira de revenda M: 1500 clientes / 16 vendedores da ~94 clientes/vendedor, acima do range 20-40 pedido.
# MAGIC    Priorizei a regra mais forte ("cada cliente pertence a exatamente 1 vendedor") e escalei o tamanho da carteira
# MAGIC    proporcionalmente para revendas M. Range 20-40 aplicado literalmente as revendas G.
# MAGIC 2. Bairros: usei uma lista fixa de 20 bairros ficticios (10 "centrais/nobres" + 10 "perifericos"), reaplicada em
# MAGIC    todas as cidades — atende "lista fixa, nao aleatoria sem padrao" sem inventar 8x20 nomes distintos.
# MAGIC 3. PM do Energetico: a tabela de referencia menciona "pack4", mas a regra de embalagem manda caixa com 12 unidades.
# MAGIC    Tratei o valor da faixa de PM como preco de referencia por caixa (12un) diretamente, para nao introduzir uma
# MAGIC    conversao nao especificada.
# MAGIC 4. Agua: unifiquei "500ml/1,5L" (intro) e "350ml/510ml" (tabela de PM) usando 500ml cx12 + 1,5L pack6.
# MAGIC 5. Cidades vizinhas por revenda: so o exemplo de Curitiba (G1) veio pronto no prompt; completei as outras 7 revendas
# MAGIC    com municipios reais da mesma regiao metropolitana / mesmo estado, mantendo a logica pedida.
# MAGIC 6. Teto da faixa de faturamento M foi assumido em R$ 21.499.999 (um a menos que o piso de G), conforme a nota do
# MAGIC    proprio prompt.
# MAGIC
# MAGIC Catalogo/schema de destino sao parametrizados nos widgets da celula de configuracao.

# COMMAND ----------

# MAGIC %pip install -q faker

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "bronze_bebidas", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

import random
import datetime as dt
from datetime import date, timedelta

import pandas as pd
import numpy as np
from faker import Faker

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, DateType, TimestampType,
)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

DATA_INICIO = date(2018, 1, 1)
DATA_FIM = date(2020, 12, 31)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. TBCERVA_dim_calendario

# COMMAND ----------

def carnaval_terca(ano):
    # Datas reais de Carnaval (terca-feira) no periodo do prompt
    return {2018: date(2018, 2, 13), 2019: date(2019, 3, 5), 2020: date(2020, 2, 25)}[ano]

def sexta_santa(ano):
    return {2018: date(2018, 3, 30), 2019: date(2019, 4, 19), 2020: date(2020, 4, 10)}[ano]

def corpus_christi(ano):
    return {2018: date(2018, 5, 31), 2019: date(2019, 6, 20), 2020: date(2020, 6, 11)}[ano]

MESES_PT = ["", "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

def feriados_fixos(ano):
    fixos = [date(ano, 1, 1), date(ano, 4, 21), date(ano, 5, 1), date(ano, 9, 7),
             date(ano, 10, 12), date(ano, 11, 2), date(ano, 11, 15), date(ano, 12, 25)]
    carn = carnaval_terca(ano)
    moveis = [carn - timedelta(days=1), carn, sexta_santa(ano), corpus_christi(ano)]
    return set(fixos + moveis)

FERIADOS = set()
for ano in (2018, 2019, 2020):
    FERIADOS |= feriados_fixos(ano)

MES_CARNAVAL = {ano: carnaval_terca(ano).month for ano in (2018, 2019, 2020)}

cal_rows = []
d = DATA_INICIO
while d <= DATA_FIM:
    cal_rows.append({
        "data": d,
        "ano": d.year,
        "mes": d.month,
        "dia": d.day,
        "semana_ano": int(d.isocalendar()[1]),
        "dia_semana": d.isoweekday(),  # 1=Segunda ... 7=Domingo
        "nome_mes": MESES_PT[d.month],
        "trimestre": (d.month - 1) // 3 + 1,
        "is_feriado": d in FERIADOS,
        "dia_util": d.isoweekday() <= 5 and d not in FERIADOS,
    })
    d += timedelta(days=1)

df_calendario = spark.createDataFrame(pd.DataFrame(cal_rows))
df_calendario.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_calendario")
print(df_calendario.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. TBCERVA_dim_estrutura_comercial

# COMMAND ----------

CIDADE_COORDS = {
    "Curitiba": (-25.4284, -49.2733), "Colombo": (-25.2925, -49.2245),
    "Sao Jose dos Pinhais": (-25.5327, -49.2054), "Araucaria": (-25.5925, -49.4104),
    "Pinhais": (-25.4420, -49.1953), "Fazenda Rio Grande": (-25.6622, -49.3072),
    "Joinville": (-26.3044, -48.8456), "Jaragua do Sul": (-26.4869, -49.0669),
    "Sao Francisco do Sul": (-26.2434, -48.6386), "Araquari": (-26.3708, -48.7217),
    "Garuva": (-26.0286, -48.8536),
    "Londrina": (-23.3103, -51.1628), "Cambe": (-23.2758, -51.2794),
    "Iboporã".replace("ã", "a"): (-23.2661, -51.0483),
    "Rolandia": (-23.3086, -51.3703),
    "Ponta Grossa": (-25.0950, -50.1619), "Castro": (-24.7908, -50.0119),
    "Carambei": (-24.9169, -50.1000), "Palmeira": (-25.4292, -50.0067),
    "Sao Paulo": (-23.5505, -46.6333), "Guarulhos": (-23.4543, -46.5337),
    "Osasco": (-23.5325, -46.7917), "Santo Andre": (-23.6639, -46.5383),
    "Sao Bernardo do Campo": (-23.6939, -46.5650),
    "Rio de Janeiro": (-22.9068, -43.1729), "Duque de Caxias": (-22.7856, -43.3117),
    "Nova Iguacu": (-22.7556, -43.4603), "Belford Roxo": (-22.7642, -43.3994),
    "Nilopolis": (-22.8078, -43.4136),
    "Campinas": (-22.9099, -47.0626), "Valinhos": (-22.9708, -46.9958),
    "Vinhedo": (-23.0300, -46.9758), "Hortolandia": (-22.8586, -47.2200),
    "Sumare": (-22.8219, -47.2669),
    "Niteroi": (-22.8833, -43.1036), "Sao Goncalo": (-22.8268, -43.0539),
    "Marica": (-22.9194, -42.8186), "Itaborai": (-22.7444, -42.8594),
}

REGIONAIS = [
    {"cod_regional": "REG_SUL", "nome_regional": "Regional Sul", "estados": ["PR", "SC"]},
    {"cod_regional": "REG_SDE", "nome_regional": "Regional Sudeste", "estados": ["SP", "RJ"]},
]

REVENDAS = [
    {"cod_revenda": "REV_G1", "nome_revenda": "Revenda G1 - Curitiba", "porte": "G",
     "cod_regional": "REG_SUL", "uf": "PR", "cidade_sede": "Curitiba",
     "cidades": ["Curitiba", "Colombo", "Sao Jose dos Pinhais", "Araucaria", "Pinhais", "Fazenda Rio Grande"]},
    {"cod_revenda": "REV_G2", "nome_revenda": "Revenda G2 - Joinville", "porte": "G",
     "cod_regional": "REG_SUL", "uf": "SC", "cidade_sede": "Joinville",
     "cidades": ["Joinville", "Jaragua do Sul", "Sao Francisco do Sul", "Araquari", "Garuva"]},
    {"cod_revenda": "REV_M1", "nome_revenda": "Revenda M1 - Londrina", "porte": "M",
     "cod_regional": "REG_SUL", "uf": "PR", "cidade_sede": "Londrina",
     "cidades": ["Londrina", "Cambe", "Iboporã".replace("ã", "a"), "Rolandia"]},
    {"cod_revenda": "REV_M2", "nome_revenda": "Revenda M2 - Ponta Grossa", "porte": "M",
     "cod_regional": "REG_SUL", "uf": "PR", "cidade_sede": "Ponta Grossa",
     "cidades": ["Ponta Grossa", "Castro", "Carambei", "Palmeira"]},
    {"cod_revenda": "REV_G3", "nome_revenda": "Revenda G3 - Sao Paulo", "porte": "G",
     "cod_regional": "REG_SDE", "uf": "SP", "cidade_sede": "Sao Paulo",
     "cidades": ["Sao Paulo", "Guarulhos", "Osasco", "Santo Andre", "Sao Bernardo do Campo"]},
    {"cod_revenda": "REV_G4", "nome_revenda": "Revenda G4 - Rio de Janeiro", "porte": "G",
     "cod_regional": "REG_SDE", "uf": "RJ", "cidade_sede": "Rio de Janeiro",
     "cidades": ["Rio de Janeiro", "Duque de Caxias", "Nova Iguacu", "Belford Roxo", "Nilopolis"]},
    {"cod_revenda": "REV_M3", "nome_revenda": "Revenda M3 - Campinas", "porte": "M",
     "cod_regional": "REG_SDE", "uf": "SP", "cidade_sede": "Campinas",
     "cidades": ["Campinas", "Valinhos", "Vinhedo", "Hortolandia", "Sumare"]},
    {"cod_revenda": "REV_M4", "nome_revenda": "Revenda M4 - Niteroi", "porte": "M",
     "cod_regional": "REG_SDE", "uf": "RJ", "cidade_sede": "Niteroi",
     "cidades": ["Niteroi", "Sao Goncalo", "Marica", "Itaborai"]},
]

def gera_estrutura():
    rows = []
    for rev in REVENDAS:
        gerentes = []
        if rev["porte"] == "G":
            gerentes = [
                {"cod_gerente": f"{rev['cod_revenda']}_GER1", "nome_gerente": fake.name(), "tipo_gerente": "Geral"},
                {"cod_gerente": f"{rev['cod_revenda']}_GER2", "nome_gerente": fake.name(), "tipo_gerente": "Comercial"},
            ]
            n_sup, n_vend_por_sup = 8, 12
        else:
            gerentes = [
                {"cod_gerente": f"{rev['cod_revenda']}_GER1", "nome_gerente": fake.name(), "tipo_gerente": "Geral"},
            ]
            n_sup, n_vend_por_sup = 4, 4

        for s in range(n_sup):
            cod_supervisor = f"{rev['cod_revenda']}_SUP{s+1:02d}"
            nome_supervisor = fake.name()
            # Distribui supervisores igualmente entre os gerentes da revenda (assuncao — o prompt nao especifica)
            gerente = gerentes[s % len(gerentes)]
            for v in range(n_vend_por_sup):
                cod_vendedor = f"{cod_supervisor}_V{v+1:02d}"
                rows.append({
                    "cod_vendedor": cod_vendedor, "nome_vendedor": fake.name(),
                    "cod_supervisor": cod_supervisor, "nome_supervisor": nome_supervisor,
                    "cod_gerente": gerente["cod_gerente"], "nome_gerente": gerente["nome_gerente"],
                    "tipo_gerente": gerente["tipo_gerente"],
                    "cod_revenda": rev["cod_revenda"], "nome_revenda": rev["nome_revenda"],
                    "porte_revenda": rev["porte"],
                    "cod_regional": rev["cod_regional"],
                    "nome_regional": next(r["nome_regional"] for r in REGIONAIS if r["cod_regional"] == rev["cod_regional"]),
                })
    return pd.DataFrame(rows)

pdf_estrutura = gera_estrutura()
df_estrutura = spark.createDataFrame(pdf_estrutura)
df_estrutura.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_estrutura_comercial")
print(df_estrutura.count(), "vendedores")
display(df_estrutura.groupBy("cod_revenda", "porte_revenda").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. TBCERVA_dim_cliente

# COMMAND ----------

BAIRROS_CENTRAIS = ["Centro", "Jardim das Americas", "Bela Vista", "Alto da Gloria", "Jardim Europa",
                    "Vila Nova", "Cidade Jardim", "Batel", "Champagnat", "Agua Verde"]
BAIRROS_PERIFERICOS = ["Vila Esperanca", "Jardim Primavera", "Nova Uniao", "Parque das Flores",
                        "Vila Sao Jose", "Jardim Santa Rita", "Cidade Nova", "Vila Operaria",
                        "Jardim Progresso", "Parque Industrial"]

CANAIS = ["Bar", "Mercado/Conveniencia", "Restaurante", "Distribuidor", "Padaria"]
CANAL_PESOS = [0.30, 0.30, 0.20, 0.05, 0.15]

# Perfil de compra por canal (prob de cada perfil dado o canal) — assuncao de negocio
PERFIL_POR_CANAL = {
    "Distribuidor": {"COMPLETO": 0.70, "MISTO_LEVE": 0.15, "LOW_PRICE": 0.10, "SO_DESCARTAVEL": 0.05},
    "Restaurante": {"COMPLETO": 0.50, "MISTO_LEVE": 0.30, "LOW_PRICE": 0.10, "SO_DESCARTAVEL": 0.10},
    "Bar": {"COMPLETO": 0.35, "MISTO_LEVE": 0.30, "LOW_PRICE": 0.20, "SO_DESCARTAVEL": 0.15},
    "Mercado/Conveniencia": {"COMPLETO": 0.05, "MISTO_LEVE": 0.20, "LOW_PRICE": 0.30, "SO_DESCARTAVEL": 0.45},
    "Padaria": {"COMPLETO": 0.05, "MISTO_LEVE": 0.35, "LOW_PRICE": 0.25, "SO_DESCARTAVEL": 0.35},
}

CANAL_TIER_CENTRAL_PROB = {
    "Distribuidor": 0.80, "Restaurante": 0.80, "Bar": 0.35, "Mercado/Conveniencia": 0.25, "Padaria": 0.30,
}

N_CLIENTES_POR_PORTE = {"G": 3000, "M": 1500}

def gera_clientes():
    rows = []
    cod_seq = 1
    for rev in REVENDAS:
        n = N_CLIENTES_POR_PORTE[rev["porte"]]
        cidades = rev["cidades"]
        # cidade-sede recebe peso maior; restante dividido entre vizinhas
        pesos_cidade = [0.50] + [0.50 / (len(cidades) - 1)] * (len(cidades) - 1) if len(cidades) > 1 else [1.0]

        for _ in range(n):
            canal = np.random.choice(CANAIS, p=CANAL_PESOS)
            cidade = np.random.choice(cidades, p=pesos_cidade)
            lat_c, lon_c = CIDADE_COORDS[cidade]
            lat = lat_c + np.random.uniform(-0.03, 0.03)
            lon = lon_c + np.random.uniform(-0.03, 0.03)

            central = np.random.rand() < CANAL_TIER_CENTRAL_PROB[canal]
            bairro = random.choice(BAIRROS_CENTRAIS if central else BAIRROS_PERIFERICOS)

            perfil_probs = PERFIL_POR_CANAL[canal]
            perfil = np.random.choice(list(perfil_probs.keys()), p=list(perfil_probs.values()))

            flag_ipc = np.random.rand() < 0.17

            # data de cadastro: ~70% ja existiam antes do periodo, 30% entraram ao longo de 2018-2020 (aquisicao)
            if np.random.rand() < 0.70:
                data_cadastro = DATA_INICIO - timedelta(days=int(np.random.uniform(30, 1500)))
            else:
                dias_periodo = (DATA_FIM - DATA_INICIO).days
                data_cadastro = DATA_INICIO + timedelta(days=int(np.random.uniform(0, dias_periodo)))

            # churn: ~8% dos clientes saem da base em algum ponto depois do cadastro (e antes do fim do periodo)
            data_inativacao = None
            status = "Ativo"
            if np.random.rand() < 0.08:
                inicio_janela = max(data_cadastro, DATA_INICIO) + timedelta(days=60)
                if inicio_janela < DATA_FIM:
                    dias_janela = (DATA_FIM - inicio_janela).days
                    data_inativacao = inicio_janela + timedelta(days=int(np.random.uniform(0, dias_janela)))
                    status = "Inativo"

            rows.append({
                "cod_cliente": f"CLI{cod_seq:06d}",
                "nome_cliente": fake.company(),
                "canal": canal,
                "estado": rev["uf"],
                "cidade": cidade,
                "bairro": bairro,
                "latitude": float(lat),
                "longitude": float(lon),
                "cod_regional": rev["cod_regional"],
                "cod_revenda": rev["cod_revenda"],
                "flag_ipc": bool(flag_ipc),
                "perfil_compra": perfil,
                "data_cadastro": data_cadastro,
                "data_inativacao": data_inativacao,
                "status": status,
            })
            cod_seq += 1
    return pd.DataFrame(rows)

pdf_clientes = gera_clientes()
df_clientes = spark.createDataFrame(pdf_clientes)
df_clientes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_cliente")
print(df_clientes.count(), "clientes")
display(df_clientes.groupBy("cod_revenda").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. TBCERVA_dim_produto

# COMMAND ----------

def sku(cod, nome, tipo, marca, linha, embalagem_fisica, categoria_embalagem, capacidade_ml, qtd_por_caixa, pm_low, pm_high):
    pm = round(random.uniform(pm_low, pm_high), 2)
    return {
        "cod_sku": cod,
        "nome_completo": f"{cod} - {nome}",
        "categoria_embalagem": categoria_embalagem,
        "tipo_produto": tipo,
        "marca": marca,
        "linha_produto": linha,
        "capacidade_ml": capacidade_ml,
        "embalagem_fisica": embalagem_fisica,
        "qtd_por_caixa": qtd_por_caixa,
        "volume_hl_unidade": round(capacidade_ml / 100000, 5),
        "pm_referencia": pm,
        "preco_unitario_calculado": pm,
    }

produtos = []
c = 15900

# Pararanguari Puro Malte
produtos.append(sku(c, "Cerveja Pararanguari Puro Malte 350ml cx12", "Cerveja", "Pararanguari", "Puro Malte", "Lata", "Descartavel", 350, 12, 25, 31)); c += 1
produtos.append(sku(c, "Cerveja Pararanguari Puro Malte 473ml cx12", "Cerveja", "Pararanguari", "Puro Malte", "Lata", "Descartavel", 473, 12, 27, 34)); c += 1
produtos.append(sku(c, "Cerveja Pararanguari Puro Malte 600ml cx24 Retornavel", "Cerveja", "Pararanguari", "Puro Malte", "Garrafa Retornavel", "Retornavel", 600, 24, 95, 125)); c += 1
# Pararanguari Tradicional Pilsen
produtos.append(sku(c, "Cerveja Pararanguari Tradicional Pilsen 350ml cx12", "Cerveja", "Pararanguari", "Tradicional Pilsen", "Lata", "Descartavel", 350, 12, 32, 34)); c += 1
produtos.append(sku(c, "Cerveja Pararanguari Tradicional Pilsen 473ml cx12", "Cerveja", "Pararanguari", "Tradicional Pilsen", "Lata", "Descartavel", 473, 12, 35, 37)); c += 1
produtos.append(sku(c, "Cerveja Pararanguari Tradicional Pilsen 600ml cx24 Retornavel", "Cerveja", "Pararanguari", "Tradicional Pilsen", "Garrafa Retornavel", "Retornavel", 600, 24, 110, 115)); c += 1
# Pararanguari Chopp (pm por litro)
produtos.append(sku(c, "Chopp Pararanguari Barril 30L", "Chopp", "Pararanguari", "Unico", "Barril", "Barril", 30000, 1, 7.35, 9.12)); c += 1
produtos.append(sku(c, "Chopp Pararanguari Barril 50L", "Chopp", "Pararanguari", "Unico", "Barril", "Barril", 50000, 1, 7.35, 9.12)); c += 1
# Pararanguari Energy
produtos.append(sku(c, "Pararanguari Energy 269ml cx12", "Energetico", "Pararanguari", "Unico", "Lata", "Descartavel", 269, 12, 86, 87)); c += 1
produtos.append(sku(c, "Pararanguari Energy 473ml cx12", "Energetico", "Pararanguari", "Unico", "Lata", "Descartavel", 473, 12, 113, 117)); c += 1
# Pararanguari Agua
produtos.append(sku(c, "Fonte Pararanguari Agua 500ml cx12", "Agua", "Pararanguari", "Unico", "PET", "Descartavel", 500, 12, 12.70, 14.45)); c += 1
produtos.append(sku(c, "Fonte Pararanguari Agua 1,5L pack6", "Agua", "Pararanguari", "Unico", "PET", "Descartavel", 1500, 6, 14.00, 14.70)); c += 1
# Pararanguari Refrigerante (3 sabores x lata + pet2l)
for sabor in ["Cola", "Guarana", "Laranja"]:
    produtos.append(sku(c, f"Refri Pararanguari {sabor} 350ml cx12", "Refrigerante", "Pararanguari", "Unico", "Lata", "Descartavel", 350, 12, 20.00, 20.20)); c += 1
for sabor in ["Cola", "Guarana", "Laranja"]:
    produtos.append(sku(c, f"Refri Pararanguari {sabor} 2L pack6", "Refrigerante", "Pararanguari", "Unico", "PET", "Descartavel", 2000, 6, 16.70, 17.20)); c += 1

# Maltetop
produtos.append(sku(c, "Cerveja Maltetop Pilsen 350ml cx12", "Cerveja", "Maltetop", "Pilsen", "Lata", "Descartavel", 350, 12, 22, 29)); c += 1
produtos.append(sku(c, "Cerveja Maltetop Pilsen 473ml cx12", "Cerveja", "Maltetop", "Pilsen", "Lata", "Descartavel", 473, 12, 24, 31)); c += 1
produtos.append(sku(c, "Cerveja Maltetop Pilsen 600ml cx24 Retornavel", "Cerveja", "Maltetop", "Pilsen", "Garrafa Retornavel", "Retornavel", 600, 24, 85, 95)); c += 1
produtos.append(sku(c, "Cerveja Maltetop Puro Malte 350ml cx12", "Cerveja", "Maltetop", "Puro Malte", "Lata", "Descartavel", 350, 12, 22, 29)); c += 1
produtos.append(sku(c, "Cerveja Maltetop Puro Malte 473ml cx12", "Cerveja", "Maltetop", "Puro Malte", "Lata", "Descartavel", 473, 12, 24, 31)); c += 1
produtos.append(sku(c, "Cerveja Maltetop Puro Malte 600ml cx24 Retornavel", "Cerveja", "Maltetop", "Puro Malte", "Garrafa Retornavel", "Retornavel", 600, 24, 85, 95)); c += 1

# Soceva
produtos.append(sku(c, "Cerveja Soceva 350ml cx12", "Cerveja", "Soceva", "Unico", "Lata", "Descartavel", 350, 12, 19, 20)); c += 1
produtos.append(sku(c, "Cerveja Soceva 473ml cx12", "Cerveja", "Soceva", "Unico", "Lata", "Descartavel", 473, 12, 19, 20)); c += 1
produtos.append(sku(c, "Cerveja Soceva 600ml cx24 Retornavel", "Cerveja", "Soceva", "Unico", "Garrafa Retornavel", "Retornavel", 600, 24, 56, 70)); c += 1

pdf_produtos = pd.DataFrame(produtos)
df_produtos = spark.createDataFrame(pdf_produtos)
df_produtos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto")
print(df_produtos.count(), "SKUs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. TBCERVA_dim_carteira

# COMMAND ----------

def gera_carteira(pdf_clientes, pdf_estrutura):
    rows = []
    dias_semana = [1, 2, 3, 4, 5, 6]  # Segunda a Sabado
    for rev_cod in pdf_estrutura["cod_revenda"].unique():
        vendedores = pdf_estrutura.loc[pdf_estrutura["cod_revenda"] == rev_cod, "cod_vendedor"].tolist()
        clientes = pdf_clientes.loc[pdf_clientes["cod_revenda"] == rev_cod, "cod_cliente"].tolist()
        random.shuffle(clientes)

        n_vend = len(vendedores)
        base = len(clientes) // n_vend
        resto = len(clientes) % n_vend

        idx = 0
        for i, cod_vendedor in enumerate(vendedores):
            tamanho = base + (1 if i < resto else 0)
            carteira_cliente = clientes[idx: idx + tamanho]
            idx += tamanho
            for j, cod_cliente in enumerate(carteira_cliente):
                dia_visita = dias_semana[j % len(dias_semana)]
                rows.append({
                    "cod_vendedor": cod_vendedor,
                    "cod_cliente": cod_cliente,
                    "dia_semana_visita": dia_visita,
                    "ativo": bool(np.random.rand() < 0.95),
                })
    return pd.DataFrame(rows)

pdf_carteira = gera_carteira(pdf_clientes, pdf_estrutura)
df_carteira = spark.createDataFrame(pdf_carteira)
df_carteira.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_carteira")
print(df_carteira.count(), "vinculos vendedor-cliente")
display(
    df_carteira.join(df_estrutura.select("cod_vendedor", "cod_revenda"), "cod_vendedor")
    .groupBy("cod_revenda").agg(F.countDistinct("cod_vendedor").alias("vendedores"),
                                 F.count("*").alias("linhas_carteira"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. TBCERVA_fato_visita

# COMMAND ----------

df_cal = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_calendario")
df_cart = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_carteira").filter("ativo = true")
df_cli = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_cliente")

visitas_base = (
    df_cart.join(df_cli.select("cod_cliente", "data_cadastro", "data_inativacao", "status"), "cod_cliente")
    .join(df_cal.select("data", "dia_semana", "ano", "mes"),
          on=[df_cart.dia_semana_visita == F.col("dia_semana")], how="inner")
    .filter(F.col("data") >= F.col("data_cadastro"))
    .filter(F.col("data_inativacao").isNull() | (F.col("data") <= F.col("data_inativacao")))
)

visitas_base = visitas_base.withColumn(
    "visitou", (F.rand(SEED) < F.lit(0.90)).cast("int")
)

df_visita = visitas_base.select(
    "data", "cod_vendedor", "cod_cliente", "visitou"
)

df_visita.write.mode("overwrite").partitionBy("data").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_visita")
print(df_visita.count(), "linhas de visita programada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. TBCERVA_fato_vendas

# COMMAND ----------

df_prod = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto")
df_visita_ok = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_visita").filter("visitou = 1")

df_visita_ctx = (
    df_visita_ok
    .join(df_cli.select("cod_cliente", "perfil_compra", "cod_revenda", "cod_regional"), "cod_cliente")
    .join(df_cal.select("data", "ano", "mes"), "data")
)

# Peso sazonal mensal (normalizado, soma 12) — usado tambem para propensao diaria de compra
PESO_SAZONAL_BASE = {12: 1.40, 2: 1.25, 3: 1.25, 1: 1.10, 10: 1.10, 11: 1.10,
                      4: 0.95, 5: 0.95, 9: 0.95, 6: 0.70, 7: 0.70, 8: 0.70}
# mes de carnaval do ano correto recebe o peso 1.25; o outro (fev OU mar, o que nao for carnaval naquele ano) cai para 0.95
peso_rows = []
for ano in (2018, 2019, 2020):
    mes_carn = MES_CARNAVAL[ano]
    for mes in range(1, 13):
        peso = PESO_SAZONAL_BASE[mes]
        if mes in (2, 3):
            peso = 1.25 if mes == mes_carn else 0.95
        variacao = 1 + np.random.uniform(-0.05, 0.05)
        peso_rows.append({"ano": ano, "mes": mes, "peso_sazonal": round(peso * variacao, 4)})
pdf_peso = pd.DataFrame(peso_rows)
df_peso = spark.createDataFrame(pdf_peso)

# efeito covid: bares/restaurantes caem forte mar-ago/2020, conveniencia/mercado sobe
df_visita_ctx = df_visita_ctx.join(df_peso, ["ano", "mes"])

df_visita_ctx = df_visita_ctx.withColumn(
    "covid_periodo",
    (F.col("ano") == 2020) & (F.col("mes").between(3, 8))
)

# propensao de compra no dia da visita: base * peso sazonal (normalizado por 1.0 medio) * ajuste covid por canal
df_visita_ctx = df_visita_ctx.join(df_cli.select("cod_cliente", "canal"), "cod_cliente")

df_visita_ctx = df_visita_ctx.withColumn(
    "ajuste_covid",
    F.when(~F.col("covid_periodo"), F.lit(1.0))
     .when(F.col("canal").isin("Bar", "Restaurante"), F.lit(0.45))
     .when(F.col("canal") == "Mercado/Conveniencia", F.lit(1.25))
     .otherwise(F.lit(0.90))
)

df_visita_ctx = df_visita_ctx.withColumn(
    "prob_venda",
    F.least(F.lit(0.97), F.lit(0.55) * F.col("peso_sazonal") * F.col("ajuste_covid"))
)

df_venda_evento = df_visita_ctx.withColumn(
    "houve_venda", (F.rand(SEED + 1) < F.col("prob_venda")).cast("int")
).filter("houve_venda = 1")

# candidatos de SKU por perfil de compra (regra de negocio central da Cobertura por perfil)
df_cand = df_venda_evento.crossJoin(df_prod.select(
    "cod_sku", "tipo_produto", "categoria_embalagem", "marca", "qtd_por_caixa", "capacidade_ml", "pm_referencia"
))

elegivel = (
    ((F.col("perfil_compra") == "SO_DESCARTAVEL") & (F.col("categoria_embalagem") == "Descartavel"))
    | ((F.col("perfil_compra") == "MISTO_LEVE") &
       ((F.col("categoria_embalagem") == "Descartavel") |
        ((F.col("categoria_embalagem") == "Retornavel") & (F.rand(SEED + 2) < 0.10))))
    | (F.col("perfil_compra") == "COMPLETO")
    | ((F.col("perfil_compra") == "LOW_PRICE") &
       (((F.col("marca") == "Soceva") & (F.rand(SEED + 3) < 0.80)) |
        ((F.col("marca") != "Soceva") & (F.rand(SEED + 4) < 0.12))))
)

df_cand = df_cand.filter(elegivel)

w = Window.partitionBy("data", "cod_cliente").orderBy(F.rand(SEED + 5))
df_cand = df_cand.withColumn("rn", F.row_number().over(w))
df_cand = df_cand.withColumn("k_itens", (F.rand(SEED + 6) * 3 + 1).cast("int"))  # 1 a 4 SKUs por venda
df_cand = df_cand.filter(F.col("rn") <= F.col("k_itens"))

df_cand = df_cand.withColumn(
    "reajuste_mes", F.lit(1.0) + (F.rand(SEED + 7) * 0.10 - 0.05)  # +/-5% ao mes
)

df_cand = df_cand.withColumn(
    "qtd_caixas",
    F.when(F.col("tipo_produto") == "Chopp", (F.rand(SEED + 8) * 2 + 1).cast("int"))  # 1 a 3 barris
     .otherwise((F.rand(SEED + 8) * 7 + 1).cast("int"))  # 1 a 8 caixas
)

df_vendas = df_cand.withColumn(
    "qtd_unidades",
    F.when(F.col("tipo_produto") == "Chopp", F.col("qtd_caixas"))
     .otherwise(F.col("qtd_caixas") * F.col("qtd_por_caixa"))
).withColumn(
    "volume_litros",
    F.when(F.col("tipo_produto") == "Chopp", F.col("qtd_caixas") * F.col("capacidade_ml") / 1000.0)
     .otherwise(F.col("qtd_unidades") * F.col("capacidade_ml") / 1000.0)
).withColumn(
    "volume_hl", F.col("volume_litros") / 100.0
).withColumn(
    "valor_total_reais",
    F.when(F.col("tipo_produto") == "Chopp", F.col("volume_litros") * F.col("pm_referencia") * F.col("reajuste_mes"))
     .otherwise(F.col("qtd_caixas") * F.col("pm_referencia") * F.col("reajuste_mes"))
).select(
    "data", "cod_cliente", "cod_sku", "cod_vendedor", "qtd_caixas", "qtd_unidades",
    "volume_litros", "volume_hl", "valor_total_reais"
)

df_vendas.write.mode("overwrite").partitionBy("data").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_vendas")
print(df_vendas.count(), "linhas de venda")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. TBCERVA_fato_devolucao

# COMMAND ----------

df_vendas_full = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_vendas")

MOTIVOS = ["Avaria", "Vencimento", "Troca de Produto", "Erro de Pedido"]
motivo_expr = F.element_at(
    F.array(*[F.lit(m) for m in MOTIVOS]),
    (F.rand(SEED + 9) * len(MOTIVOS)).cast("int") + 1
)

df_devolucao = (
    df_vendas_full
    .withColumn("_r", F.rand(SEED + 10))
    .filter(F.col("_r") < 0.035)  # ~3.5% do volume vendido devolvido
    .withColumn("pct_dev", F.rand(SEED + 11) * 0.03 + 0.02)  # devolve 2%-5% da qtd vendida naquela linha
    .withColumn("qtd_caixas", F.greatest(F.lit(1), (F.col("qtd_caixas") * F.col("pct_dev")).cast("int")))
    .withColumn("qtd_unidades", (F.col("qtd_unidades") * F.col("pct_dev")).cast("int"))
    .withColumn("volume_litros", F.col("volume_litros") * F.col("pct_dev"))
    .withColumn("volume_hl", F.col("volume_hl") * F.col("pct_dev"))
    .withColumn("valor_total_reais", F.col("valor_total_reais") * F.col("pct_dev"))
    .withColumn("motivo_devolucao", motivo_expr)
    .select("data", "cod_cliente", "cod_sku", "cod_vendedor", "qtd_caixas", "qtd_unidades",
            "volume_litros", "volume_hl", "valor_total_reais", "motivo_devolucao")
)

df_devolucao.write.mode("overwrite").partitionBy("data").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_devolucao")
print(df_devolucao.count(), "linhas de devolucao")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. TBCERVA_fato_financeiro

# COMMAND ----------

df_vendas_mes = (
    df_vendas_full
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .groupBy("cod_cliente", "ano_mes")
    .agg(F.sum("valor_total_reais").alias("valor_compras_mes"))
    .filter("valor_compras_mes > 0")
)

df_cli_perfil = df_cli.select("cod_cliente", "perfil_compra")

df_boletos = df_vendas_mes.join(df_cli_perfil, "cod_cliente")

df_boletos = df_boletos.withColumn(
    "data_emissao", F.last_day(F.to_date(F.concat(F.col("ano_mes"), F.lit("-01"))))
).withColumn(
    "data_vencimento", F.date_add(F.col("data_emissao"), 15)
).withColumn(
    "valor_boleto", F.round(F.col("valor_compras_mes") * (F.lit(1.0) + (F.rand(SEED + 12) * 0.06 - 0.03)), 2)
).withColumn(
    "prob_inadimplencia",
    F.when(F.col("perfil_compra") == "LOW_PRICE", F.lit(0.15)).otherwise(F.lit(0.08))
).withColumn(
    "_r", F.rand(SEED + 13)
).withColumn(
    "status",
    F.when(F.col("_r") < F.col("prob_inadimplencia"), F.lit("Vencido"))
     .when(F.col("_r") < F.col("prob_inadimplencia") + 0.05, F.lit("Em Aberto"))
     .otherwise(F.lit("Pago"))
).withColumn(
    "data_pagamento",
    F.when(F.col("status") == "Pago",
           F.date_add(F.col("data_vencimento"), (F.rand(SEED + 14) * 10 - 5).cast("int")))
     .otherwise(F.lit(None).cast("date"))
).withColumn(
    "numero_boleto",
    F.concat(F.col("cod_cliente"), F.lit("-"), F.regexp_replace(F.col("ano_mes"), "-", ""))
).select(
    "cod_cliente", "numero_boleto", "data_emissao", "data_vencimento", "data_pagamento",
    "valor_boleto", "status"
)

df_boletos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_financeiro")
print(df_boletos.count(), "boletos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. TBCERVA_fato_metas

# COMMAND ----------

FAIXAS_META = {
    "G": {"hectolitro": (35000, 50000), "volume": (700000, 1000000), "faturamento": (21500000, 41000000)},
    "M": {"hectolitro": (9000, 34999), "volume": (150000, 699999), "faturamento": (4800000, 21499999)},
}

def sorteia_metas_revenda(porte):
    faixa = FAIXAS_META[porte]
    hl = random.uniform(*faixa["hectolitro"])
    vol = random.uniform(*faixa["volume"])
    faixa_rz = (400, 900)  # R$/HL implicito plausivel
    fat = random.uniform(*faixa["faturamento"])
    tentativas = 0
    while not (faixa_rz[0] <= fat / hl <= faixa_rz[1]) and tentativas < 200:
        fat = random.uniform(*faixa["faturamento"])
        tentativas += 1
    return hl, vol, fat

metas_base = {}
for rev in REVENDAS:
    metas_base[rev["cod_revenda"]] = sorteia_metas_revenda(rev["porte"])

dias_uteis_mes = (
    df_cal.groupBy("ano", "mes").agg(F.sum(F.col("dia_util").cast("int")).alias("qtd_dias_uteis_mes"))
).toPandas()
dias_uteis_mes["ano_mes"] = dias_uteis_mes["ano"].astype(str) + "-" + dias_uteis_mes["mes"].astype(str).str.zfill(2)

KPIS_PERCENTUAIS = {
    "Eficiencia de Visita": (0.97, "Maior e melhor"),
    "IPC": (3.2, "Maior e melhor"),
    "Inadimplencia": (0.01, "Menor e melhor"),
    "Clientes sem Compra": (0.02, "Menor e melhor"),
    "Cobertura": (0.98, "Maior e melhor"),
    "Positivacao": (0.98, "Maior e melhor"),
}

meta_rows = []
for rev in REVENDAS:
    hl_base, vol_base, fat_base = metas_base[rev["cod_revenda"]]
    for _, r in pdf_peso[pdf_peso["ano"].isin([2018, 2019, 2020])].iterrows():
        pass
    for ano in (2018, 2019, 2020):
        for mes in range(1, 13):
            peso = pdf_peso.loc[(pdf_peso.ano == ano) & (pdf_peso.mes == mes), "peso_sazonal"].iloc[0]
            ano_mes = f"{ano}-{mes:02d}"
            dias_uteis = int(dias_uteis_mes.loc[dias_uteis_mes.ano_mes == ano_mes, "qtd_dias_uteis_mes"].iloc[0])

            for kpi, valor_mensal, direcao in [
                ("Hectolitro", hl_base / 12 * peso, "Maior e melhor"),
                ("Volume", vol_base / 12 * peso, "Maior e melhor"),
                ("Faturamento", fat_base / 12 * peso, "Maior e melhor"),
                ("Devolucoes", (vol_base / 12 * peso) * 0.03, "Menor e melhor"),
            ]:
                meta_rows.append({
                    "ano_mes": ano_mes, "cod_revenda": rev["cod_revenda"], "kpi": kpi,
                    "tipo_meta": "Fluxo", "meta_mensal": round(float(valor_mensal), 2),
                    "meta_dia_util": round(float(valor_mensal) / dias_uteis, 2) if dias_uteis else None,
                    "direcao_meta": direcao,
                })

            for kpi, (valor, direcao) in KPIS_PERCENTUAIS.items():
                meta_rows.append({
                    "ano_mes": ano_mes, "cod_revenda": rev["cod_revenda"], "kpi": kpi,
                    "tipo_meta": "Percentual", "meta_mensal": valor, "meta_dia_util": None,
                    "direcao_meta": direcao,
                })

            # PM (meta como referencia de precificacao media, tratado como percentual/fixo — nao soma no mes)
            meta_rows.append({
                "ano_mes": ano_mes, "cod_revenda": rev["cod_revenda"], "kpi": "PM",
                "tipo_meta": "Percentual", "meta_mensal": None, "meta_dia_util": None,
                "direcao_meta": "Maior e melhor",
            })

df_metas = spark.createDataFrame(pd.DataFrame(meta_rows))
df_metas.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_metas")
print(df_metas.count(), "linhas de meta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

tabelas = [
    "TBCERVA_dim_calendario", "TBCERVA_dim_estrutura_comercial", "TBCERVA_dim_cliente",
    "TBCERVA_dim_produto", "TBCERVA_dim_carteira", "TBCERVA_fato_vendas",
    "TBCERVA_fato_devolucao", "TBCERVA_fato_visita", "TBCERVA_fato_financeiro", "TBCERVA_fato_metas",
]
for t in tabelas:
    n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"{t}: {n} registros")

# COMMAND ----------

print("--- Clientes por porte de revenda ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_cliente")
    .join(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_estrutura_comercial").select("cod_revenda", "porte_revenda").distinct(), "cod_revenda")
    .groupBy("cod_revenda", "porte_revenda").count()
)

print("--- Vendedores por revenda (esperado: G=96, M=16) ---")
display(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_estrutura_comercial").groupBy("cod_revenda").count())

print("--- Sazonalidade: soma de meta Hectolitro Dezembro vs Julho (esperado Dez > Jul) ---")
df_m = spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_metas").filter("kpi = 'Hectolitro'")
display(
    df_m.withColumn("mes", F.split("ano_mes", "-")[1])
    .filter(F.col("mes").isin("12", "07"))
    .groupBy("mes").agg(F.sum("meta_mensal").alias("soma_meta_hl"))
)

print("--- Energetico so em Pararanguari? ---")
display(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto").filter("tipo_produto = 'Energetico'").select("marca").distinct())

print("--- Soceva so tem Cerveja? ---")
display(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto").filter("marca = 'Soceva'").select("tipo_produto").distinct())

print("--- Chopp so em Pararanguari? ---")
display(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto").filter("tipo_produto = 'Chopp'").select("marca").distinct())

print("--- Linhas de cerveja por marca ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto")
    .filter("tipo_produto = 'Cerveja'")
    .select("marca", "linha_produto").distinct()
    .orderBy("marca", "linha_produto")
)

print("--- qtd_por_caixa de todo retornavel 600ml (esperado: sempre 24) ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto")
    .filter("categoria_embalagem = 'Retornavel'")
    .select("qtd_por_caixa").distinct()
)

print("--- Estado do cliente compativel com a regional? ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_cliente")
    .groupBy("cod_regional", "estado").count()
)

print("--- Faturamento total e PM medio por SKU ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_vendas")
    .groupBy("cod_sku")
    .agg(F.sum("valor_total_reais").alias("faturamento_total"),
         (F.sum("valor_total_reais") / F.sum("qtd_caixas")).alias("pm_medio_calculado"))
    .join(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto").select("cod_sku", "nome_completo", "pm_referencia"), "cod_sku")
    .orderBy("cod_sku")
)

print("--- Cobertura por categoria de embalagem (retornavel vs descartavel) — deve ser desigual entre perfis ---")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_fato_vendas")
    .join(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_produto").select("cod_sku", "categoria_embalagem"), "cod_sku")
    .join(spark.table(f"{CATALOG}.{SCHEMA}.TBCERVA_dim_cliente").select("cod_cliente", "perfil_compra"), "cod_cliente")
    .groupBy("perfil_compra", "categoria_embalagem")
    .agg(F.countDistinct("cod_cliente").alias("clientes_distintos"))
    .orderBy("perfil_compra", "categoria_embalagem")
)

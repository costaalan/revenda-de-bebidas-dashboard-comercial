# Revenda de Bebidas — Dashboard Comercial

Base de dados fictícia (porém estatisticamente coerente) de uma distribuidora de
bebidas, gerada no Databricks, com um dashboard comercial de KPIs construído em
Google Apps Script (Web App) consultando o Databricks via REST.

> **Dados sintéticos.** Nomes de clientes, vendedores e volumes de venda são
> gerados artificialmente (Faker + regras de negócio + seeds fixas). Não há
> nenhum dado real de nenhuma empresa aqui.

## Arquitetura

```
Databricks (Unity Catalog)                     Google Apps Script (Web App)
┌─────────────────────────────┐   SQL Statement  ┌───────────────────────────┐
│ workspace.bronze_bebidas     │   Execution API  │ Code.gs  (server)         │
│  ├─ TBCERVA_dim_*  (5)       │ <───────────────> │  - dbQuery_/dbQueryBatch_ │
│  ├─ TBCERVA_fato_*  (5)      │   (UrlFetchApp)   │  - calculo de KPIs/metas  │
│  └─ TBCERVA_fato_metas       │                   │ Index.html (client)       │
│                               │                   │  - filtros, cards, charts │
│  gold layer (agregados):     │                   │  - Google Charts + SVG    │
│  ├─ TBCERVA_gold_vendas_dia  │                   └───────────────────────────┘
│  ├─ TBCERVA_gold_devolucao_dia│
│  ├─ TBCERVA_gold_visita_dia  │
│  ├─ TBCERVA_gold_cliente_mes │
│  └─ TBCERVA_gold_financeiro_mes│
└─────────────────────────────┘
```

Não existe conector JDBC nativo pra Databricks no Apps Script — a integração é
via `UrlFetchApp` chamando a **SQL Statement Execution API** do Databricks
(`/api/2.0/sql/statements`), autenticado com um Personal Access Token guardado
em **Script Properties** (nunca no código).

O Apps Script nunca lê as tabelas fato brutas diretamente (`fato_vendas` tem
~2M linhas) — só consulta a **gold layer**, agregada por dia/vendedor/produto,
pra manter as respostas rápidas.

## Estrutura do repositório

```
databricks/
  01_gerador_dados.py   → Notebook PySpark (rodar DENTRO do Databricks).
                           Cria as 10 tabelas base (5 dimensões + 5 fatos +
                           metas) cobrindo 01/01/2018 a 31/12/2020.
  02_gold_layer.py       → Script local. Cria as tabelas agregadas (gold)
                           que o dashboard consulta.
  03_recalibrar_metas.py → Script local. Recalibra as metas de
                           Volume/Hectolitro/Faturamento/Devoluções pra ficarem
                           coerentes com o volume realmente gerado (ver
                           "Decisões e ajustes" abaixo).
apps-script/
  Code.gs           → Backend do Web App (queries, calculo de KPI/meta/desvio).
  Index.html        → Frontend (filtros, cards, gráficos, Matriz BCG).
  appsscript.json    → Manifesto do projeto (executeAs, access).
```

## Modelo de dados (Databricks)

**Dimensões:** `dim_calendario`, `dim_estrutura_comercial` (hierarquia Regional
→ Revenda → Gerente → Supervisor → Vendedor), `dim_cliente` (18.000 clientes,
geolocalizados), `dim_produto` (27 SKUs — Pararanguari/Maltetop/Soceva),
`dim_carteira` (roteiro vendedor↔cliente).

**Fatos:** `fato_vendas`, `fato_devolucao`, `fato_visita`, `fato_financeiro`
(boletos/inadimplência), `fato_metas` (metas mensais por revenda/KPI, com
sazonalidade real de bebidas — pico dez/jan, queda mai-jul).

## Dashboard (Overview + Análise)

- **Filtros**: granularidade (Dia/Semana/Mês/Trimestre) + Ano, hierarquia
  comercial em cascata (Regional → Revenda → Gerente → Supervisor → Vendedor),
  produto (na página Análise).
- **KPIs (Overview)**: Volume (caixas), Hectolitro (com gauge de garrafa
  enchendo até a meta), Faturamento, Inadimplência, Clientes sem Compra,
  Eficiência de Visita — cada um com desvio vs meta e vs período anterior
  (seta + cor considerando se o KPI é "maior é melhor" ou "menor é melhor").
- **Gráficos**: evolutivo (Faturamento/HL/PM), top 10 produtos, ranking de
  revendas, participação por regional (rosca), e uma **Matriz BCG** de
  produtos (rentabilidade × crescimento, com ícone de lata/PET/garrafa/barril
  no lugar de bolinha genérica).
- **Análise**: tabela + quadrante Volume × PM por produto.

## Como reproduzir

### 1. Databricks

1. Importe `databricks/01_gerador_dados.py` como notebook no seu workspace.
2. Rode do início ao fim (compute Spark — cluster ou serverless notebook).
   Cria `workspace.bronze_bebidas.TBCERVA_*` (10 tabelas).
3. Localmente, com as variáveis de ambiente configuradas:
   ```bash
   export DATABRICKS_HOST="seu-host.cloud.databricks.com"
   export DATABRICKS_WAREHOUSE_ID="id-do-seu-sql-warehouse"
   export DATABRICKS_TOKEN="dapi..."
   python databricks/02_gold_layer.py
   python databricks/03_recalibrar_metas.py   # opcional, ver secao abaixo
   ```

### 2. Apps Script

1. `npm install -g @google/clasp && clasp login`
2. Crie o projeto: `clasp create --type standalone --title "Dashboard Comercial" --rootDir apps-script`
3. `clasp push`
4. No editor (script.google.com) → **Project Settings → Script Properties**,
   adicione: `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_TOKEN`.
5. **Deploy → New deployment → Web app**. Recomendado: `Execute as: User
   accessing the web app` + `Who has access: Anyone` (evita o bug conhecido do
   Google de apps "Execute as: Me" travarem quando o navegador tem outra conta
   logada — em troca, cada visitante vê uma tela "app não verificado" na
   primeira visita, normal pra apps pessoais).

## Decisões e ajustes feitos (documentados pra não virarem surpresa)

- **Volume = caixas vendidas**, não unidades individuais (uma lata avulsa
  custa R$2-3; o PM implícito da meta original só bate em nível de caixa).
- **Metas recalibradas** (`03_recalibrar_metas.py`): as faixas de meta do
  briefing original (ex: G = 700k-1M "Volume"/mês) pressupunham uma operação
  maior do que a base de 18.000 clientes gera. As metas de
  Hectolitro/Faturamento agora são **derivadas** da meta de Volume via razão
  real HL/caixa e R$/caixa de cada revenda (coerentes entre si por
  construção), com um fator ±15% por revenda pra dar uma mistura crível de
  quem bate e quem não bate a meta.
- **Margem (Matriz BCG)**: não há custo real na base. Margem estimada por
  categoria de embalagem — Retornável (~60%) e Barril (~63%) rendem mais que
  Descartável (~31%) por reuso da embalagem, com ajuste por marca.
- **Carteira de revenda M**: 1.500 clientes / 16 vendedores dá ~94
  clientes/vendedor, acima do range 20-40 pedido originalmente — priorizei a
  regra "todo cliente pertence a exatamente 1 vendedor".

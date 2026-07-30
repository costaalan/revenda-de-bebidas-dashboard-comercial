// ============================================================================
// TBCERVA Dashboard — Apps Script backend
// Le as gold tables criadas em workspace.bronze_bebidas via SQL Statement
// Execution API do Databricks (nao existe conector JDBC nativo pra Databricks
// no Apps Script). Token/host ficam em Script Properties, nunca no codigo.
// ============================================================================

const SCHEMA = 'workspace.bronze_bebidas';

// KPIs Fluxo somam a meta entre revendas/vendedores do escopo.
// KPIs Percentual usam a mesma meta fixa (nao soma, nao faz media de media).
const KPI_TIPO = {
  'Volume': 'Fluxo',
  'Hectolitro': 'Fluxo',
  'Faturamento': 'Fluxo',
  'Devolucoes': 'Fluxo',
  'Inadimplencia': 'Percentual',
  'Clientes sem Compra': 'Percentual',
  'Eficiencia de Visita': 'Percentual',
};
// KPIs cujo desvio deve ser exibido em pontos percentuais (pp) — sao os que ja
// sao expressos em % de verdade. PM e IPC sao "Percentual" no fato_metas (nao
// somam), mas nao sao %, entao o desvio deles e exibido em % relativo.
const KPI_EH_PERCENTUAL_DE_VERDADE = {
  'Inadimplencia': true,
  'Clientes sem Compra': true,
  'Eficiencia de Visita': true,
  'Cobertura': true,
  'Positivacao': true,
};

function getConfig_() {
  const p = PropertiesService.getScriptProperties();
  const host = p.getProperty('DATABRICKS_HOST');
  const warehouseId = p.getProperty('DATABRICKS_WAREHOUSE_ID');
  const token = p.getProperty('DATABRICKS_TOKEN');
  if (!host || !warehouseId || !token) {
    throw new Error('Configure DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID e DATABRICKS_TOKEN em Project Settings > Script Properties.');
  }
  return { host, warehouseId, token };
}

function doGet() {
  return HtmlService.createTemplateFromFile('Index')
    .evaluate()
    .setTitle('Dashboard Comercial')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

// ---------------------------------------------------------------------------
// Cliente HTTP para a SQL Statement Execution API do Databricks
// ---------------------------------------------------------------------------
function dbQuery_(statement) {
  const cfg = getConfig_();
  const base = `https://${cfg.host}/api/2.0/sql/statements`;
  const headers = { Authorization: 'Bearer ' + cfg.token };

  let resp = UrlFetchApp.fetch(base, {
    method: 'post',
    contentType: 'application/json',
    headers,
    payload: JSON.stringify({ warehouse_id: cfg.warehouseId, statement, wait_timeout: '50s' }),
    muteHttpExceptions: true,
  });

  if (resp.getResponseCode() >= 400) {
    throw new Error('Databricks HTTP ' + resp.getResponseCode() + ': ' + resp.getContentText());
  }

  let json = JSON.parse(resp.getContentText());
  let tries = 0;
  while (json.status && (json.status.state === 'PENDING' || json.status.state === 'RUNNING') && tries < 30) {
    Utilities.sleep(1500);
    resp = UrlFetchApp.fetch(`${base}/${json.statement_id}`, { method: 'get', headers, muteHttpExceptions: true });
    json = JSON.parse(resp.getContentText());
    tries++;
  }

  if (!json.status || json.status.state !== 'SUCCEEDED') {
    throw new Error('Databricks query falhou: ' + JSON.stringify(json.status));
  }

  const cols = json.manifest.schema.columns.map(c => c.name);
  const rows = (json.result && json.result.data_array) || [];
  return rows.map(row => {
    const o = {};
    cols.forEach((c, i) => { o[c] = row[i]; });
    return o;
  });
}

// Roda varias statements em paralelo via UrlFetchApp.fetchAll (uma unica leva
// de requests HTTP em vez de N idas-e-voltas sequenciais). Cada statement usa
// wait_timeout=50s (long-poll), entao a maioria volta ja SUCCEEDED; sobras em
// PENDING/RUNNING sao resolvidas com polling individual (raro nas gold tables).
function dbQueryBatch_(statements) {
  const cfg = getConfig_();
  const base = `https://${cfg.host}/api/2.0/sql/statements`;
  const headers = { Authorization: 'Bearer ' + cfg.token };

  const requests = statements.map(stmt => ({
    url: base,
    method: 'post',
    contentType: 'application/json',
    headers,
    payload: JSON.stringify({ warehouse_id: cfg.warehouseId, statement: stmt, wait_timeout: '50s' }),
    muteHttpExceptions: true,
  }));

  const responses = UrlFetchApp.fetchAll(requests);
  const parsed = responses.map(r => JSON.parse(r.getContentText()));

  for (let i = 0; i < parsed.length; i++) {
    let json = parsed[i];
    let tries = 0;
    while (json.status && (json.status.state === 'PENDING' || json.status.state === 'RUNNING') && tries < 30) {
      Utilities.sleep(1500);
      const resp = UrlFetchApp.fetch(`${base}/${json.statement_id}`, { method: 'get', headers, muteHttpExceptions: true });
      json = JSON.parse(resp.getContentText());
      tries++;
    }
    parsed[i] = json;
  }

  return parsed.map((json, i) => {
    if (!json.status || json.status.state !== 'SUCCEEDED') {
      throw new Error(`Databricks query [${i}] falhou: ` + JSON.stringify(json.status) + ' | SQL: ' + statements[i].slice(0, 200));
    }
    const cols = json.manifest.schema.columns.map(c => c.name);
    const rows = (json.result && json.result.data_array) || [];
    return rows.map(row => {
      const o = {};
      cols.forEach((c, i2) => { o[c] = row[i2]; });
      return o;
    });
  });
}

function sqlEsc_(v) {
  return String(v).replace(/'/g, "''");
}

function sqlInList_(arr) {
  return arr.map(v => `'${sqlEsc_(v)}'`).join(',');
}

// ---------------------------------------------------------------------------
// Opcoes de filtro. Sem CacheService aqui: a lista de 448 vendedores com
// nomes repetidos passa do limite de 100KB por chave do cache do Apps
// Script ("Argumento grande demais"). As queries em si sao baratas (tabelas
// pequenas, sem scan de fato), entao rodar direto a cada load e suficiente.
// ---------------------------------------------------------------------------
function getFiltroOpcoes() {
  const estrutura = dbQuery_(`
    SELECT DISTINCT cod_regional, nome_regional, cod_revenda, nome_revenda, porte_revenda,
           cod_gerente, nome_gerente, cod_supervisor, nome_supervisor, cod_vendedor, nome_vendedor
    FROM ${SCHEMA}.TBCERVA_dim_estrutura_comercial
  `);
  const produtos = dbQuery_(`
    SELECT cod_sku, nome_completo, marca, tipo_produto, categoria_embalagem
    FROM ${SCHEMA}.TBCERVA_dim_produto ORDER BY marca, nome_completo
  `);
  const anos = dbQuery_(`SELECT DISTINCT ano FROM ${SCHEMA}.TBCERVA_dim_calendario ORDER BY ano`);

  return { estrutura, produtos, anos: anos.map(a => a.ano) };
}

// ---------------------------------------------------------------------------
// Periodo selecionado -> [inicio, fim] usando a dim_calendario (evita
// reimplementar regra de semana ISO / trimestre em JS)
// ---------------------------------------------------------------------------
function getPeriodo_(sel) {
  let where;
  if (sel.granularidade === 'dia') {
    where = `data = '${sqlEsc_(sel.dia)}'`;
  } else if (sel.granularidade === 'semana') {
    where = `ano = ${Number(sel.ano)} AND semana_ano = ${Number(sel.semana)}`;
  } else if (sel.granularidade === 'mes') {
    where = `ano = ${Number(sel.ano)} AND mes = ${Number(sel.mes)}`;
  } else if (sel.granularidade === 'trimestre') {
    where = `ano = ${Number(sel.ano)} AND trimestre = ${Number(sel.trimestre)}`;
  } else {
    throw new Error('Granularidade invalida: ' + sel.granularidade);
  }
  const r = dbQuery_(`SELECT MIN(data) AS inicio, MAX(data) AS fim FROM ${SCHEMA}.TBCERVA_dim_calendario WHERE ${where}`);
  if (!r[0].inicio) throw new Error('Periodo sem datas correspondentes. Filtro recebido: ' + JSON.stringify(sel) + ' | WHERE gerado: ' + where);
  return { inicio: r[0].inicio, fim: r[0].fim };
}

function getPeriodoAnterior_(granularidade, inicio) {
  let exprIni, exprFim;
  if (granularidade === 'dia') {
    exprIni = `date_sub('${inicio}', 1)`; exprFim = `date_sub('${inicio}', 1)`;
  } else if (granularidade === 'semana') {
    exprIni = `date_sub('${inicio}', 7)`; exprFim = `date_add(date_sub('${inicio}', 7), 6)`;
  } else if (granularidade === 'mes') {
    exprIni = `add_months('${inicio}', -1)`; exprFim = `date_sub('${inicio}', 1)`;
  } else if (granularidade === 'trimestre') {
    exprIni = `add_months('${inicio}', -3)`; exprFim = `date_sub('${inicio}', 1)`;
  }
  const r = dbQuery_(`SELECT ${exprIni} AS inicio, ${exprFim} AS fim`);
  return { inicio: r[0].inicio, fim: r[0].fim };
}

function anoMesesNoIntervalo_(inicio, fim) {
  const r = dbQuery_(`
    SELECT DISTINCT CONCAT(ano,'-',LPAD(mes,2,'0')) AS ano_mes
    FROM ${SCHEMA}.TBCERVA_dim_calendario WHERE data BETWEEN '${sqlEsc_(inicio)}' AND '${sqlEsc_(fim)}'
    ORDER BY ano_mes
  `);
  return r.map(x => x.ano_mes);
}

// ---------------------------------------------------------------------------
// Filtro de hierarquia comercial -> clausula WHERE reutilizavel
// ---------------------------------------------------------------------------
function whereHierarquia_(filtros, alias) {
  const a = alias ? alias + '.' : '';
  const cond = [];
  if (filtros.cod_regional) cond.push(`${a}cod_regional = '${sqlEsc_(filtros.cod_regional)}'`);
  if (filtros.cod_revenda) cond.push(`${a}cod_revenda = '${sqlEsc_(filtros.cod_revenda)}'`);
  if (filtros.cod_gerente) cond.push(`${a}cod_gerente = '${sqlEsc_(filtros.cod_gerente)}'`);
  if (filtros.cod_supervisor) cond.push(`${a}cod_supervisor = '${sqlEsc_(filtros.cod_supervisor)}'`);
  if (filtros.cod_vendedor) cond.push(`${a}cod_vendedor = '${sqlEsc_(filtros.cod_vendedor)}'`);
  return cond.length ? cond.map(c => 'AND ' + c).join(' ') : '';
}

// ---------------------------------------------------------------------------
// Meta do escopo selecionado (soma ponderada por qtd de vendedores p/ Fluxo;
// valor fixo p/ Percentual)
// ---------------------------------------------------------------------------
function sqlMeta_(kpi, filtros, anoMesList) {
  const where = whereHierarquia_(filtros, null).replace(/^AND /, 'WHERE ');
  const escopoWhere = where || 'WHERE 1=1';
  return `
    WITH escopo AS (
      SELECT cod_vendedor, cod_revenda FROM ${SCHEMA}.TBCERVA_dim_estrutura_comercial ${escopoWhere}
    ),
    tot AS (
      SELECT cod_revenda, COUNT(*) AS vendedores_totais
      FROM ${SCHEMA}.TBCERVA_dim_estrutura_comercial GROUP BY cod_revenda
    ),
    pesos AS (
      SELECT e.cod_revenda, COUNT(*) / t.vendedores_totais AS peso
      FROM escopo e JOIN tot t ON e.cod_revenda = t.cod_revenda
      GROUP BY e.cod_revenda, t.vendedores_totais
    )
    SELECT
      SUM(m.meta_mensal * p.peso) AS meta_fluxo,
      SUM(m.meta_dia_util * p.peso) AS meta_dia_util,
      AVG(m.meta_mensal) AS meta_percentual
    FROM ${SCHEMA}.TBCERVA_fato_metas m
    JOIN pesos p ON m.cod_revenda = p.cod_revenda
    WHERE m.kpi = '${sqlEsc_(kpi)}' AND m.ano_mes IN (${sqlInList_(anoMesList)})
  `;
}

function parseMeta_(rows, tipo) {
  const row = rows[0] || {};
  if (tipo === 'Fluxo') {
    return { valor: Number(row.meta_fluxo) || 0, metaDiaUtil: Number(row.meta_dia_util) || 0 };
  }
  return { valor: Number(row.meta_percentual) || 0, metaDiaUtil: null };
}

// ---------------------------------------------------------------------------
// Realizado por KPI — cada KPI tem um par (monta SQL) / (extrai valor)
// ---------------------------------------------------------------------------
function sqlRealizado_(kpi, filtros, inicio, fim, anoMesList) {
  if (kpi === 'Inadimplencia') {
    return `SELECT SUM(valor_vencido) AS vencido, SUM(valor_total_boleto) AS total
            FROM ${SCHEMA}.TBCERVA_gold_financeiro_mes
            WHERE ano_mes IN (${sqlInList_(anoMesList)}) ${whereHierarquia_(filtros, null)}`;
  }
  if (kpi === 'Clientes sem Compra') {
    const anoMesRef = anoMesList[anoMesList.length - 1]; // ultimo mes do periodo (definicao tipo MTD)
    return `SELECT SUM(clientes_carteira) AS carteira, SUM(clientes_com_compra) AS com_compra
            FROM ${SCHEMA}.TBCERVA_gold_cliente_mes
            WHERE ano_mes = '${sqlEsc_(anoMesRef)}' ${whereHierarquia_(filtros, null)}`;
  }
  if (kpi === 'Eficiencia de Visita') {
    return `SELECT SUM(clientes_com_venda_no_dia) AS vendeu, SUM(visitas_realizadas) AS visitou
            FROM ${SCHEMA}.TBCERVA_gold_visita_dia
            WHERE data BETWEEN '${sqlEsc_(inicio)}' AND '${sqlEsc_(fim)}' ${whereHierarquia_(filtros, null)}`;
  }
  // "Volume" = caixas vendidas (nao unidades individuais) — o PM implicito da
  // meta original (~R$30/"volume") so faz sentido em nivel de caixa; lata
  // avulsa custa R$2-3, nao bateria com a meta se fosse unidade a unidade.
  const campo = { Volume: 'SUM(qtd_caixas)', Hectolitro: 'SUM(volume_hl)', Faturamento: 'SUM(valor_total_reais)' }[kpi];
  const tabela = kpi === 'Devolucoes' ? 'TBCERVA_gold_devolucao_dia' : 'TBCERVA_gold_vendas_dia';
  const campoFinal = kpi === 'Devolucoes' ? 'SUM(valor_total_reais)' : campo;
  const prodFiltro = filtros.cod_sku ? `AND cod_sku = '${sqlEsc_(filtros.cod_sku)}'` : '';
  return `SELECT ${campoFinal} AS v FROM ${SCHEMA}.${tabela}
          WHERE data BETWEEN '${sqlEsc_(inicio)}' AND '${sqlEsc_(fim)}' ${whereHierarquia_(filtros, null)} ${prodFiltro}`;
}

function parseRealizado_(kpi, rows) {
  const r = rows[0] || {};
  if (kpi === 'Inadimplencia') {
    const total = Number(r.total) || 0;
    return total > 0 ? Number(r.vencido) / total : 0;
  }
  if (kpi === 'Clientes sem Compra') {
    const carteira = Number(r.carteira) || 0;
    const comCompra = Number(r.com_compra) || 0;
    return carteira > 0 ? (carteira - comCompra) / carteira : 0;
  }
  if (kpi === 'Eficiencia de Visita') {
    const visitou = Number(r.visitou) || 0;
    return visitou > 0 ? Number(r.vendeu) / visitou : 0;
  }
  return Number(r.v) || 0;
}

// ---------------------------------------------------------------------------
// Card de KPI: realizado, meta (com desvio) e variacao vs periodo anterior
// ---------------------------------------------------------------------------
function montarCardKpi_(kpi, realizado, anterior, meta, ehDiaUnico) {
  const tipo = KPI_TIPO[kpi];
  const ehPercentual = !!KPI_EH_PERCENTUAL_DE_VERDADE[kpi];

  let metaComparavel;
  if (tipo === 'Fluxo') {
    metaComparavel = ehDiaUnico ? meta.metaDiaUtil : meta.valor;
  } else {
    metaComparavel = meta.valor;
  }

  const direcaoMaiorMelhor = kpi !== 'Inadimplencia' && kpi !== 'Clientes sem Compra' && kpi !== 'Devolucoes';

  let desvioMeta = null;
  if (metaComparavel) {
    if (ehPercentual) {
      desvioMeta = { valor: (realizado - metaComparavel) * 100, unidade: 'pp' };
    } else {
      desvioMeta = { valor: metaComparavel !== 0 ? ((realizado - metaComparavel) / metaComparavel) * 100 : null, unidade: '%' };
    }
  }
  const acimaMeta = desvioMeta && desvioMeta.valor != null ? (desvioMeta.valor >= 0) === direcaoMaiorMelhor : null;

  let variacaoAnterior;
  if (ehPercentual) {
    variacaoAnterior = { valor: (realizado - anterior) * 100, unidade: 'pp' };
  } else {
    variacaoAnterior = { valor: anterior !== 0 ? ((realizado - anterior) / anterior) * 100 : null, unidade: '%' };
  }
  const acimaAnterior = variacaoAnterior.valor != null ? (variacaoAnterior.valor >= 0) === direcaoMaiorMelhor : null;

  return {
    kpi, tipo, ehPercentual,
    realizado, meta: metaComparavel, desvioMeta, acimaMeta,
    anterior, variacaoAnterior, acimaAnterior,
  };
}

const OVERVIEW_KPIS = ['Volume', 'Hectolitro', 'Faturamento', 'Inadimplencia', 'Clientes sem Compra', 'Eficiencia de Visita'];

// Todas as ~20 consultas do Overview (6 KPIs x [atual, anterior, meta] + evolutivo
// + top produtos) rodam em UMA leva paralela via UrlFetchApp.fetchAll — rodar
// sequencial levava 20-30s (uma ida-e-volta ao Databricks por vez).
function getOverviewData(sel) {
  const filtros = sel.filtros || {};
  const periodo = getPeriodo_(sel);
  const periodoAnterior = getPeriodoAnterior_(sel.granularidade, periodo.inicio);
  const anoMesList = anoMesesNoIntervalo_(periodo.inicio, periodo.fim);
  const anoMesListAnterior = anoMesesNoIntervalo_(periodoAnterior.inicio, periodoAnterior.fim);
  const ehDiaUnico = sel.granularidade === 'dia';

  const jobs = [];
  OVERVIEW_KPIS.forEach(kpi => {
    jobs.push({ key: kpi + '|atual', sql: sqlRealizado_(kpi, filtros, periodo.inicio, periodo.fim, anoMesList) });
    jobs.push({ key: kpi + '|anterior', sql: sqlRealizado_(kpi, filtros, periodoAnterior.inicio, periodoAnterior.fim, anoMesListAnterior) });
    jobs.push({ key: kpi + '|meta', sql: sqlMeta_(kpi, filtros, anoMesList) });
  });
  jobs.push({
    key: 'evolutivo', sql: `
    SELECT data,
           SUM(volume_hl) AS hl,
           SUM(valor_total_reais) AS faturamento,
           SUM(valor_total_reais) / NULLIF(SUM(qtd_caixas), 0) AS pm
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia
    WHERE data BETWEEN '${sqlEsc_(periodo.inicio)}' AND '${sqlEsc_(periodo.fim)}' ${whereHierarquia_(filtros, null)}
    GROUP BY data ORDER BY data`
  });
  jobs.push({
    key: 'topProdutos', sql: `
    SELECT p.nome_completo, SUM(g.valor_total_reais) AS faturamento, SUM(g.volume_hl) AS hl
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia g
    JOIN ${SCHEMA}.TBCERVA_dim_produto p ON g.cod_sku = p.cod_sku
    WHERE g.data BETWEEN '${sqlEsc_(periodo.inicio)}' AND '${sqlEsc_(periodo.fim)}' ${whereHierarquia_(filtros, 'g')}
    GROUP BY p.nome_completo ORDER BY faturamento DESC LIMIT 10`
  });
  jobs.push({ key: 'bcgAtual', sql: sqlVolumeProduto_(filtros, periodo.inicio, periodo.fim) });
  jobs.push({ key: 'bcgAnterior', sql: sqlVolumeProduto_(filtros, periodoAnterior.inicio, periodoAnterior.fim) });
  jobs.push({
    // Ranking respeita o filtro de Regional (se houver) mas ignora Revenda/
    // Gerente/Supervisor/Vendedor pra baixo — ranquear revendas so faz sentido
    // no nivel de revenda pra cima.
    key: 'rankingRevendas', sql: `
    SELECT e.cod_revenda, e.nome_revenda, e.porte_revenda, SUM(g.valor_total_reais) AS faturamento
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia g
    JOIN ${SCHEMA}.TBCERVA_dim_estrutura_comercial e ON g.cod_vendedor = e.cod_vendedor
    WHERE g.data BETWEEN '${sqlEsc_(periodo.inicio)}' AND '${sqlEsc_(periodo.fim)}'
      ${filtros.cod_regional ? `AND e.cod_regional = '${sqlEsc_(filtros.cod_regional)}'` : ''}
    GROUP BY e.cod_revenda, e.nome_revenda, e.porte_revenda`
  });
  jobs.push({
    key: 'rankingRevendasMeta', sql: `
    SELECT cod_revenda, SUM(meta_mensal) AS meta
    FROM ${SCHEMA}.TBCERVA_fato_metas
    WHERE kpi = 'Faturamento' AND ano_mes IN (${sqlInList_(anoMesList)})
    GROUP BY cod_revenda`
  });
  jobs.push({
    key: 'regionalDonut', sql: `
    SELECT e.cod_regional, e.nome_regional, SUM(g.valor_total_reais) AS faturamento
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia g
    JOIN ${SCHEMA}.TBCERVA_dim_estrutura_comercial e ON g.cod_vendedor = e.cod_vendedor
    WHERE g.data BETWEEN '${sqlEsc_(periodo.inicio)}' AND '${sqlEsc_(periodo.fim)}' ${whereHierarquia_(filtros, 'e')}
    GROUP BY e.cod_regional, e.nome_regional`
  });

  const resultados = dbQueryBatch_(jobs.map(j => j.sql));
  const porChave = {};
  jobs.forEach((j, i) => { porChave[j.key] = resultados[i]; });

  const cards = OVERVIEW_KPIS.map(kpi => montarCardKpi_(
    kpi,
    parseRealizado_(kpi, porChave[kpi + '|atual']),
    parseRealizado_(kpi, porChave[kpi + '|anterior']),
    parseMeta_(porChave[kpi + '|meta'], KPI_TIPO[kpi]),
    ehDiaUnico
  ));

  const bcg = montarMatrizBcg_(porChave['bcgAtual'], porChave['bcgAnterior']);
  const rankingRevendas = montarRankingRevendas_(porChave['rankingRevendas'], porChave['rankingRevendasMeta']);

  return {
    periodo, periodoAnterior, cards,
    evolutivo: porChave['evolutivo'], topProdutos: porChave['topProdutos'], bcg,
    rankingRevendas, regionalDonut: porChave['regionalDonut'],
  };
}

function montarRankingRevendas_(realizado, meta) {
  const metaPorRevenda = {};
  meta.forEach(r => { metaPorRevenda[r.cod_revenda] = Number(r.meta) || 0; });

  return realizado.map(r => {
    const faturamento = Number(r.faturamento) || 0;
    const metaRevenda = metaPorRevenda[r.cod_revenda] || 0;
    const desvioPct = metaRevenda > 0 ? (faturamento - metaRevenda) / metaRevenda * 100 : null;
    return {
      cod_revenda: r.cod_revenda, nome_revenda: r.nome_revenda, porte_revenda: r.porte_revenda,
      faturamento, meta: metaRevenda, desvioPct, acimaMeta: desvioPct !== null ? desvioPct >= 0 : null,
    };
  }).sort((a, b) => b.faturamento - a.faturamento);
}

function sqlVolumeProduto_(filtros, inicio, fim) {
  return `
    SELECT g.cod_sku, p.nome_completo, p.marca, p.embalagem_fisica, p.capacidade_ml, p.categoria_embalagem,
           SUM(g.volume_hl) AS volume_hl, SUM(g.valor_total_reais) AS faturamento
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia g
    JOIN ${SCHEMA}.TBCERVA_dim_produto p ON g.cod_sku = p.cod_sku
    WHERE g.data BETWEEN '${sqlEsc_(inicio)}' AND '${sqlEsc_(fim)}' ${whereHierarquia_(filtros, 'g')}
    GROUP BY g.cod_sku, p.nome_completo, p.marca, p.embalagem_fisica, p.capacidade_ml, p.categoria_embalagem
  `;
}

// Margem estimada (nao ha custo real na base). Retornavel/Barril tem custo de
// materia-prima bem menor por reuso da embalagem (garrafa retornavel, barril)
// vs Descartavel (lata/PET novos a cada venda) — diferenca de margem real e
// conhecida no setor. Ajuste fino por marca (posicionamento de preco).
function margemEstimada_(categoriaEmbalagem, marca) {
  const baseCategoria = { 'Retornavel': 0.60, 'Barril': 0.63, 'Descartavel': 0.31 }[categoriaEmbalagem] || 0.35;
  const multMarca = { 'Pararanguari': 1.15, 'Maltetop': 1.0, 'Soceva': 0.82 }[marca] || 1.0;
  return Math.min(0.75, Math.max(0.15, baseCategoria * multMarca));
}

// ---------------------------------------------------------------------------
// Matriz BCG (Overview): eixo X = rentabilidade (lucro estimado = faturamento
// x margem por categoria de embalagem/marca — retornavel e barril rendem mais
// que descartavel, que tem custo de embalagem novo a cada venda); eixo Y =
// crescimento do lucro estimado vs periodo anterior. Posicionamento por
// percentil (nao valor bruto) para os pontos ocuparem os 4 quadrantes de
// forma robusta mesmo com distribuicao de cauda longa (poucos SKUs dominando
// volume), em vez de todos caindo no mesmo canto.
// ---------------------------------------------------------------------------
function montarMatrizBcg_(atual, anterior) {
  const anteriorPorSku = {};
  anterior.forEach(r => {
    const margem = margemEstimada_(r.categoria_embalagem, r.marca);
    anteriorPorSku[r.cod_sku] = (Number(r.faturamento) || 0) * margem;
  });

  let produtos = atual.map(r => {
    const margem = margemEstimada_(r.categoria_embalagem, r.marca);
    const faturamentoAtual = Number(r.faturamento) || 0;
    const lucroAtual = faturamentoAtual * margem;
    const lucroAnterior = anteriorPorSku[r.cod_sku] || 0;
    return {
      cod_sku: r.cod_sku, nome: r.nome_completo, marca: r.marca,
      embalagem_fisica: r.embalagem_fisica, capacidade_ml: Number(r.capacidade_ml) || 0,
      categoria_embalagem: r.categoria_embalagem, margem,
      volume_hl: Number(r.volume_hl) || 0, faturamento: faturamentoAtual, lucroAtual,
    };
  }).filter(p => p.volume_hl > 0);

  const totalLucro = produtos.reduce((s, p) => s + p.lucroAtual, 0);
  produtos.forEach(p => {
    p.rentabilidade = totalLucro > 0 ? p.lucroAtual / totalLucro : 0;
    const lucroAnterior = anteriorPorSku[p.cod_sku] || 0;
    if (lucroAnterior > 0) p.crescimento = (p.lucroAtual - lucroAnterior) / lucroAnterior;
    else p.crescimento = p.lucroAtual > 0 ? 1 : 0; // produto novo no periodo = tratado como alto crescimento
  });

  const n = produtos.length;
  if (n === 0) return [];

  // participacao/percentilParticipacao mantidos como nomes de campo (consumidos
  // pelo client) mas agora representam rentabilidade, nao participacao de volume.
  const porRentabilidadeDesc = produtos.slice().sort((a, b) => b.rentabilidade - a.rentabilidade);
  porRentabilidadeDesc.forEach((p, i) => { p.percentilParticipacao = n > 1 ? i / (n - 1) : 0.5; }); // 0 = maior rentabilidade

  const porCrescimentoDesc = produtos.slice().sort((a, b) => b.crescimento - a.crescimento);
  porCrescimentoDesc.forEach((p, i) => { p.percentilCrescimento = n > 1 ? i / (n - 1) : 0.5; }); // 0 = maior crescimento

  produtos.forEach(p => {
    const altaRentabilidade = p.percentilParticipacao < 0.5;
    const altoCrescimento = p.percentilCrescimento < 0.5;
    if (altaRentabilidade && altoCrescimento) p.quadrante = 'Estrela';
    else if (!altaRentabilidade && altoCrescimento) p.quadrante = 'Em Questionamento';
    else if (altaRentabilidade && !altoCrescimento) p.quadrante = 'Vaca Leiteira';
    else p.quadrante = 'Abacaxi';
    p.participacao = p.rentabilidade; // compat com o client (rotulo/tooltip)
  });

  return produtos;
}

// ---------------------------------------------------------------------------
// Pagina de Analise: tabela + quadrante Volume x PM por produto
// ---------------------------------------------------------------------------
function getAnaliseData(sel) {
  const filtros = sel.filtros || {};
  const periodo = getPeriodo_(sel);
  const prodFiltro = filtros.cod_sku ? `AND g.cod_sku = '${sqlEsc_(filtros.cod_sku)}'` : '';

  const tabela = dbQuery_(`
    SELECT g.cod_sku, p.nome_completo, p.marca, p.tipo_produto,
           SUM(g.qtd_caixas) AS qtd_caixas, SUM(g.volume_hl) AS volume_hl,
           SUM(g.valor_total_reais) AS faturamento,
           SUM(g.valor_total_reais) / NULLIF(SUM(g.qtd_caixas), 0) AS pm
    FROM ${SCHEMA}.TBCERVA_gold_vendas_dia g
    JOIN ${SCHEMA}.TBCERVA_dim_produto p ON g.cod_sku = p.cod_sku
    WHERE g.data BETWEEN '${sqlEsc_(periodo.inicio)}' AND '${sqlEsc_(periodo.fim)}' ${whereHierarquia_(filtros, 'g')} ${prodFiltro}
    GROUP BY g.cod_sku, p.nome_completo, p.marca, p.tipo_produto
    ORDER BY faturamento DESC
  `);

  const volumes = tabela.map(r => Number(r.qtd_caixas) || 0);
  const pms = tabela.map(r => Number(r.pm) || 0);
  const medianaVolume = mediana_(volumes);
  const medianaPm = mediana_(pms);

  const quadrante = tabela.map(r => ({
    nome: r.nome_completo, marca: r.marca,
    volume: Number(r.qtd_caixas) || 0, pm: Number(r.pm) || 0,
    quadrante: classificarQuadrante_(Number(r.qtd_caixas) || 0, Number(r.pm) || 0, medianaVolume, medianaPm),
  }));

  const projecao = montarProjecao_(sel.granularidade, periodo, filtros);

  return { periodo, tabela, quadrante, medianaVolume, medianaPm, projecao };
}

function mediana_(arr) {
  const s = arr.slice().sort((a, b) => a - b);
  if (!s.length) return 0;
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

function classificarQuadrante_(volume, pm, medVolume, medPm) {
  if (volume >= medVolume && pm >= medPm) return 'Alto giro / Alto PM';
  if (volume >= medVolume && pm < medPm) return 'Alto giro / Baixo PM';
  if (volume < medVolume && pm >= medPm) return 'Baixo giro / Alto PM';
  return 'Baixo giro / Baixo PM';
}

// ---------------------------------------------------------------------------
// Projecao linear (regressao OLS) — Faturamento geral ou de um produto (se
// filtrado), com banda de erro (intervalo de previsao) e comparacao com o
// realizado quando a janela projetada ja tem dado real disponivel (permite
// "backtestar" visualmente: a linha de projecao passa paralela a realizada).
//
// Nao usamos ARIMA/Prophet/ML pesado de proposito: regressao linear OLS ja e
// um modelo estatistico legitimo, roda instantaneo sem infra extra (nao tem
// como rodar Python/sklearn a partir do Apps Script sem um servico externo),
// e o ganho de um modelo mais sofisticado seria marginal pra dados sinteticos
// com sazonalidade ja conhecida.
// ---------------------------------------------------------------------------
const PROJECAO_CONFIG = {
  dia: { unit: 'DAY', nHist: 7, nProj: 7 },
  semana: { unit: 'WEEK', nHist: 8, nProj: 8 },
  mes: { unit: 'MONTH', nHist: 12, nProj: 6 },
  trimestre: { unit: 'QUARTER', nHist: 8, nProj: 4 },
};

// t-critico (95%) por grau de liberdade (n-2) — melhora a banda de erro em
// amostras pequenas em vez de usar sempre a aproximacao normal (1.96).
const T_CRITICO_95 = { 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228 };
function tCritico_(df) {
  return T_CRITICO_95[df] || 1.96;
}

const MAX_DATA_DISPONIVEL = '2020-12-31'; // limite fixo da base sintetica gerada

function sqlJanelaProjecao_(gran, periodo) {
  const cfg = PROJECAO_CONFIG[gran];
  let histStart, projEnd;
  if (gran === 'dia') {
    histStart = `date_sub('${periodo.inicio}', ${cfg.nHist - 1})`;
    projEnd = `date_add('${periodo.fim}', ${cfg.nProj})`;
  } else if (gran === 'semana') {
    histStart = `date_sub('${periodo.inicio}', ${(cfg.nHist - 1) * 7})`;
    projEnd = `date_add('${periodo.fim}', ${cfg.nProj * 7})`;
  } else if (gran === 'mes') {
    histStart = `add_months('${periodo.inicio}', -${cfg.nHist - 1})`;
    projEnd = `last_day(add_months('${periodo.fim}', ${cfg.nProj}))`;
  } else {
    histStart = `add_months('${periodo.inicio}', -${(cfg.nHist - 1) * 3})`;
    projEnd = `last_day(add_months('${periodo.fim}', ${cfg.nProj * 3}))`;
  }
  return `SELECT ${histStart} AS hist_start, ${projEnd} AS proj_end`;
}

function montarProjecao_(gran, periodo, filtros) {
  const cfg = PROJECAO_CONFIG[gran];
  const janela = dbQuery_(sqlJanelaProjecao_(gran, periodo))[0];
  const histStart = janela.hist_start;
  const projEnd = janela.proj_end;
  const prodFiltro = filtros.cod_sku ? `AND cod_sku = '${sqlEsc_(filtros.cod_sku)}'` : '';

  const rows = dbQuery_(`
    WITH dias AS (
      SELECT explode(sequence(to_date('${sqlEsc_(histStart)}'), to_date('${sqlEsc_(projEnd)}'), interval 1 day)) AS d
    ),
    buckets AS (
      SELECT DISTINCT date_trunc('${cfg.unit}', d) AS bucket FROM dias
    ),
    vendas AS (
      SELECT date_trunc('${cfg.unit}', data) AS bucket, SUM(valor_total_reais) AS faturamento
      FROM ${SCHEMA}.TBCERVA_gold_vendas_dia
      WHERE data BETWEEN '${sqlEsc_(histStart)}' AND '${sqlEsc_(projEnd)}' ${whereHierarquia_(filtros, null)} ${prodFiltro}
      GROUP BY date_trunc('${cfg.unit}', data)
    )
    SELECT b.bucket, COALESCE(v.faturamento, 0) AS faturamento
    FROM buckets b LEFT JOIN vendas v ON b.bucket = v.bucket
    ORDER BY b.bucket
  `);

  const historico = rows.slice(0, cfg.nHist);
  const futuro = rows.slice(cfg.nHist, cfg.nHist + cfg.nProj);

  // regressao OLS sobre o historico: x = indice (0..nHist-1), y = faturamento
  const n = historico.length;
  const xs = historico.map((_, i) => i);
  const ys = historico.map(r => Number(r.faturamento) || 0);
  const xMean = xs.reduce((a, b) => a + b, 0) / n;
  const yMean = ys.reduce((a, b) => a + b, 0) / n;
  let sxy = 0, sxx = 0;
  for (let i = 0; i < n; i++) { sxy += (xs[i] - xMean) * (ys[i] - yMean); sxx += (xs[i] - xMean) * (xs[i] - xMean); }
  const slope = sxx !== 0 ? sxy / sxx : 0;
  const intercept = yMean - slope * xMean;

  let sce = 0, sct = 0;
  for (let i = 0; i < n; i++) {
    const pred = slope * xs[i] + intercept;
    sce += (ys[i] - pred) * (ys[i] - pred);
    sct += (ys[i] - yMean) * (ys[i] - yMean);
  }
  const df = Math.max(1, n - 2);
  const rse = Math.sqrt(sce / df);
  const r2 = sct !== 0 ? 1 - sce / sct : 0;
  const tCrit = tCritico_(df);

  const buckets = rows.map(r => r.bucket);
  const historicoSerie = historico.map(r => Number(r.faturamento) || 0);
  const projecaoSerie = new Array(n).fill(null);
  const projecaoMin = new Array(n).fill(null);
  const projecaoMax = new Array(n).fill(null);
  const realizadoFuturo = new Array(n).fill(null);

  // ultimo ponto historico repetido no inicio da projecao, so pra linha tracejada
  // conectar visualmente sem buraco no grafico
  projecaoSerie.push(historicoSerie[n - 1]);
  projecaoMin.push(historicoSerie[n - 1]);
  projecaoMax.push(historicoSerie[n - 1]);
  historicoSerie.push(null);
  realizadoFuturo.push(null);

  futuro.forEach((r, i) => {
    const x = cfg.nHist + i;
    const pred = slope * x + intercept;
    const margem = tCrit * rse * Math.sqrt(1 + 1 / n + ((x - xMean) * (x - xMean)) / sxx);
    projecaoSerie.push(Math.max(0, pred));
    projecaoMin.push(Math.max(0, pred - margem));
    projecaoMax.push(pred + margem);
    historicoSerie.push(null);

    // so mostra "realizado (comparacao)" se o bucket ja tem dado real disponivel
    // (bucket dentro do limite de dados da base); senao fica null (projecao pura)
    const bucketDate = r.bucket; // string yyyy-mm-dd
    realizadoFuturo.push(bucketDate <= MAX_DATA_DISPONIVEL ? Number(r.faturamento) || 0 : null);
  });

  return {
    granularidade: gran, unidade: cfg.unit, nHist: cfg.nHist, nProj: cfg.nProj,
    buckets, historico: historicoSerie, projecao: projecaoSerie,
    projecaoMin, projecaoMax, realizadoFuturo,
    slope, r2: Math.max(0, Math.min(1, r2)),
  };
}

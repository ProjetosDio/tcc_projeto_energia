"""Tempo real de ingestão bronze por carga, extraído dos logs do scheduler.

data_fim - data_inicio em public.tbl_controle_carga não mede a ingestão
completa: NOW() devolve o início da transação, e no caminho de sucesso a
transação do UPDATE final começa no TRUNCATE, antes dos INSERTs (ver
docs/notas_tecnicas.md, §1.1). Este script usa os logs que
ingestao_bronze.py escreve no stdout — capturado pelo contêiner do
scheduler — e mede, por id_carga:

  tempo_ingestao_ms  "ID da carga criada: N"  ->  "Carga concluída com sucesso"
  pre_leitura_ms     "ID da carga criada: N"  ->  "Lendo arquivo: ..."
                     (no TRATAMENTO contém validar_schema_csv(); no BASELINE
                     a validação é pulada)
  validacao_schema_ms "ID da carga criada: N" -> "Schema validado com sucesso"
                     (só existe no TRATAMENTO)

Cada carga é associada a grupo/tipo_falha_injetada via public.tbl_experimento
(id_carga_afetada). Resultado por carga em
scripts/experimento/resultados/05_tempo_ingestao.csv; resumo estatístico por
grupo x cenário em 05_tempo_ingestao_resumo.csv e no stdout, junto com o
overhead TRATAMENTO - BASELINE no cenário NENHUMA (Mann-Whitney e t de Welch).

Roda no HOST (como os demais scripts de scripts/experimento/).

Uso:
    python analise_tempo_ingestao.py                       # lê docker logs do scheduler
    python analise_tempo_ingestao.py --arquivo-log x.log   # lê uma cópia salva do log
"""
import argparse
import csv
import os
import re
import statistics
import subprocess
from datetime import datetime
from pathlib import Path

import psycopg2
from scipy import stats

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia_db")
DB_USER = os.getenv("DB_USER", "energia_user")
DB_PASS = os.getenv("DB_PASS", "energia_pass")

SCHEDULER_CONTAINER = os.getenv(
    "AIRFLOW_CONTAINER", "projeto_tcc_energia-airflow-scheduler-1"
)

RESULTADOS_DIR = Path(__file__).resolve().parent / "resultados"
CSV_CARGAS = RESULTADOS_DIR / "05_tempo_ingestao.csv"
CSV_RESUMO = RESULTADOS_DIR / "05_tempo_ingestao_resumo.csv"

# Formato do logging.basicConfig de ingestao_bronze.py:
# '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
RE_LINHA = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\S+) - \w+ - (.*)$"
)
RE_ID_CRIADA = re.compile(r"^ID da carga criada: (\d+)")
RE_CONCLUIDA = re.compile(r"^Carga concluída com sucesso\. Registros inseridos: (\d+)")

# Cenários reportados, na ordem do relatório
CENARIOS = [
    ("BASELINE", "NENHUMA"),
    ("TRATAMENTO", "NENHUMA"),
    ("TRATAMENTO", "CONEXAO"),
    ("TRATAMENTO", "DADOS_NULOS"),
    ("TRATAMENTO", "ARQUIVO_AUSENTE"),
]


def ler_log(arquivo_log):
    if arquivo_log:
        with open(arquivo_log, encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()
    saida = subprocess.run(
        ["docker", "logs", SCHEDULER_CONTAINER],
        capture_output=True, check=True,
    )
    # docker logs manda stdout e stderr do contêiner separados; o logging do
    # Python escreve em stderr, então os dois são concatenados.
    texto = (saida.stdout + saida.stderr).decode("utf-8", errors="replace")
    return texto.splitlines()


def extrair_tempos(linhas):
    """Percorre o log e devolve {id_carga: dict de marcos (datetime)}.

    As execuções do experimento são sequenciais, então todas as mensagens
    de __main__/validador_schema entre "ID da carga criada: N" e o próximo
    "ID da carga criada" pertencem à carga N.
    """
    cargas = {}
    atual = None
    for linha in linhas:
        m = RE_LINHA.match(linha)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f")
        origem, msg = m.group(2), m.group(3)

        m_id = RE_ID_CRIADA.match(msg)
        if m_id:
            atual = {"id_criada": ts}
            cargas[int(m_id.group(1))] = atual
            continue
        if atual is None:
            continue
        if origem == "validador_schema" and msg.startswith("Schema validado com sucesso"):
            atual.setdefault("schema_validado", ts)
        elif msg.startswith("Lendo arquivo:"):
            atual.setdefault("lendo_arquivo", ts)
        else:
            m_ok = RE_CONCLUIDA.match(msg)
            if m_ok:
                atual.setdefault("concluida", ts)
                atual.setdefault("registros_log", int(m_ok.group(1)))
    return cargas


def ms(inicio, fim):
    if inicio is None or fim is None:
        return None
    return (fim - inicio).total_seconds() * 1000


def buscar_experimentos():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.id_experimento, e.grupo, e.tipo_falha_injetada, e.iteracao,
                       e.observacoes, c.id_carga, c.status, c.qtd_registros
                  FROM public.tbl_experimento e
                  JOIN public.tbl_controle_carga c ON c.id_carga = e.id_carga_afetada
                 ORDER BY c.id_carga;
                """
            )
            colunas = [d[0] for d in cur.description]
            return [dict(zip(colunas, linha)) for linha in cur.fetchall()]
    finally:
        conn.close()


def resumir(valores):
    return {
        "n": len(valores),
        "media_ms": statistics.mean(valores),
        "dp_ms": statistics.stdev(valores) if len(valores) > 1 else None,
        "min_ms": min(valores),
        "max_ms": max(valores),
        "mediana_ms": statistics.median(valores),
    }


def pct(diferenca, referencia):
    # Percentual sem sentido quando a referência é ~0 (ex.: pre_leitura_ms
    # no BASELINE, que não valida schema)
    if referencia < 1:
        return "% n/a"
    return f"{100 * diferenca / referencia:+.2f} %"


def fmt(v, casas=2):
    return "" if v is None else f"{v:.{casas}f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--arquivo-log",
        help="Cópia salva do log do scheduler (default: lê via docker logs)",
    )
    args = parser.parse_args()

    tempos = extrair_tempos(ler_log(args.arquivo_log))
    experimentos = buscar_experimentos()

    registros = []
    for exp in experimentos:
        marcos = tempos.get(exp["id_carga"], {})
        registros.append({
            "id_carga": exp["id_carga"],
            "id_experimento": exp["id_experimento"],
            "grupo": exp["grupo"],
            "tipo_falha_injetada": exp["tipo_falha_injetada"],
            "iteracao": exp["iteracao"],
            "dag_state": exp["observacoes"],
            "status_carga": exp["status"],
            "qtd_registros": exp["qtd_registros"],
            "registros_log": marcos.get("registros_log"),
            "ts_id_criada": marcos.get("id_criada"),
            "ts_concluida": marcos.get("concluida"),
            "tempo_ingestao_ms": ms(marcos.get("id_criada"), marcos.get("concluida")),
            "pre_leitura_ms": ms(marcos.get("id_criada"), marcos.get("lendo_arquivo")),
            "validacao_schema_ms": ms(marcos.get("id_criada"), marcos.get("schema_validado")),
            "leitura_ate_concluida_ms": ms(marcos.get("lendo_arquivo"), marcos.get("concluida")),
        })

    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    campos = list(registros[0].keys())
    with open(CSV_CARGAS, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for r in registros:
            writer.writerow({
                k: (v.isoformat(sep=" ", timespec="milliseconds") if isinstance(v, datetime)
                    else fmt(v, 0) if isinstance(v, float) else ("" if v is None else v))
                for k, v in r.items()
            })
    print(f"Por carga: {CSV_CARGAS} ({len(registros)} linhas)")

    # --- Resumo por grupo x cenário (só cargas com o intervalo completo no log)
    linhas_resumo = []
    amostras = {}
    print("\nMétrica | grupo | cenário | n | média | dp | mín | máx | mediana (ms)")
    for metrica in ("tempo_ingestao_ms", "pre_leitura_ms", "leitura_ate_concluida_ms",
                    "validacao_schema_ms"):
        for grupo, cenario in CENARIOS:
            valores = [
                r[metrica] for r in registros
                if r["grupo"] == grupo and r["tipo_falha_injetada"] == cenario
                and r[metrica] is not None
            ]
            if not valores:
                continue
            amostras[(metrica, grupo, cenario)] = valores
            s = resumir(valores)
            linhas_resumo.append({"metrica": metrica, "grupo": grupo, "cenario": cenario, **s})
            print(f"{metrica} | {grupo} | {cenario} | {s['n']} | {fmt(s['media_ms'])} | "
                  f"{fmt(s['dp_ms'])} | {fmt(s['min_ms'], 0)} | {fmt(s['max_ms'], 0)} | "
                  f"{fmt(s['mediana_ms'], 1)}")

    with open(CSV_RESUMO, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["metrica", "grupo", "cenario", "n", "media_ms", "dp_ms",
                           "min_ms", "max_ms", "mediana_ms"],
        )
        writer.writeheader()
        for linha in linhas_resumo:
            writer.writerow({k: (fmt(v) if isinstance(v, float) else v) for k, v in linha.items()})
    print(f"\nResumo: {CSV_RESUMO}")

    # --- Overhead no cenário NENHUMA: TRATAMENTO - BASELINE
    for metrica in ("tempo_ingestao_ms", "pre_leitura_ms", "leitura_ate_concluida_ms"):
        base = amostras.get((metrica, "BASELINE", "NENHUMA"))
        trat = amostras.get((metrica, "TRATAMENTO", "NENHUMA"))
        if not base or not trat:
            print(f"\n[{metrica}] sem amostras NENHUMA nos dois grupos — overhead não calculado.")
            continue
        m_b, m_t = statistics.mean(base), statistics.mean(trat)
        md_b, md_t = statistics.median(base), statistics.median(trat)
        mw = stats.mannwhitneyu(trat, base, alternative="two-sided")
        welch = stats.ttest_ind(trat, base, equal_var=False)
        print(f"\n[{metrica}] overhead NENHUMA (TRATAMENTO - BASELINE)")
        print(f"  médias:   {m_t:.2f} - {m_b:.2f} = {m_t - m_b:+.2f} ms ({pct(m_t - m_b, m_b)})")
        print(f"  medianas: {md_t:.1f} - {md_b:.1f} = {md_t - md_b:+.1f} ms ({pct(md_t - md_b, md_b)})")
        print(f"  Mann-Whitney U (bilateral): U={mw.statistic:.1f}  p={mw.pvalue:.4g}")
        print(f"  t de Welch:                 t={welch.statistic:.3f}  p={welch.pvalue:.4g}  "
              f"gl={welch.df:.1f}")


if __name__ == "__main__":
    main()

"""Exporta os resultados do experimento comparativo BASELINE x TRATAMENTO.

Executa as 5 consultas de sql/consultas/02_analise_experimento.sql (as
queries aqui são cópias literais daquele arquivo — mantenha os dois em
sincronia se alterar uma consulta) e gera:
  - um .csv por consulta em scripts/experimento/resultados/
  - resultados_consolidado.md com todas as tabelas em markdown, prontas
    para colar na seção "Resultados e Discussão" do TCC

Roda no HOST, como os demais scripts de scripts/experimento/ e
scripts/injecao_falhas/ (conecta ao Postgres via localhost:5432).

Uso:
    python exportar_resultados.py
"""
import csv
import os
from pathlib import Path

import psycopg2

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia_db")
DB_USER = os.getenv("DB_USER", "energia_user")
DB_PASS = os.getenv("DB_PASS", "energia_pass")

RESULTADOS_DIR = Path(__file__).resolve().parent / "resultados"

CONSULTAS = [
    {
        "arquivo": "01_taxa_sucesso_por_grupo_e_falha.csv",
        "titulo": "Taxa de sucesso por grupo e tipo de falha",
        "descricao": (
            "Para cada combinação (grupo, tipo de falha injetada), total de "
            "execuções e fração que o runner observou como concluída com "
            "sucesso (e.observacoes='success'). LEFT JOIN: falhas graves "
            "(ex.: CONEXAO no BASELINE) podem nem chegar a gravar uma "
            "carga em tbl_controle_carga, e não podem sumir da contagem."
        ),
        "sql": """
            SELECT grupo, tipo_falha_injetada,
                   COUNT(*) as total_execucoes,
                   SUM(CASE WHEN e.observacoes='success' THEN 1 ELSE 0 END) as sucessos,
                   ROUND(100.0 * SUM(CASE WHEN e.observacoes='success' THEN 1 ELSE 0 END)
                         / COUNT(*), 2) as taxa_sucesso_pct
            FROM public.tbl_experimento e
            LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
            GROUP BY grupo, tipo_falha_injetada
            ORDER BY tipo_falha_injetada, grupo;
        """,
    },
    {
        "arquivo": "02_mttr_por_tipo_falha.csv",
        "titulo": "MTTR (tempo médio de recuperação) por tipo de falha",
        "descricao": (
            "Apenas grupo TRATAMENTO, apenas cargas com "
            "resolvido_automaticamente=TRUE: quão rápido cada mecanismo de "
            "autocorreção resolveu a falha."
        ),
        "sql": """
            SELECT tipo_falha, COUNT(*) as execucoes_recuperadas,
                   ROUND(AVG(tempo_recuperacao_ms)::numeric, 2) as mttr_ms,
                   ROUND(AVG(tempo_recuperacao_ms)::numeric / 1000, 3) as mttr_s,
                   MIN(tempo_recuperacao_ms) as min_ms,
                   MAX(tempo_recuperacao_ms) as max_ms
            FROM public.tbl_experimento e
            JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
            WHERE e.grupo = 'TRATAMENTO'
              AND tcc.resolvido_automaticamente = TRUE
              AND tcc.tempo_recuperacao_ms IS NOT NULL
            GROUP BY tipo_falha;
        """,
    },
    {
        "arquivo": "03_pct_auto_resolvido.csv",
        "titulo": "Percentual de falhas resolvidas automaticamente (TRATAMENTO)",
        "descricao": (
            "Das execuções em que uma falha foi injetada (exclui o "
            "controle NENHUMA), fração que terminou auto-resolvida."
        ),
        "sql": """
            SELECT tipo_falha_injetada,
                   COUNT(*) as execucoes,
                   SUM(CASE WHEN tcc.resolvido_automaticamente THEN 1 ELSE 0 END) as auto_resolvidas,
                   ROUND(100.0 * SUM(CASE WHEN tcc.resolvido_automaticamente THEN 1 ELSE 0 END)
                         / COUNT(*), 2) as pct_auto_resolvido
            FROM public.tbl_experimento e
            JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
            WHERE e.grupo = 'TRATAMENTO' AND e.tipo_falha_injetada != 'NENHUMA'
            GROUP BY tipo_falha_injetada;
        """,
    },
    {
        "arquivo": "04_overhead_execucao.csv",
        "titulo": "Overhead médio de execução (sem falha injetada)",
        "descricao": (
            "Compara o tempo total de carga entre BASELINE e TRATAMENTO "
            "apenas nas execuções de controle (tipo_falha_injetada='NENHUMA'), "
            "isolando o custo dos próprios mecanismos de autocorreção."
        ),
        "sql": """
            SELECT grupo,
                   COUNT(*) as execucoes,
                   ROUND(AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio)) * 1000)::numeric, 2) as tempo_medio_ms
            FROM public.tbl_experimento e
            LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
            WHERE e.tipo_falha_injetada = 'NENHUMA'
            GROUP BY grupo;
        """,
    },
    {
        "arquivo": "05_consolidado_baseline_vs_tratamento.csv",
        "titulo": "Consolidado: BASELINE x TRATAMENTO por tipo de falha",
        "descricao": (
            "Tabela principal do TCC: sucessos e taxa de sucesso de cada "
            "grupo lado a lado, por tipo de falha injetada. Sucesso = "
            "e.observacoes='success'; LEFT JOIN para não descartar "
            "execuções sem carga registrada."
        ),
        "sql": """
            SELECT tipo_falha_injetada as "Tipo de Falha",
                   SUM(CASE WHEN grupo='BASELINE' AND e.observacoes='success' THEN 1 ELSE 0 END) as "Baseline Sucesso",
                   SUM(CASE WHEN grupo='TRATAMENTO' AND e.observacoes='success' THEN 1 ELSE 0 END) as "Tratamento Sucesso",
                   ROUND(100.0 * SUM(CASE WHEN grupo='BASELINE' AND e.observacoes='success' THEN 1 ELSE 0 END)
                         / NULLIF(SUM(CASE WHEN grupo='BASELINE' THEN 1 ELSE 0 END), 0), 1) as "Baseline %",
                   ROUND(100.0 * SUM(CASE WHEN grupo='TRATAMENTO' AND e.observacoes='success' THEN 1 ELSE 0 END)
                         / NULLIF(SUM(CASE WHEN grupo='TRATAMENTO' THEN 1 ELSE 0 END), 0), 1) as "Tratamento %"
            FROM public.tbl_experimento e
            LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
            WHERE tipo_falha_injetada != 'NENHUMA'
            GROUP BY tipo_falha_injetada
            ORDER BY tipo_falha_injetada;
        """,
    },
]


def conectar():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
    )


def formatar_valor(v):
    return "" if v is None else str(v)


def executar_consulta(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        colunas = [desc[0] for desc in cur.description]
        linhas = cur.fetchall()
    return colunas, linhas


def escrever_csv(caminho, colunas, linhas):
    with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(colunas)
        for linha in linhas:
            writer.writerow([formatar_valor(v) for v in linha])


def tabela_markdown(colunas, linhas):
    cabecalho = "| " + " | ".join(colunas) + " |"
    separador = "| " + " | ".join("---" for _ in colunas) + " |"
    corpo = [
        "| " + " | ".join(formatar_valor(v) for v in linha) + " |"
        for linha in linhas
    ]
    if not corpo:
        corpo = ["| " + " | ".join("(sem dados)" for _ in colunas) + " |"]
    return "\n".join([cabecalho, separador, *corpo])


def main():
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)

    conn = conectar()
    blocos_md = [
        "# Resultados do experimento comparativo (BASELINE x TRATAMENTO)",
        "",
        "Gerado automaticamente por `scripts/experimento/exportar_resultados.py` "
        "a partir das consultas em `sql/consultas/02_analise_experimento.sql`.",
    ]

    try:
        for consulta in CONSULTAS:
            print(f"Executando: {consulta['titulo']}...")
            colunas, linhas = executar_consulta(conn, consulta["sql"])

            caminho_csv = RESULTADOS_DIR / consulta["arquivo"]
            escrever_csv(caminho_csv, colunas, linhas)
            print(f"  -> {caminho_csv} ({len(linhas)} linha(s))")

            blocos_md.append("")
            blocos_md.append(f"## {consulta['titulo']}")
            blocos_md.append("")
            blocos_md.append(consulta["descricao"])
            blocos_md.append("")
            blocos_md.append(tabela_markdown(colunas, linhas))
    finally:
        conn.close()

    caminho_md = RESULTADOS_DIR / "resultados_consolidado.md"
    with open(caminho_md, "w", encoding="utf-8") as f:
        f.write("\n".join(blocos_md) + "\n")
    print(f"\nMarkdown consolidado: {caminho_md}")


if __name__ == "__main__":
    main()

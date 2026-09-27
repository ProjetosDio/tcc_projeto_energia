"""Runner do experimento comparativo BASELINE x TRATAMENTO.

Roda N iterações de um tipo de falha (ou NENHUMA, como controle),
injetando a falha, disparando a DAG energia_pipeline via API REST do
Airflow, aguardando a conclusão e registrando o resultado em
public.tbl_experimento.

Este script roda no HOST (fora dos containers) e fala com o Postgres
via localhost:5432 e com o Airflow via localhost:8088 — ambos expostos
pelo docker-compose. Quem liga/desliga os mecanismos de autocorreção
(BASELINE vs TRATAMENTO) é a Airflow Variable 'feature_autocorrecao',
setada por rodar_tudo.sh antes de cada grupo — este runner só dispara
a DAG e mede o resultado.

Uso:
    python runner.py --grupo TRATAMENTO --tipo-falha CONEXAO --iteracoes 30
"""
import argparse
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import requests

# --- Conexão com o Postgres (host) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia_db")
DB_USER = os.getenv("DB_USER", "energia_user")
DB_PASS = os.getenv("DB_PASS", "energia_pass")

# --- API do Airflow ---
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8088")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")
DAG_ID = "energia_pipeline"

# Nome exato da Airflow Variable lida por resolver_feature_autocorrecao()
# em docker/airflow/dags/pipeline_energia.py — minúsculo, igual ao criado
# pelo airflow-init no docker-compose.yml. Variables do Airflow são
# case-sensitive: setar com outro casing (ex.: FEATURE_AUTOCORRECAO)
# criaria uma chave à parte que a DAG nunca leria.
AIRFLOW_VAR_FEATURE_AUTOCORRECAO = "feature_autocorrecao"

POLL_TIMEOUT_S = 180
POLL_INTERVAL_S = 3
SLEEP_ENTRE_ITERACOES_S = 3
DURACAO_INJECAO_CONEXAO = int(os.getenv("DURACAO_INJECAO_CONEXAO", "10"))

INJECAO_DIR = Path(__file__).resolve().parents[1] / "injecao_falhas"
SCRIPT_POR_TIPO = {
    "CONEXAO": INJECAO_DIR / "injetar_conexao.py",
    "SCHEMA": INJECAO_DIR / "injetar_schema.py",
    "DADOS_NULOS": INJECAO_DIR / "injetar_dados_nulos.py",
    "ARQUIVO_AUSENTE": INJECAO_DIR / "injetar_arquivo_ausente.py",
}
# CONEXAO se autorrestaura (o próprio script sobe o container de novo no
# fim); os demais alteram arquivo em disco e precisam do subcomando
# 'restaurar' explícito depois que a DAG terminar.
TIPOS_COM_RESTAURACAO_EXPLICITA = {"SCHEMA", "DADOS_NULOS", "ARQUIVO_AUSENTE"}


def executar_sql(query, params=None, fetch=False, tentativas=10, espera_s=2):
    """Abre uma conexão nova por chamada (não reaproveita uma conexão de
    longa duração) e tenta algumas vezes em caso de OperationalError.

    Necessário porque o próprio experimento derruba o Postgres de
    propósito durante a injeção de falha CONEXAO — uma conexão mantida
    aberta pelo runner cairia junto.
    """
    ultimo_erro = None
    for _ in range(tentativas):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    resultado = cur.fetchone() if fetch else None
                conn.commit()
                return resultado
            finally:
                conn.close()
        except psycopg2.OperationalError as e:
            ultimo_erro = e
            time.sleep(espera_s)
    raise RuntimeError(f"Falha ao executar SQL após {tentativas} tentativa(s): {ultimo_erro}")


def registrar_experimento(grupo, tipo_falha, iteracao):
    return executar_sql(
        """
        INSERT INTO public.tbl_experimento (grupo, tipo_falha_injetada, iteracao)
        VALUES (%s, %s, %s)
        RETURNING id_experimento, timestamp_injecao;
        """,
        (grupo, tipo_falha, iteracao),
        fetch=True,
    )


def atualizar_experimento(id_experimento, id_carga_afetada, observacoes):
    executar_sql(
        """
        UPDATE public.tbl_experimento
           SET id_carga_afetada = %s,
               observacoes = %s
         WHERE id_experimento = %s;
        """,
        (id_carga_afetada, observacoes, id_experimento),
    )


def buscar_id_carga_apos(timestamp_injecao):
    row = executar_sql(
        """
        SELECT id_carga FROM public.tbl_controle_carga
         WHERE data_inicio >= %s
         ORDER BY data_inicio DESC
         LIMIT 1;
        """,
        (timestamp_injecao,),
        fetch=True,
    )
    return row[0] if row else None


def aplicar_injecao(tipo_falha):
    """Aplica a falha correspondente.

    Para CONEXAO, dispara em background (não bloqueia): a queda do banco
    precisa se sobrepor ao disparo/execução da DAG, não acontecer antes
    dela. Retorna o Popen para o chamador aguardar depois de disparar a
    DAG. Para os demais tipos, aplica de forma síncrona — o arquivo já
    precisa estar alterado antes de disparar a DAG — e retorna None.
    """
    script = SCRIPT_POR_TIPO[tipo_falha]
    if tipo_falha == "CONEXAO":
        return subprocess.Popen(
            [sys.executable, str(script), "--duracao", str(DURACAO_INJECAO_CONEXAO)]
        )
    subprocess.run([sys.executable, str(script), "aplicar"], check=True)
    return None


def restaurar_injecao(tipo_falha):
    script = SCRIPT_POR_TIPO[tipo_falha]
    subprocess.run([sys.executable, str(script), "restaurar"], check=True)


def set_airflow_variable(nome: str, valor: str):
    """Seta uma Airflow Variable via API REST (PATCH); cria via POST se
    ainda não existir (PATCH em variable inexistente retorna 404).
    Levanta exceção se a chamada não retornar 2xx, para o experimento
    não seguir rodando com a flag errada silenciosamente.
    """
    body = {"key": nome, "value": valor}
    resp = requests.patch(
        f"{AIRFLOW_BASE_URL}/api/v1/variables/{nome}",
        json=body,
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=30,
    )
    if resp.status_code == 404:
        resp = requests.post(
            f"{AIRFLOW_BASE_URL}/api/v1/variables",
            json=body,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=30,
        )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Falha ao setar a Airflow Variable '{nome}'='{valor}': "
            f"HTTP {resp.status_code} - {resp.text}"
        )


def disparar_dag(dag_run_id):
    resp = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        json={"dag_run_id": dag_run_id},
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=30,
    )
    resp.raise_for_status()


def aguardar_conclusao(dag_run_id):
    """Faz polling do status da DAG run até success/failed ou timeout."""
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    inicio = time.monotonic()
    while time.monotonic() - inicio < POLL_TIMEOUT_S:
        resp = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=30)
        resp.raise_for_status()
        state = resp.json().get("state")
        if state in ("success", "failed"):
            return state
        time.sleep(POLL_INTERVAL_S)
    return "timeout"


def rodar_iteracao(grupo, tipo_falha, iteracao):
    id_experimento, timestamp_injecao = registrar_experimento(grupo, tipo_falha, iteracao)
    print(f"  [{iteracao}] id_experimento={id_experimento} tipo_falha={tipo_falha}")

    processo_injecao = None
    if tipo_falha != "NENHUMA":
        processo_injecao = aplicar_injecao(tipo_falha)

    dag_run_id = f"exp_{grupo}_{tipo_falha}_{iteracao:03d}_{uuid.uuid4().hex[:8]}".lower()
    try:
        disparar_dag(dag_run_id)
        state = aguardar_conclusao(dag_run_id)
    except Exception as e:
        print(f"    ERRO ao disparar/acompanhar a DAG: {e}")
        state = "erro_runner"

    # Para CONEXAO, garante que o container já voltou antes de seguir
    # (o script de injeção sobe o container e espera ele estabilizar).
    if processo_injecao is not None:
        processo_injecao.wait()

    if tipo_falha in TIPOS_COM_RESTAURACAO_EXPLICITA:
        try:
            restaurar_injecao(tipo_falha)
        except Exception as e:
            print(f"    ERRO ao restaurar injeção: {e}")

    id_carga = buscar_id_carga_apos(timestamp_injecao)
    atualizar_experimento(id_experimento, id_carga, state)
    print(f"    state={state} id_carga_afetada={id_carga}")

    return state


def main():
    parser = argparse.ArgumentParser(
        description="Runner do experimento comparativo BASELINE x TRATAMENTO."
    )
    parser.add_argument("--grupo", required=True, choices=["BASELINE", "TRATAMENTO"])
    parser.add_argument(
        "--tipo-falha", required=True, dest="tipo_falha",
        choices=["CONEXAO", "SCHEMA", "DADOS_NULOS", "ARQUIVO_AUSENTE", "NENHUMA"],
    )
    parser.add_argument("--iteracoes", type=int, default=30)
    args = parser.parse_args()

    valor_flag = "ON" if args.grupo == "TRATAMENTO" else "OFF"
    set_airflow_variable(AIRFLOW_VAR_FEATURE_AUTOCORRECAO, valor_flag)
    print(f"FEATURE_AUTOCORRECAO setada para {valor_flag}")
    time.sleep(3)

    print(f"=== Iniciando: grupo={args.grupo} tipo_falha={args.tipo_falha} iteracoes={args.iteracoes} ===")

    inicio = time.monotonic()
    estados = []
    for i in range(1, args.iteracoes + 1):
        estado = rodar_iteracao(args.grupo, args.tipo_falha, i)
        estados.append(estado)
        if i < args.iteracoes:
            time.sleep(SLEEP_ENTRE_ITERACOES_S)
    tempo_total = time.monotonic() - inicio

    contagem_por_estado = {}
    for s in estados:
        contagem_por_estado[s] = contagem_por_estado.get(s, 0) + 1

    print("\n=== Resumo ===")
    print(f"Grupo: {args.grupo} | Tipo de falha: {args.tipo_falha}")
    print(f"Iterações executadas: {len(estados)}")
    for estado, qtd in sorted(contagem_por_estado.items(), key=lambda kv: -kv[1]):
        print(f"  {estado}: {qtd}")
    print(f"Tempo total: {tempo_total:.1f}s ({tempo_total / 60:.1f} min)")


if __name__ == "__main__":
    main()

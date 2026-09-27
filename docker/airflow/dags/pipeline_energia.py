from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
import subprocess
import sys

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.append(f"{AIRFLOW_HOME}/scripts")

from db_utils import conectar_com_retry
from verificar_arquivo import verificar_arquivo_entrada

logger = logging.getLogger(__name__)

default_args = {
    "owner": "energia",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="energia_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["energia", "cip"],
) as dag:

    # Definir diretórios base do Airflow
    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    SILVER_SQL_DIR = f"{AIRFLOW_HOME}/sql/silver"
    GOLD_SQL_DIR = f"{AIRFLOW_HOME}/sql/gold"

    # Definir diretório do script de ingestão
    BRONZE_SCRIPT = f"{AIRFLOW_HOME}/scripts/ingestao_bronze.py"
    INPUT_DIR = f"{AIRFLOW_HOME}/data/input"
    ARQUIVO_PRIMARIO = "Amostra_Base_Dados.csv"

    # Código de saída que ingestao_bronze.py usa para falhas não
    # recuperáveis (schema inválido, todas as linhas em quarentena) — ver
    # EXIT_FALHA_NAO_RECUPERAVEL em scripts/ingestao_bronze.py. Falhas de
    # conexão (transitórias) saem com outro código e seguem o retry normal
    # da task, já que tentar de novo pode de fato resolver.
    EXIT_FALHA_NAO_RECUPERAVEL = 2

    def resolver_feature_autocorrecao():
        """Lê a Airflow Variable 'feature_autocorrecao' (ON/OFF, default ON)
        e propaga para os.environ deste processo — de onde os mecanismos de
        autocorreção em db_utils/validador_schema/verificar_arquivo/
        ingestao_bronze leem via os.getenv('FEATURE_AUTOCORRECAO'). Usada
        pelo runner do experimento comparativo (scripts/experimento) para
        alternar entre os grupos BASELINE (OFF) e TRATAMENTO (ON) sem
        precisar reiniciar os containers.
        """
        valor = Variable.get("feature_autocorrecao", default_var="ON")
        os.environ["FEATURE_AUTOCORRECAO"] = valor
        return valor

    def verificar_e_registrar(diretorio: str, arquivo_primario: str, **context):
        """Verifica se o arquivo de entrada existe (ou um fallback por
        padrão de nome) antes de disparar a ingestão. Se nada for
        encontrado, registra a falha em tbl_controle_carga e aborta a DAG.
        """
        resolver_feature_autocorrecao()
        resultado = verificar_arquivo_entrada(diretorio, arquivo_primario)

        if not resultado["encontrado"]:
            resultado_conexao = conectar_com_retry(max_tentativas=3, backoff_base=2)
            if resultado_conexao.conn is not None:
                conn = resultado_conexao.conn
                try:
                    conn.autocommit = True
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO public.tbl_controle_carga (
                            nome_arquivo, data_fim, status, mensagem_erro,
                            tipo_falha, mecanismo_utilizado,
                            resolvido_automaticamente,
                            tentativas_recuperacao, tempo_recuperacao_ms
                        )
                        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            "(ausente)",
                            "ERRO",
                            resultado["mensagem"],
                            "ARQUIVO_AUSENTE",
                            "FALLBACK_ARQUIVO",
                            False,
                            resultado_conexao.tentativas_realizadas,
                            resultado_conexao.tempo_total_ms,
                        ),
                    )
                    cur.close()
                finally:
                    conn.close()
            else:
                logger.error(
                    "Não foi possível conectar ao banco para registrar a "
                    "falha de arquivo ausente."
                )
            raise AirflowException(resultado["mensagem"])

        if resultado.get("usou_fallback"):
            logger.warning(
                f"Arquivo primário ausente; usando fallback: {resultado['caminho']}"
            )

        context["ti"].xcom_push(key="csv_path", value=resultado["caminho"])

    # Verifica a disponibilidade do arquivo de entrada (com fallback por
    # padrão de nome) antes de iniciar a ingestão
    check_input_file = PythonOperator(
        task_id="check_input_file",
        python_callable=verificar_e_registrar,
        op_kwargs={
            "diretorio": INPUT_DIR,
            "arquivo_primario": ARQUIVO_PRIMARIO,
        },
    )

    def run_ingest_bronze(**context):
        """Executa ingestao_bronze.py usando o caminho de CSV resolvido
        pela task check_input_file (arquivo primário ou fallback).
        """
        resolver_feature_autocorrecao()
        csv_path = context["ti"].xcom_pull(task_ids="check_input_file", key="csv_path")
        env = os.environ.copy()
        if csv_path:
            env["CSV_PATH_OVERRIDE"] = csv_path
        try:
            subprocess.run([sys.executable, BRONZE_SCRIPT], check=True, env=env)
        except subprocess.CalledProcessError as e:
            if e.returncode == EXIT_FALHA_NAO_RECUPERAVEL:
                raise AirflowFailException(
                    "Falha não recuperável na ingestão (schema inválido ou "
                    "nenhum registro válido) — tentar de novo não resolve, "
                    "sem retry."
                ) from e
            raise

    # Ingestão de dados para a camada bronze
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_ingest_bronze,
    )

    def run_sql_dir(schema_dir: str, **context):
        """Lê todos os arquivos .sql em ordem e executa usando psycopg2.

        Conecta ao banco Energia com retry e backoff exponencial em caso
        de falha temporária de conexão.
        """
        resolver_feature_autocorrecao()
        if not os.path.exists(schema_dir):
            raise FileNotFoundError(f"Diretório não encontrado: {schema_dir}")

        # Conectar ao banco
        resultado_conexao = conectar_com_retry(max_tentativas=3, backoff_base=2)
        if resultado_conexao.conn is None:
            raise RuntimeError(
                f"Falha ao conectar ao banco Energia após "
                f"{resultado_conexao.tentativas_realizadas} tentativa(s)."
            )
        conn = resultado_conexao.conn
        cursor = conn.cursor()

        try:
            for filename in sorted(os.listdir(schema_dir)):
                if not filename.endswith(".sql"):
                    continue
                path = os.path.join(schema_dir, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        sql = f.read()
                    print(f"Executando SQL via psycopg2: {path}")
                    cursor.execute(sql)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise RuntimeError(f"Falha ao executar {path}: {e}")
        finally:
            cursor.close()
            conn.close()

    #transformação e carga para a camada silver
    load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=run_sql_dir,
        op_kwargs={"schema_dir": SILVER_SQL_DIR},
    )

    #transformação e carga para a camada gold
    load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=run_sql_dir,
        op_kwargs={"schema_dir": GOLD_SQL_DIR},
    )

    check_input_file >> ingest_bronze >> load_silver >> load_gold

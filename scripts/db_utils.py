import os
import time
import logging
import psycopg2
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "energia-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia_db")
DB_USER = os.getenv("DB_USER", "energia_user")
DB_PASS = os.getenv("DB_PASS", "energia_pass")


class ResultadoConexao:
    """Resultado de uma tentativa de conexão com retry.

    conn é None quando todas as tentativas falharam.
    """

    def __init__(self, conn, tentativas, tempo_ms):
        self.conn = conn
        self.tentativas_realizadas = tentativas
        self.tempo_total_ms = tempo_ms


def conectar_com_retry(max_tentativas=3, backoff_base=2):
    """Conecta ao Postgres com retry e backoff exponencial.

    Faz retry apenas em psycopg2.OperationalError (falhas de conexão),
    nunca em erros de SQL. Sempre retorna um ResultadoConexao com o
    número de tentativas e o tempo total decorrido, mesmo quando todas
    as tentativas falham (nesse caso, resultado.conn é None).

    Com FEATURE_AUTOCORRECAO=OFF (usado no grupo BASELINE do experimento
    comparativo), força max_tentativas=1 — equivalente a não ter retry.
    """
    if os.getenv("FEATURE_AUTOCORRECAO", "ON") == "OFF":
        max_tentativas = 1

    tentativas_realizadas = 0
    inicio = time.monotonic()

    def _tentar_conectar():
        nonlocal tentativas_realizadas
        tentativas_realizadas += 1
        logger.info(
            "Tentativa %d/%d de conexão com o banco %s:%s/%s",
            tentativas_realizadas, max_tentativas, DB_HOST, DB_PORT, DB_NAME,
        )
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )

    retrying = Retrying(
        stop=stop_after_attempt(max_tentativas),
        wait=wait_exponential(multiplier=backoff_base),
        retry=retry_if_exception_type(psycopg2.OperationalError),
        reraise=True,
    )

    try:
        conn = retrying(_tentar_conectar)
        tempo_total_ms = int((time.monotonic() - inicio) * 1000)
        logger.info(
            "Conexão estabelecida após %d tentativa(s) em %d ms",
            tentativas_realizadas, tempo_total_ms,
        )
        return ResultadoConexao(conn, tentativas_realizadas, tempo_total_ms)
    except psycopg2.OperationalError as e:
        tempo_total_ms = int((time.monotonic() - inicio) * 1000)
        logger.error(
            "Falha ao conectar após %d tentativa(s) em %d ms: %s",
            tentativas_realizadas, tempo_total_ms, e,
        )
        return ResultadoConexao(None, tentativas_realizadas, tempo_total_ms)

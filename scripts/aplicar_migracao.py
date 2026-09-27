import os
import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "energia-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia_db")
DB_USER = os.getenv("DB_USER", "energia_user")
DB_PASS = os.getenv("DB_PASS", "energia_pass")

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
SQL_PATH = os.path.join(AIRFLOW_HOME, "sql", "init", "03_migracao_autocorrecao.sql")


def main():
    logger.info(f"Lendo script de migração: {SQL_PATH}")
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        logger.info("Executando migração de autocorreção...")
        cur.execute(sql)
        conn.commit()
        logger.info("Migração aplicada com sucesso.")
    except Exception as e:
        conn.rollback()
        logger.error("Erro ao aplicar migração: %s", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

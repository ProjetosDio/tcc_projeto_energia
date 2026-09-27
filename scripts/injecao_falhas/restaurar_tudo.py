from _common import get_logger, restaurar

logger = get_logger(__name__)


def main():
    """Reverte qualquer injeção de falha baseada em arquivo que esteja
    pendente (schema, dados nulos ou arquivo ausente) — todas usam o
    mesmo Amostra_Base_Dados.csv.bkp, então basta restaurar uma vez.
    """
    restaurar(logger)


if __name__ == "__main__":
    main()

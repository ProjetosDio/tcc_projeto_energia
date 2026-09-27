import argparse

from _common import CSV_PATH, BACKUP_PATH, get_logger, fazer_backup, restaurar

logger = get_logger(__name__)


def aplicar():
    fazer_backup(logger)

    # Só o header precisa mudar; o resto do arquivo é copiado tal qual.
    with open(BACKUP_PATH, "r", encoding="utf-8-sig", newline="") as f:
        linhas = f.readlines()

    linhas[0] = linhas[0].replace("VALOR_FATURA", "VALOR_FAT")

    with open(CSV_PATH, "w", encoding="utf-8-sig", newline="") as f:
        f.writelines(linhas)

    logger.info(f"Coluna VALOR_FATURA renomeada para VALOR_FAT em {CSV_PATH}")


def main():
    parser = argparse.ArgumentParser(
        description="Injeta falha de schema renomeando a coluna VALOR_FATURA para VALOR_FAT no CSV de entrada."
    )
    parser.add_argument(
        "comando", nargs="?", choices=["aplicar", "restaurar"], default="aplicar",
    )
    args = parser.parse_args()

    if args.comando == "restaurar":
        restaurar(logger)
    else:
        aplicar()


if __name__ == "__main__":
    main()

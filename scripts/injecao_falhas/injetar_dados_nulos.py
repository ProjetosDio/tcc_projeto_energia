import argparse
import csv
import random

from _common import CSV_PATH, BACKUP_PATH, get_logger, fazer_backup, restaurar

logger = get_logger(__name__)


def aplicar():
    fazer_backup(logger)

    with open(BACKUP_PATH, "r", encoding="utf-8-sig", newline="") as f:
        linhas = list(csv.reader(f, delimiter=";"))

    header = linhas[0]
    dados = linhas[1:]

    idx_valor_fatura = header.index("VALOR_FATURA")
    idx_municipio = header.index("MUNICIPIO")

    random.seed(42)
    total_linhas = len(dados)
    linhas_valor_fatura = random.sample(range(total_linhas), min(10, total_linhas))
    linhas_municipio = random.sample(range(total_linhas), min(5, total_linhas))

    for i in linhas_valor_fatura:
        dados[i][idx_valor_fatura] = ""
    for i in linhas_municipio:
        dados[i][idx_municipio] = ""

    with open(CSV_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(dados)

    logger.info(
        f"{len(linhas_valor_fatura)} linhas com VALOR_FATURA vazio, "
        f"{len(linhas_municipio)} linhas com MUNICIPIO vazio (seed=42)."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Injeta falha de dados nulos: esvazia VALOR_FATURA em 10 linhas "
                    "e MUNICIPIO em 5 linhas, escolhidas aleatoriamente (seed=42)."
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

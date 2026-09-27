import argparse
import shutil

from _common import CSV_PATH, BACKUP_PATH, get_logger, restaurar

logger = get_logger(__name__)

# Cópia deixada para trás com nome que bate no padrão de fallback
# (Amostra*.csv, ver verificar_arquivo.py) — sem ela, o mecanismo de
# fallback nunca tem o que encontrar e TRATAMENTO falha igual ao
# BASELINE, não exercitando a recuperação automática.
FALLBACK_PATH = CSV_PATH.with_name("Amostra_Backup_Fallback.csv")


def aplicar():
    if BACKUP_PATH.exists():
        raise FileExistsError(
            f"Já existe um backup pendente em {BACKUP_PATH}. "
            f"Rode 'restaurar' antes de aplicar uma nova injeção."
        )
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")

    shutil.copy2(CSV_PATH, FALLBACK_PATH)
    logger.info(f"Cópia de fallback criada: {CSV_PATH} -> {FALLBACK_PATH}")

    shutil.move(str(CSV_PATH), str(BACKUP_PATH))
    logger.info(f"Arquivo movido: {CSV_PATH} -> {BACKUP_PATH} (arquivo de entrada agora ausente)")


def restaurar_com_fallback():
    """Remove a cópia de fallback (se existir) e restaura o .bkp para o
    nome primário — desfaz por completo o estado deixado por aplicar().
    """
    if FALLBACK_PATH.exists():
        FALLBACK_PATH.unlink()
        logger.info(f"Cópia de fallback removida: {FALLBACK_PATH}")
    restaurar(logger)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Injeta falha de arquivo ausente: move o CSV primário para .bkp "
            "e deixa uma cópia com nome de fallback (Amostra*.csv) disponível "
            "para o mecanismo de fallback do pipeline."
        )
    )
    parser.add_argument(
        "comando", nargs="?", choices=["aplicar", "restaurar"], default="aplicar",
    )
    args = parser.parse_args()

    if args.comando == "restaurar":
        restaurar_com_fallback()
    else:
        aplicar()


if __name__ == "__main__":
    main()

import logging
import shutil
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Raiz do projeto: scripts/injecao_falhas/_common.py -> scripts -> raiz
BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "data" / "input" / "Amostra_Base_Dados.csv"
BACKUP_PATH = CSV_PATH.with_name(CSV_PATH.name + ".bkp")


def get_logger(name):
    return logging.getLogger(name)


def fazer_backup(logger):
    """Copia o CSV original para o .bkp antes de uma injeção que edita o
    conteúdo do arquivo (schema, dados nulos). Recusa sobrescrever um
    backup já pendente para não perder o arquivo original de verdade.
    """
    if BACKUP_PATH.exists():
        raise FileExistsError(
            f"Já existe um backup pendente em {BACKUP_PATH}. "
            f"Rode 'restaurar' antes de aplicar uma nova injeção."
        )
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Arquivo original não encontrado: {CSV_PATH}")
    shutil.copy2(CSV_PATH, BACKUP_PATH)
    logger.info(f"Backup criado: {CSV_PATH} -> {BACKUP_PATH}")


def restaurar(logger):
    """Restaura o CSV original a partir do .bkp, se existir.

    Retorna True se havia um backup e ele foi restaurado, False se não
    havia nada pendente.
    """
    if not BACKUP_PATH.exists():
        logger.info(f"Nenhum backup pendente em {BACKUP_PATH}. Nada a restaurar.")
        return False
    shutil.move(str(BACKUP_PATH), str(CSV_PATH))
    logger.info(f"Restaurado: {BACKUP_PATH} -> {CSV_PATH}")
    return True

import argparse
import subprocess
import time
from datetime import datetime

from _common import get_logger

logger = get_logger(__name__)

CONTAINER = "energia-postgres"


def log_acao(mensagem):
    logger.info(f"[{datetime.now().isoformat()}] {mensagem}")


def main():
    parser = argparse.ArgumentParser(
        description="Injeta falha de conexão parando o container do Postgres temporariamente."
    )
    parser.add_argument(
        "--duracao", type=int, default=10,
        help="Segundos que o banco fica indisponível (default: 10)",
    )
    args = parser.parse_args()

    log_acao(f"Parando container {CONTAINER}...")
    subprocess.run(["docker", "stop", CONTAINER], check=True)
    log_acao(f"Container {CONTAINER} parado. Aguardando {args.duracao}s...")

    time.sleep(args.duracao)

    log_acao(f"Subindo container {CONTAINER}...")
    subprocess.run(["docker", "start", CONTAINER], check=True)
    log_acao(f"Container {CONTAINER} iniciado. Aguardando 5s para estabilizar...")

    time.sleep(5)

    log_acao("Injeção de falha de conexão concluída.")


if __name__ == "__main__":
    main()

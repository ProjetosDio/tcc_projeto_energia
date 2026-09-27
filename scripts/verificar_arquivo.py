import os
import glob
import logging

logger = logging.getLogger(__name__)


def verificar_arquivo_entrada(diretorio: str, arquivo_primario: str) -> dict:
    """Verifica se o arquivo de entrada primário existe em `diretorio`.

    Se não existir, procura um fallback por padrão de nome ("Amostra*.csv")
    no mesmo diretório e usa o mais recente (por mtime).

    Com FEATURE_AUTOCORRECAO=OFF (grupo BASELINE do experimento
    comparativo), o fallback é desativado: se o arquivo primário não
    existir, retorna encontrado=False direto, sem procurar alternativas.
    """
    caminho_primario = os.path.join(diretorio, arquivo_primario)

    if os.path.exists(caminho_primario):
        return {
            "encontrado": True,
            "caminho": caminho_primario,
            "usou_fallback": False,
        }

    if os.getenv("FEATURE_AUTOCORRECAO", "ON") == "OFF":
        return {
            "encontrado": False,
            "mensagem": (
                f"Arquivo primário '{caminho_primario}' não encontrado "
                f"(fallback desativado: FEATURE_AUTOCORRECAO=OFF)"
            ),
        }

    candidatos = glob.glob(os.path.join(diretorio, "Amostra*.csv"))
    candidatos.sort(key=os.path.getmtime, reverse=True)

    if candidatos:
        caminho_fallback = candidatos[0]
        logger.warning(
            f"Arquivo primário '{caminho_primario}' não encontrado. "
            f"Usando fallback mais recente: '{caminho_fallback}'."
        )
        return {
            "encontrado": True,
            "caminho": caminho_fallback,
            "usou_fallback": True,
            "arquivo_original": arquivo_primario,
        }

    return {
        "encontrado": False,
        "mensagem": f"Nenhum arquivo Amostra*.csv encontrado em {diretorio}",
    }

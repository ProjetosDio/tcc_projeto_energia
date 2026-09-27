import logging
import os
from typing import List, Dict

import pandas as pd

logger = logging.getLogger(__name__)

COLUNAS_ESPERADAS: List[str] = [
    "Ordem",
    "MES_COMPETENCIA",
    "MES_REFERENCIA",
    "NOME",
    "CPF",
    "CNPJ",
    "MUNICIPIO",
    "LOCALIDADE",
    "ENDERECO",
    "NUMERO",
    "COMPLEMENTO",
    "CEP",
    "BAIRRO",
    "CALENDARIO_FABRICA",
    "LONGITUDE",
    "LATITUDE",
    "CLASSE_PRINCIPAL",
    "SUBCLASSE",
    "CATEGORIA_TARIFA",
    "DATA_LIGACAO_CC",
    "STATUS_COMERCIAL",
    "CONTA_CONTRATO",
    "INSTALACAO",
    "ETAPA",
    "GRUPO",
    "CLIENTE_LIVRE",
    "FASE",
    "PERIMETRO",
    "MICRO_GERADOR",
    "IRRIGANTE",
    "MOTIVO_BLOQUEIO_CONTRATO",
    "CONTRATO_ATIVO",
    "MEIO_PAGAMENTO",
    "DOCUMENTO_IMPRESSAO",
    "FATURA",
    "CNR",
    "RECEITA_BANDEIRAS",
    "CONSUMO_FATURADO",
    "CONSUMO_MEDIDO",
    "DATA_VENCIMENTO_ORIGINAL",
    "DATA_PAGAMENTO",
    "INICIO_CALCULO",
    "FIM_CALCULO",
    "QUANTIDADE_DIAS",
    "CANCELAMENTO",
    "REFATURAMENTO",
    "DATA_REFATURAMENTO",
    "TARIFA",
    "PRECO",
    "PIS",
    "COFINS",
    "ICMS",
    "RECEITA_CONSUMO_FATURADO",
    "VALOR_FATURA",
    "VLR_ARRECADADO",
    "SITUACAO",
    "VALOR_FATURA_SUB",
    "CIP",
    "CIP_SUB",
]


def validar_schema_csv(caminho_csv: str) -> Dict:
    """Valida se o header do CSV bate com COLUNAS_ESPERADAS.

    Não levanta exceção: qualquer falha de leitura também é reportada
    como schema inválido no dict retornado.

    Com FEATURE_AUTOCORRECAO=OFF (grupo BASELINE do experimento
    comparativo), a validação é desativada e sempre retorna válido — em
    condições normais o chamador (ingestao_bronze.py) já pula essa
    chamada quando a feature está OFF; este bypass é só uma segunda
    camada de segurança para quem chamar a função diretamente.
    """
    if os.getenv("FEATURE_AUTOCORRECAO", "ON") == "OFF":
        return {
            "valido": True,
            "colunas_faltando": [],
            "colunas_extras": [],
            "mensagem": "Validação de schema desativada (FEATURE_AUTOCORRECAO=OFF)",
        }

    try:
        df_header = pd.read_csv(
            caminho_csv,
            sep=";",
            encoding="utf-8-sig",
            nrows=0,
        )
        colunas_csv = list(df_header.columns)
    except Exception as e:
        logger.error(f"Erro ao ler header do CSV para validação de schema: {e}")
        return {
            "valido": False,
            "colunas_faltando": list(COLUNAS_ESPERADAS),
            "colunas_extras": [],
            "mensagem": f"Não foi possível ler o CSV para validação de schema: {e}",
        }

    colunas_faltando = [c for c in COLUNAS_ESPERADAS if c not in colunas_csv]
    colunas_extras = [c for c in colunas_csv if c not in COLUNAS_ESPERADAS]
    valido = not colunas_faltando and not colunas_extras

    if valido:
        mensagem = f"Schema validado com sucesso: {len(COLUNAS_ESPERADAS)} colunas"
        logger.info(mensagem)
    else:
        mensagem = (
            f"Schema inválido: {len(colunas_faltando)} colunas faltando, "
            f"{len(colunas_extras)} colunas extras. "
            f"Faltando: {colunas_faltando}. Extras: {colunas_extras}"
        )
        logger.info(mensagem)

    return {
        "valido": valido,
        "colunas_faltando": colunas_faltando,
        "colunas_extras": colunas_extras,
        "mensagem": mensagem,
    }

import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from db_utils import conectar_com_retry
from validador_schema import validar_schema_csv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminho do CSV - usar AIRFLOW_HOME para localizar dados
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
CSV_PATH = os.path.join(AIRFLOW_HOME, "data", "input", "Amostra_Base_Dados.csv")

# Ordem de criticidade usada quando mais de um mecanismo de autocorreção
# atua na mesma carga (ex.: fallback de arquivo + quarentena de linhas)
PRIORIDADE_TIPO_FALHA = ["CONEXAO", "ARQUIVO_AUSENTE", "SCHEMA", "DADOS_NULOS", "NENHUMA"]

# Código de saída para falhas não recuperáveis (schema inválido, todas as
# linhas em quarentena): erro determinístico, tentar de novo sem mudar o
# arquivo nunca resolve, então a DAG (run_ingest_bronze) usa esse código
# para falhar a task via AirflowFailException, sem consumir retry.
# Falhas de conexão (transitórias) continuam saindo com código 1, deixando
# o retry padrão do Airflow em jogo.
EXIT_FALHA_NAO_RECUPERAVEL = 2


def montar_tipo_e_mecanismo(candidatos):
    """candidatos: lista de tuplas (tipo_falha, mecanismo) na ordem em que
    os mecanismos de autocorreção foram acionados nesta carga — já em
    ordem de criticidade (CONEXAO > ARQUIVO_AUSENTE > SCHEMA > DADOS_NULOS),
    pois é assim que main() os acrescenta.

    Retorna (tipo_falha, mecanismo_utilizado): tipo_falha é o mais crítico
    entre os que ocorreram; mecanismo_utilizado concatena TODOS os
    mecanismos acionados com '+' (ex.: 'FALLBACK_ARQUIVO+QUARENTENA'), sem
    descartar informação quando mais de um mecanismo atua na mesma carga.
    """
    if not candidatos:
        return "NENHUMA", "NENHUM"
    tipo_falha = candidatos[0][0]
    mecanismo_utilizado = "+".join(mecanismo for _, mecanismo in candidatos)
    return tipo_falha, mecanismo_utilizado


def resolvido(tipo_falha, status):
    """resolvido_automaticamente = TRUE se e somente se houve falha
    (tipo_falha != NENHUMA) E a carga terminou com status = SUCESSO.
    """
    return tipo_falha != "NENHUMA" and status == "SUCESSO"


def _sanitizar_para_json(valor):
    """NaN/Infinity são floats válidos em Python, mas json.dumps os grava
    como os literais NaN/Infinity, que não são JSON válido — o Postgres
    rejeita o cast ::jsonb. Troca esses valores por None antes de serializar.
    """
    if isinstance(valor, float) and (np.isnan(valor) or np.isinf(valor)):
        return None
    return valor


def enviar_para_quarentena(cur, id_carga, linha_origem, motivo_rejeicao, conteudo_linha):
    """Insere uma linha problemática em bronze.tbl_carga_quarentena.

    Usa o mesmo cursor/conexão da carga em andamento (nenhuma conexão
    nova é aberta), num savepoint próprio — se o INSERT falhar (ex.: JSON
    inválido), só ele é revertido, sem afetar o savepoint da linha
    (sp_linha) aberto por quem chamou. Não propaga exceção: a falha é só
    logada para não derrubar o batch.
    """
    conteudo_limpo = {
        chave: _sanitizar_para_json(valor) for chave, valor in conteudo_linha.items()
    }
    try:
        cur.execute("SAVEPOINT sp_quarentena")
        cur.execute(
            """
            INSERT INTO bronze.tbl_carga_quarentena (
                id_carga, linha_origem, motivo_rejeicao, conteudo_linha
            )
            VALUES (%s, %s, %s, %s::jsonb);
            """,
            (
                id_carga,
                linha_origem,
                motivo_rejeicao,
                json.dumps(conteudo_limpo, default=str, ensure_ascii=False),
            ),
        )
        cur.execute("RELEASE SAVEPOINT sp_quarentena")
        return True
    except Exception as quarentena_error:
        logger.error(
            f"Falha ao enviar linha {linha_origem} para quarentena: {quarentena_error}"
        )
        cur.execute("ROLLBACK TO SAVEPOINT sp_quarentena")
        cur.execute("RELEASE SAVEPOINT sp_quarentena")
        return False


def main():

    # Liga/desliga os 4 mecanismos de autocorreção para o experimento
    # comparativo (BASELINE roda com FEATURE_AUTOCORRECAO=OFF). O retry de
    # conexão é controlado dentro de db_utils.conectar_com_retry(), e o
    # fallback de arquivo dentro de verificar_arquivo.verificar_arquivo_entrada();
    # os outros dois (validação de schema e quarentena de dados nulos) são
    # controlados aqui.
    feature_autocorrecao_on = os.getenv("FEATURE_AUTOCORRECAO", "ON") != "OFF"

    # 0. Resolver o caminho efetivo do CSV. Quando a task check_input_file
    # do Airflow não encontra o arquivo primário, ela busca um fallback
    # (Amostra*.csv mais recente) e expõe o caminho via CSV_PATH_OVERRIDE.
    csv_path_override = os.getenv("CSV_PATH_OVERRIDE")
    csv_path_efetivo = csv_path_override if csv_path_override else CSV_PATH
    usou_fallback_arquivo = bool(csv_path_override) and csv_path_override != CSV_PATH

    # 1. Conectar ao Postgres (com retry e backoff exponencial em caso de
    # falha temporária de conexão)
    resultado_conexao = conectar_com_retry(max_tentativas=3, backoff_base=2)

    if resultado_conexao.conn is None:
        # Todas as tentativas de conexão falharam. Sem conexão não é
        # possível registrar a falha em tbl_controle_carga, então
        # tentamos novamente com mais tentativas/timeout maior apenas
        # para conseguir persistir o registro da falha.
        logger.error(
            "Não foi possível conectar ao banco após %d tentativa(s) (%d ms).",
            resultado_conexao.tentativas_realizadas,
            resultado_conexao.tempo_total_ms,
        )
        resultado_log = conectar_com_retry(max_tentativas=6, backoff_base=4)
        if resultado_log.conn is not None:
            try:
                conn_log = resultado_log.conn
                conn_log.autocommit = True
                cur_log = conn_log.cursor()
                cur_log.execute(
                    """
                    INSERT INTO public.tbl_controle_carga (
                        nome_arquivo, data_fim, status, mensagem_erro,
                        tipo_falha, mecanismo_utilizado,
                        resolvido_automaticamente,
                        tentativas_recuperacao, tempo_recuperacao_ms
                    )
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        os.path.basename(csv_path_efetivo),
                        "ERRO",
                        "Falha ao conectar ao banco Postgres após esgotar as tentativas de retry.",
                        "CONEXAO",
                        "RETRY_BACKOFF",
                        False,
                        resultado_conexao.tentativas_realizadas,
                        resultado_conexao.tempo_total_ms,
                    ),
                )
                cur_log.close()
            finally:
                conn_log.close()
        else:
            logger.error(
                "Também não foi possível conectar para registrar a falha "
                "em tbl_controle_carga. Falha não persistida."
            )
        raise RuntimeError(
            f"Falha ao conectar ao banco Postgres após "
            f"{resultado_conexao.tentativas_realizadas} tentativa(s) "
            f"({resultado_conexao.tempo_total_ms} ms)."
        )

    conn = resultado_conexao.conn
    conn.autocommit = False
    cur = conn.cursor()

    conexao_recuperada = resultado_conexao.tentativas_realizadas > 1

    # Mecanismos de autocorreção acionados nesta carga, na ordem de
    # criticidade em que são detectados ao longo do pipeline (CONEXAO >
    # ARQUIVO_AUSENTE > SCHEMA > DADOS_NULOS). Usado para compor tipo_falha
    # (o mais crítico) e mecanismo_utilizado (concatenação de todos).
    candidatos_tipo = []
    if conexao_recuperada:
        candidatos_tipo.append(("CONEXAO", "RETRY_BACKOFF"))
    if usou_fallback_arquivo:
        candidatos_tipo.append(("ARQUIVO_AUSENTE", "FALLBACK_ARQUIVO"))
    tipo_falha, mecanismo_utilizado = montar_tipo_e_mecanismo(candidatos_tipo)

    # 4. Registrar nova carga na tabela de controle. resolvido_automaticamente
    # só é decidido quando a carga efetivamente termina (ver resolvido()) —
    # aqui ela ainda está EM_ANDAMENTO, então começa sempre como FALSE.
    nome_arquivo = os.path.basename(csv_path_efetivo)
    cur.execute(
        """
        INSERT INTO public.tbl_controle_carga (
            nome_arquivo, status,
            tipo_falha, mecanismo_utilizado, resolvido_automaticamente,
            tentativas_recuperacao, tempo_recuperacao_ms
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id_carga;
        """,
        (
            nome_arquivo, "EM_ANDAMENTO",
            tipo_falha, mecanismo_utilizado, False,
            resultado_conexao.tentativas_realizadas,
            resultado_conexao.tempo_total_ms,
        ),
    )
    id_carga = cur.fetchone()[0]
    # Commit isolado: a existência da carga (EM_ANDAMENTO) não pode
    # depender do resultado do processamento abaixo. Sem isso, um
    # conn.rollback() no except final desfaria também este INSERT,
    # apagando o registro da carga por completo em vez de marcá-la ERRO.
    conn.commit()
    logger.info(f"ID da carga criada: {id_carga}")

    # 5. Validar schema do CSV antes de ler o arquivo completo
    # (BASELINE: FEATURE_AUTOCORRECAO=OFF pula essa validação por completo)
    if feature_autocorrecao_on:
        resultado_schema = validar_schema_csv(csv_path_efetivo)
        if not resultado_schema["valido"]:
            candidatos_tipo.append(("SCHEMA", "VALIDACAO_SCHEMA"))
            tipo_falha, mecanismo_utilizado = montar_tipo_e_mecanismo(candidatos_tipo)
            cur.execute(
                """
                UPDATE public.tbl_controle_carga
                   SET data_fim = NOW(),
                       status = %s,
                       tipo_falha = %s,
                       mecanismo_utilizado = %s,
                       resolvido_automaticamente = %s,
                       mensagem_erro = %s
                 WHERE id_carga = %s;
                """,
                (
                    "ERRO", tipo_falha, mecanismo_utilizado,
                    resolvido(tipo_falha, "ERRO"),
                    resultado_schema["mensagem"], id_carga,
                ),
            )
            conn.commit()
            cur.close()
            conn.close()
            sys.exit(EXIT_FALHA_NAO_RECUPERAVEL)

    # 6. Ler CSV com pandas
    logger.info(f"Lendo arquivo: {csv_path_efetivo}")
    df = pd.read_csv(
        csv_path_efetivo,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",   # trata o BOM do início (﻿Ordem)
        dtype=str,              # lê tudo como texto para não ter surpresa
        keep_default_na=False   # não converter strings vazias automaticamente em NaN
    )

    # 7. Conversões de tipos (datas e números)
    # Datas no formato DD/MM/AAAA
    date_cols = [
        "DATA_LIGACAO_CC",
        "DATA_VENCIMENTO_ORIGINAL",
        "DATA_PAGAMENTO",
        "INICIO_CALCULO",
        "FIM_CALCULO",
        "DATA_REFATURAMENTO",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)

    # Números decimais (já estão com vírgula, mas lemos como string; vamos converter para float)
    numeric_cols = [
        "LONGITUDE",
        "LATITUDE",
        "RECEITA_BANDEIRAS",
        "CONSUMO_FATURADO",
        "CONSUMO_MEDIDO",
        "TARIFA",
        "PRECO",
        "PIS",
        "COFINS",
        "ICMS",
        "RECEITA_CONSUMO_FATURADO",
        "VALOR_FATURA",
        "VLR_ARRECADADO",
        "VALOR_FATURA_SUB",
        "CIP",
        "CIP_SUB",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_decimal)

    # Ordem em inteiro
    if "Ordem" in df.columns:
        df["Ordem"] = df["Ordem"].apply(lambda x: int(x) if x.strip() != "" else None)

    # Convertendo numpy.int64 para int para compatibilidade com psycopg2
    df["Ordem"] = df["Ordem"].astype('object')

    # Quantidade de dias em inteiro
    if "QUANTIDADE_DIAS" in df.columns:
        df["QUANTIDADE_DIAS"] = df["QUANTIDADE_DIAS"].apply(
            lambda x: int(x) if x.strip() != "" else None
        )
        # Convertendo numpy.int64 para int para compatibilidade com psycopg2
        df["QUANTIDADE_DIAS"] = df["QUANTIDADE_DIAS"].astype('object')

    try:
        # 8. Limpar tabela bronze.tbl_carga_raw antes de inserir novos dados
        logger.info("Limpando tabela bronze.tbl_carga_raw...")
        cur.execute("TRUNCATE TABLE bronze.tbl_carga_raw;")
        logger.info("Tabela limpa com sucesso.")

        insert_sql = """
            INSERT INTO bronze.tbl_carga_raw (
                ordem,
                mes_competencia,
                mes_referencia,
                nome,
                cpf,
                cnpj,
                municipio,
                localidade,
                endereco,
                numero,
                complemento,
                cep,
                bairro,
                calendario_fabrica,
                longitude,
                latitude,
                classe_principal,
                subclasse,
                categoria_tarifa,
                data_ligacao_cc,
                status_comercial,
                conta_contrato,
                instalacao,
                etapa,
                grupo,
                cliente_livre,
                fase,
                perimetro,
                micro_gerador,
                irrigante,
                motivo_bloqueio_contrato,
                contrato_ativo,
                meio_pagamento,
                documento_impressao,
                fatura,
                cnr,
                receita_bandeiras,
                consumo_faturado,
                consumo_medido,
                data_vencimento_original,
                data_pagamento,
                inicio_calculo,
                fim_calculo,
                quantidade_dias,
                cancelamento,
                refaturamento,
                data_refaturamento,
                tarifa,
                preco,
                pis,
                cofins,
                icms,
                receita_consumo_faturado,
                valor_fatura,
                vlr_arrecadado,
                situacao,
                valor_fatura_sub,
                cip,
                cip_sub,
                carga_id,
                arquivo_origem
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s
            );
        """

        campos_obrigatorios = [
            "MES_COMPETENCIA", "FATURA", "VALOR_FATURA",
            "DATA_VENCIMENTO_ORIGINAL", "MUNICIPIO", "CLASSE_PRINCIPAL",
        ]

        total = 0
        total_quarentena = 0
        for idx, (_, row) in enumerate(df.iterrows(), 1):
            # Savepoint por linha: se o INSERT da linha falhar no Postgres,
            # a transação principal ficaria abortada até um ROLLBACK. O
            # savepoint permite descartar só a linha problemática e seguir
            # o batch normalmente.
            cur.execute("SAVEPOINT sp_linha")
            try:
                # A detecção de campos obrigatórios nulos roda sempre,
                # com ou sem autocorreção — o que muda é a reação:
                # ON  -> quarentena (isola a linha, segue o batch);
                # OFF -> levanta erro (cai no except abaixo, que já
                #        aborta o batch inteiro no BASELINE). Assim os 4
                # mecanismos seguem o mesmo padrão: sempre detectam o
                # problema, só a recuperação automática é que é opcional.
                # Campos numéricos/data (convertidos na etapa 7) viram NaN
                # quando vazios, não None — por isso pd.isna() é necessário
                # além da checagem de None/string vazia.
                campos_nulos = [
                    c for c in campos_obrigatorios
                    if row.get(c) is None
                    or (isinstance(row.get(c), float) and pd.isna(row.get(c)))
                    or str(row.get(c)).strip() == ""
                ]
                if campos_nulos:
                    motivo = f"Campos obrigatórios nulos: {campos_nulos}"
                    if not feature_autocorrecao_on:
                        candidatos_tipo.append(("DADOS_NULOS", "NENHUM"))
                        raise ValueError(motivo)
                    logger.warning(f"Linha {idx} enviada para quarentena: {motivo}")
                    if enviar_para_quarentena(cur, id_carga, idx, motivo, row.to_dict()):
                        cur.execute("RELEASE SAVEPOINT sp_linha")
                    else:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_linha")
                        cur.execute("RELEASE SAVEPOINT sp_linha")
                    total_quarentena += 1
                    continue

                valores = [
                    row.get("Ordem"),
                    row.get("MES_COMPETENCIA"),
                    row.get("MES_REFERENCIA"),
                    row.get("NOME"),
                    row.get("CPF"),
                    row.get("CNPJ"),
                    row.get("MUNICIPIO"),
                    row.get("LOCALIDADE"),
                    row.get("ENDERECO"),
                    row.get("NUMERO"),
                    row.get("COMPLEMENTO"),
                    row.get("CEP"),
                    row.get("BAIRRO"),
                    row.get("CALENDARIO_FABRICA"),
                    row.get("LONGITUDE"),
                    row.get("LATITUDE"),
                    row.get("CLASSE_PRINCIPAL"),
                    row.get("SUBCLASSE"),
                    row.get("CATEGORIA_TARIFA"),
                    row.get("DATA_LIGACAO_CC"),
                    row.get("STATUS_COMERCIAL"),
                    row.get("CONTA_CONTRATO"),
                    row.get("INSTALACAO"),
                    row.get("ETAPA"),
                    row.get("GRUPO"),
                    row.get("CLIENTE_LIVRE"),
                    row.get("FASE"),
                    row.get("PERIMETRO"),
                    row.get("MICRO_GERADOR"),
                    row.get("IRRIGANTE"),
                    row.get("MOTIVO_BLOQUEIO_CONTRATO"),
                    row.get("CONTRATO_ATIVO"),
                    row.get("MEIO_PAGAMENTO"),
                    row.get("DOCUMENTO_IMPRESSAO"),
                    row.get("FATURA"),
                    row.get("CNR"),
                    row.get("RECEITA_BANDEIRAS"),
                    row.get("CONSUMO_FATURADO"),
                    row.get("CONSUMO_MEDIDO"),
                    row.get("DATA_VENCIMENTO_ORIGINAL"),
                    row.get("DATA_PAGAMENTO"),
                    row.get("INICIO_CALCULO"),
                    row.get("FIM_CALCULO"),
                    row.get("QUANTIDADE_DIAS"),
                    row.get("CANCELAMENTO"),
                    row.get("REFATURAMENTO"),
                    row.get("DATA_REFATURAMENTO"),
                    row.get("TARIFA"),
                    row.get("PRECO"),
                    row.get("PIS"),
                    row.get("COFINS"),
                    row.get("ICMS"),
                    row.get("RECEITA_CONSUMO_FATURADO"),
                    row.get("VALOR_FATURA"),
                    row.get("VLR_ARRECADADO"),
                    row.get("SITUACAO"),
                    row.get("VALOR_FATURA_SUB"),
                    row.get("CIP"),
                    row.get("CIP_SUB"),
                    id_carga,
                    nome_arquivo,
                ]
                # Converter valores numpy para Python types
                valores_limpos = []
                for v in valores:
                    if isinstance(v, (np.integer,)):
                        valores_limpos.append(int(v))
                    elif isinstance(v, (np.floating,)) and np.isnan(v):
                        valores_limpos.append(None)
                    elif isinstance(v, float) and np.isnan(v):
                        valores_limpos.append(None)
                    else:
                        valores_limpos.append(v)
                valores = valores_limpos
                cur.execute(insert_sql, valores)
                cur.execute("RELEASE SAVEPOINT sp_linha")
                total += 1
            except Exception as row_error:
                logger.error(f"Erro na linha {idx}: {row_error}")
                # Debug: imprimir os valores da linha problemática
                if idx == 268 and "valores" in locals():
                    logger.debug("Valores da linha:")
                    for i, val in enumerate(valores):
                        logger.debug(f"  {i}: {type(val).__name__} = {repr(val)}")

                cur.execute("ROLLBACK TO SAVEPOINT sp_linha")

                if not feature_autocorrecao_on:
                    # BASELINE: sem quarentena — uma linha problemática
                    # aborta o batch inteiro (comportamento original).
                    raise

                if enviar_para_quarentena(cur, id_carga, idx, str(row_error), row.to_dict()):
                    cur.execute("RELEASE SAVEPOINT sp_linha")
                else:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_linha")
                    cur.execute("RELEASE SAVEPOINT sp_linha")
                total_quarentena += 1
                continue

        # 9. Inserir os mesmos dados na tabela histórica bronze.tbl_carga_raw_hist
        logger.info("Inserindo dados na tabela histórica bronze.tbl_carga_raw_hist...")
        cur.execute("""
            INSERT INTO bronze.tbl_carga_raw_hist 
            SELECT * FROM bronze.tbl_carga_raw;
        """)
        logger.info(f"Dados copiados para histórico: {total} registros")
        if total_quarentena > 0:
            logger.info(f"Linhas enviadas para quarentena: {total_quarentena}")

        # Atualiza controle de carga como sucesso (ou sucesso parcial, se
        # houve linhas enviadas para quarentena)
        if total_quarentena > 0:
            candidatos_tipo.append(("DADOS_NULOS", "QUARENTENA"))
            tipo_falha, mecanismo_utilizado = montar_tipo_e_mecanismo(candidatos_tipo)
            # Houve recuperação parcial (registros bons entraram, os
            # problemáticos foram isolados em quarentena) só se total > 0;
            # se nada entrou, a falha não foi recuperada.
            status_final = "SUCESSO" if total > 0 else "ERRO"

            cur.execute(
                """
                UPDATE public.tbl_controle_carga
                   SET data_fim = NOW(),
                       qtd_registros = %s,
                       qtd_registros_quarentena = %s,
                       status = %s,
                       tipo_falha = %s,
                       mecanismo_utilizado = %s,
                       resolvido_automaticamente = %s
                 WHERE id_carga = %s;
                """,
                (
                    total, total_quarentena, status_final,
                    tipo_falha, mecanismo_utilizado,
                    resolvido(tipo_falha, status_final),
                    id_carga,
                ),
            )
        else:
            status_final = "SUCESSO"
            cur.execute(
                """
                UPDATE public.tbl_controle_carga
                   SET data_fim = NOW(),
                       qtd_registros = %s,
                       status = %s,
                       tipo_falha = %s,
                       mecanismo_utilizado = %s,
                       resolvido_automaticamente = %s
                 WHERE id_carga = %s;
                """,
                (
                    total, status_final, tipo_falha, mecanismo_utilizado,
                    resolvido(tipo_falha, status_final),
                    id_carga,
                ),
            )

        conn.commit()

        if status_final == "ERRO":
            logger.error("Carga finalizada como ERRO: nenhum registro válido inserido.")
            sys.exit(EXIT_FALHA_NAO_RECUPERAVEL)

        logger.info(f"Carga concluída com sucesso. Registros inseridos: {total}")

    except Exception as e:
        conn.rollback()
        logger.error("Erro durante a carga: %s", e)
        logger.error(f"Linha atual processada: {total + 1}")


        import traceback
        logger.exception("Traceback completo:")
        # Atualiza controle de carga como ERRO. Recalcula tipo_falha /
        # mecanismo_utilizado a partir dos candidatos já detectados (ex.:
        # conexão recuperada antes de uma falha catastrófica posterior) e
        # força resolvido_automaticamente=FALSE, já que o status é ERRO.
        tipo_falha, mecanismo_utilizado = montar_tipo_e_mecanismo(candidatos_tipo)
        cur.execute(
            """
            UPDATE public.tbl_controle_carga
               SET data_fim = NOW(),
                   status = %s,
                   tipo_falha = %s,
                   mecanismo_utilizado = %s,
                   resolvido_automaticamente = %s,
                   mensagem_erro = %s
             WHERE id_carga = %s;
            """,
            (
                "ERRO", tipo_falha, mecanismo_utilizado,
                resolvido(tipo_falha, "ERRO"),
                str(e), id_carga,
            ),
        )
        conn.commit()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def convert_date(value: str):
    """Converte 'DD/MM/AAAA' para objeto date ou retorna None."""
    if not value or str(value).strip() == "":
        return None
    try:
        # Alguns campos podem vir como '20230808' (yyyymmdd). Tratamos os dois casos.
        v = str(value).strip()
        if "/" in v:
            return datetime.strptime(v, "%d/%m/%Y").date()
        if len(v) == 8 and v.isdigit():
            return datetime.strptime(v, "%Y%m%d").date()
        return None
    except Exception:
        return None


def convert_decimal(value: str):
    """Converte string com vírgula decimal para float ou retorna None."""
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    try:
        # troca vírgula por ponto e converte
        # Se há vírgula como decimal, substitui por ponto
        if "," in v:
            v = v.replace(".", "")  # Remove ponto de separador de milhares
            v = v.replace(",", ".")  # Substitui vírgula decimal por ponto
        return float(v)
    except Exception:
        return None


if __name__ == "__main__":
    main()

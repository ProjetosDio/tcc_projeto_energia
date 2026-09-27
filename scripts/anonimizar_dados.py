"""Anonimiza o CSV de entrada do pipeline (Amostra_Base_Dados.csv).

Substitui por valores genéricos as colunas que funcionam como referência
geográfica ou identificador, preservando tudo de que o pipeline e o
experimento dependem:

  - as mesmas linhas, na mesma ordem, e o mesmo cabeçalho (59 colunas);
  - o formato do arquivo (BOM UTF-8, separador ';', quebra de linha e
    ausência de newline no final, iguais aos do original);
  - os campos obrigatórios da ingestão e todos os valores, datas e
    categorias (colunas não listadas abaixo não são tocadas);
  - a estrutura de igualdade usada pelas junções da camada silver: os
    mapeamentos são injetivos, então linhas que compartilhavam (ou não)
    a mesma tupla (ENDERECO, NUMERO, COMPLEMENTO, CEP), o mesmo BAIRRO ou
    a mesma INSTALACAO continuam compartilhando (ou não) após a máscara.

Regras por coluna:

  ENDERECO             'RUA FICTICIA 001', 'RUA FICTICIA 002', ... por
                       ordem de primeira aparição (mesmo original ->
                       mesmo fictício).
  COMPLEMENTO          'COMPLEMENTO 001', ... pela mesma regra; valores em
                       branco continuam em branco.
  NUMERO               'S/N' é mantido; cada número distinto vira um
                       número aleatório distinto (seed fixa), escolhido
                       fora do conjunto de números originais.
  BAIRRO               'BAIRRO 001', ... por ordem de primeira aparição.
  CEP                  CEP genérico do município: prefixo de 5 dígitos
                       mais frequente entre as linhas do município (empate
                       -> menor prefixo) + '-000'.
  LATITUDE, LONGITUDE  arredondadas para 2 casas decimais (ROUND_HALF_UP),
                       mantendo o formato original (vírgula decimal, 12
                       casas).
  INSTALACAO           sequencial por linha, 10 dígitos: '0000000001', ...
  CONTA_CONTRATO       12 dígitos, preservando a relação com a instalação:
                       quando no original CONTA_CONTRATO == '00' +
                       INSTALACAO, continua valendo; nas demais linhas,
                       sequencial '0030' + 8 dígitos.
  FATURA               '0' + MES_COMPETENCIA + sequencial de 9 dígitos
                       (16 dígitos, mesmo padrão do original).
  DOCUMENTO_IMPRESSAO  '3' + sequencial de 11 dígitos (12 dígitos).

O script é determinístico: a mesma entrada sempre gera a mesma saída.

Uso:
    python scripts/anonimizar_dados.py <entrada.csv> <saida.csv>

Entrada e saída podem ser o mesmo arquivo (a entrada é lida por completo
antes da escrita).
"""
import argparse
import random
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP

SEED = 20260926
SEPARADOR = ";"
BOM = "﻿"

COLUNAS_MASCARADAS = [
    "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO",
    "LATITUDE", "LONGITUDE",
    "CONTA_CONTRATO", "INSTALACAO", "DOCUMENTO_IMPRESSAO", "FATURA",
]


def ler_csv(caminho):
    with open(caminho, "rb") as f:
        bruto = f.read()
    texto = bruto.decode("utf-8")
    tem_bom = texto.startswith(BOM)
    if tem_bom:
        texto = texto[len(BOM):]
    quebra = "\r\n" if "\r\n" in texto else "\n"
    newline_final = texto.endswith(quebra)
    linhas = texto.split(quebra)
    if newline_final:
        linhas = linhas[:-1]

    # O arquivo não usa aspas; split simples só é seguro nesse caso.
    if '"' in texto:
        raise ValueError("O CSV contém aspas; este script não trata campos entre aspas.")

    cabecalho = linhas[0].split(SEPARADOR)
    registros = [linha.split(SEPARADOR) for linha in linhas[1:]]
    for i, campos in enumerate(registros, 2):
        if len(campos) != len(cabecalho):
            raise ValueError(f"Linha {i}: {len(campos)} campos, esperado {len(cabecalho)}.")
    formato = {"bom": tem_bom, "quebra": quebra, "newline_final": newline_final}
    return cabecalho, registros, formato


def escrever_csv(caminho, cabecalho, registros, formato):
    linhas = [SEPARADOR.join(cabecalho)] + [SEPARADOR.join(r) for r in registros]
    texto = formato["quebra"].join(linhas)
    if formato["newline_final"]:
        texto += formato["quebra"]
    if formato["bom"]:
        texto = BOM + texto
    with open(caminho, "wb") as f:
        f.write(texto.encode("utf-8"))


def mapa_sequencial(valores, prefixo, largura, manter=lambda v: False):
    """Mesmo valor original -> mesmo rótulo, numerado por primeira aparição."""
    mapa = {}
    for v in valores:
        if manter(v) or v in mapa:
            continue
        mapa[v] = f"{prefixo}{len(mapa) + 1:0{largura}d}"
    return mapa


def mapa_numeros(valores):
    """Cada número distinto -> número aleatório distinto, fora dos originais."""
    originais = sorted({v for v in valores if v.isdigit()}, key=int)
    proibidos = {int(v) for v in originais}
    candidatos = [n for n in range(1, 10000) if n not in proibidos]
    sorteados = random.Random(SEED).sample(candidatos, len(originais))
    return {orig: str(novo) for orig, novo in zip(originais, sorteados)}


def cep_por_municipio(registros, i_municipio, i_cep):
    prefixos = defaultdict(Counter)
    for r in registros:
        prefixos[r[i_municipio]][r[i_cep][:5]] += 1
    return {
        municipio: sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] + "-000"
        for municipio, c in prefixos.items()
    }


def arredondar_coordenada(valor):
    """'-5,123456000000' -> '-5,120000000000' (2 casas, mesma largura decimal)."""
    inteiro, decimais = valor.split(",")
    numero = Decimal(f"{inteiro}.{decimais}").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    texto = f"{numero:.{len(decimais)}f}"
    return texto.replace(".", ",")


def anonimizar(cabecalho, registros):
    idx = {nome: cabecalho.index(nome) for nome in cabecalho}
    faltando = [c for c in COLUNAS_MASCARADAS + ["MUNICIPIO", "MES_COMPETENCIA"] if c not in idx]
    if faltando:
        raise ValueError(f"Colunas ausentes no CSV: {faltando}")

    coluna = lambda nome: [r[idx[nome]] for r in registros]
    em_branco = lambda v: not v.strip()

    m_endereco = mapa_sequencial(coluna("ENDERECO"), "RUA FICTICIA ", 3)
    m_complemento = mapa_sequencial(coluna("COMPLEMENTO"), "COMPLEMENTO ", 3, manter=em_branco)
    m_bairro = mapa_sequencial(coluna("BAIRRO"), "BAIRRO ", 3)
    m_numero = mapa_numeros(coluna("NUMERO"))
    m_cep = cep_por_municipio(registros, idx["MUNICIPIO"], idx["CEP"])

    saida = []
    contas_independentes = 0
    for seq, r in enumerate(registros, 1):
        novo = list(r)
        novo[idx["ENDERECO"]] = m_endereco[r[idx["ENDERECO"]]]
        novo[idx["COMPLEMENTO"]] = m_complemento.get(r[idx["COMPLEMENTO"]], r[idx["COMPLEMENTO"]])
        novo[idx["BAIRRO"]] = m_bairro[r[idx["BAIRRO"]]]
        novo[idx["NUMERO"]] = m_numero.get(r[idx["NUMERO"]], r[idx["NUMERO"]])
        novo[idx["CEP"]] = m_cep[r[idx["MUNICIPIO"]]]
        for c in ("LATITUDE", "LONGITUDE"):
            if r[idx[c]].strip():
                novo[idx[c]] = arredondar_coordenada(r[idx[c]])

        instalacao = f"{seq:010d}"
        novo[idx["INSTALACAO"]] = instalacao
        if r[idx["CONTA_CONTRATO"]] == "00" + r[idx["INSTALACAO"]]:
            novo[idx["CONTA_CONTRATO"]] = "00" + instalacao
        else:
            contas_independentes += 1
            novo[idx["CONTA_CONTRATO"]] = f"0030{contas_independentes:08d}"
        novo[idx["FATURA"]] = f"0{r[idx['MES_COMPETENCIA']]}{seq:09d}"
        novo[idx["DOCUMENTO_IMPRESSAO"]] = f"3{seq:011d}"
        saida.append(novo)
    return saida


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("entrada", help="CSV original")
    parser.add_argument("saida", help="CSV anonimizado (pode ser o mesmo caminho da entrada)")
    args = parser.parse_args()

    cabecalho, registros, formato = ler_csv(args.entrada)
    anonimizado = anonimizar(cabecalho, registros)
    escrever_csv(args.saida, cabecalho, anonimizado, formato)
    print(f"{len(anonimizado)} linhas anonimizadas: {args.entrada} -> {args.saida}")
    print(f"Colunas mascaradas: {', '.join(COLUNAS_MASCARADAS)}")


if __name__ == "__main__":
    main()

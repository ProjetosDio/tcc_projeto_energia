#!/usr/bin/env bash
# Executa a matriz completa do experimento comparativo:
#   2 grupos (BASELINE, TRATAMENTO) x 4 tipos de falha x N iterações.
#
# Roda no HOST (fora dos containers). Requer:
#   - docker compose (serviços já de pé: energia-postgres, airflow-*)
#   - python com psycopg2 e requests instalados (ver requirements.txt)
#
# Uso:
#   ./rodar_tudo.sh [iteracoes_por_combinacao]   # default: 30
#
# Total esperado com o default: 2 grupos x 4 tipos x 30 = 240 execuções,
# ~2-3 horas (a maior parte do tempo é o polling da DAG + sleeps entre
# iterações; ver observação sobre timeout no final deste arquivo).

set -euo pipefail

ITERACOES="${1:-30}"
TIPOS_FALHA=("CONEXAO" "SCHEMA" "DADOS_NULOS" "ARQUIVO_AUSENTE")

# Nome do container do scheduler, usado para setar a Airflow Variable via
# CLI (mais simples que autenticar na API só para isso).
AIRFLOW_CONTAINER="${AIRFLOW_CONTAINER:-projeto_tcc_energia-airflow-scheduler-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INJECAO_DIR="$SCRIPT_DIR/../injecao_falhas"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

restaurar_pendencias() {
    log "Restaurando qualquer injeção de arquivo pendente..."
    python "$INJECAO_DIR/restaurar_tudo.py"
}

definir_feature_autocorrecao() {
    local valor="$1"
    log "Definindo Airflow Variable feature_autocorrecao=$valor"
    docker exec "$AIRFLOW_CONTAINER" airflow variables set feature_autocorrecao "$valor" >/dev/null
}

rodar_grupo() {
    local grupo="$1"
    local feature="$2"

    restaurar_pendencias
    definir_feature_autocorrecao "$feature"

    log "===== Iniciando grupo $grupo (FEATURE_AUTOCORRECAO=$feature) ====="
    local inicio_grupo
    inicio_grupo=$(date +%s)

    for tipo in "${TIPOS_FALHA[@]}"; do
        log "--- $grupo / $tipo ($ITERACOES iterações) ---"
        python "$SCRIPT_DIR/runner.py" \
            --grupo "$grupo" \
            --tipo-falha "$tipo" \
            --iteracoes "$ITERACOES"
    done

    local fim_grupo
    fim_grupo=$(date +%s)
    log "===== Grupo $grupo concluído em $(( (fim_grupo - inicio_grupo) / 60 )) min ====="
}

log "===== Início da matriz completa do experimento ====="
inicio_total=$(date +%s)

rodar_grupo "BASELINE" "OFF"
rodar_grupo "TRATAMENTO" "ON"

restaurar_pendencias
definir_feature_autocorrecao "ON"   # deixa o ambiente no estado padrão

fim_total=$(date +%s)
log "===== Matriz completa concluída em $(( (fim_total - inicio_total) / 60 )) min ====="
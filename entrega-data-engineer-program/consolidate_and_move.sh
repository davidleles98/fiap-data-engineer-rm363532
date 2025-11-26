#!/bin/bash

# --- Configurações de Download ---
API_URL_PAGAMENTOS="https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos"
API_URL_PEDIDOS="https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos"

# --- Configurações de Diretório e Saída ---
DIR_PAGAMENTOS="pagamentos_raw"
DIR_PEDIDOS="pedidos_raw"

# Arquivos intermediários consolidados (sem compactação)
TEMP_PAGAMENTOS="pagamentos_consolidado.json"
TEMP_PEDIDOS="pedidos_consolidado.csv"

# Diretório de destino final
OUTPUT_DIR_PAGAMENTOS="data/input/pagamentos"
OUTPUT_DIR_PEDIDOS="data/input/pedidos"

# --- Função para baixar e consolidar arquivos ---
download_and_consolidate() {
    local api_url=$1
    local temp_dir=$2
    local output_file=$3
    local file_type=$4 # json ou csv

    echo "Iniciando processamento para: $temp_dir"

    # 1. Criar diretório temporário
    mkdir -p "$temp_dir"

    # 2. Listar arquivos e baixar usando a API do GitHub
    echo "Baixando arquivos de $api_url..."
    
    # Usa curl para chamar a API, jq para extrair as URLs de download e wget para baixar
    curl -s "$api_url" | \
    jq -r '.[] | select(.name | endswith(".gz")) | .download_url' | \
    while read url; do
        wget -q -P "$temp_dir" "$url"
    done

    if [ ! "$(ls -A $temp_dir)" ]; then
        echo "ERRO: Nenhum arquivo foi baixado. Verifique a URL da API ou a conexão."
        return 1
    fi

    echo "Download concluído. Consolidando arquivos..."

    # 3. Consolidar arquivos (ordenados por nome)
    find "$temp_dir" -name "*.${file_type}.gz" -print0 | sort -z | xargs -0 -I {} sh -c "gunzip -c {} >> \"$output_file\""

    echo "Consolidação concluída. Arquivo salvo em: $output_file"
    
    # 4. Limpar diretório temporário
    rm -rf "$temp_dir"
}

# --- Execução Principal ---

# Instalar 'jq' se não estiver presente
if ! command -v jq &> /dev/null
then
    echo "O utilitário 'jq' não está instalado. Instalando..."
    sudo apt-get update -qq && sudo apt-get install -y jq
fi

# 1. Processar Pagamentos (JSON)
download_and_consolidate "$API_URL_PAGAMENTOS" "$DIR_PAGAMENTOS" "$TEMP_PAGAMENTOS" "json"

# 2. Processar Pedidos (CSV)
download_and_consolidate "$API_URL_PEDIDOS" "$DIR_PEDIDOS" "$TEMP_PEDIDOS" "csv"

# 3. Compactar e Mover para o Diretório de Destino
echo -e "\nCompactando e movendo arquivos para o diretório de destino: $OUTPUT_DIR_PAGAMENTOS E $OUTPUT_DIR_PEDIDOS"

# Cria a estrutura de pastas se não existir
mkdir -p "$OUTPUT_DIR_PAGAMENTOS"
mkdir -p "$OUTPUT_DIR_PEDIDOS"

# Compacta o JSON e move para o destino com o nome correto
echo "Compactando $TEMP_PAGAMENTOS para $OUTPUT_DIR_PAGAMENTOS/pagamentos.gz"
gzip -c "$TEMP_PAGAMENTOS" > "$OUTPUT_DIR_PAGAMENTOS/pagamentos.gz"
rm "$TEMP_PAGAMENTOS"

# Compacta o CSV e move para o destino com o nome correto
echo "Compactando $TEMP_PEDIDOS para $OUTPUT_DIR_PEDIDOS/pedidos.gz"
gzip -c "$TEMP_PEDIDOS" > "$OUTPUT_DIR_PEDIDOS/pedidos.gz"
rm "$TEMP_PEDIDOS"

echo -e "\nProcesso concluído."
echo "Arquivos consolidados e compactados estão em: $OUTPUT_DIR_PAGAMENTOS E $OUTPUT_DIR_PEDIDOS"
du -h "$OUTPUT_DIR_PAGAMENTOS/pagamentos.gz" "$OUTPUT_DIR_PEDIDOS/pedidos.gz"

#!/bin/bash

# Configuração
DB_USER="bot"
DB_NAME="market"
DB_PASS="botpass"
DB_HOST="localhost"

# Função para checar tabela
check_table() {
    local TABLE_NAME=$1
    echo -e "\nÚltimos dados em '${TABLE_NAME}':"
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
        SELECT symbol, interval, COUNT(*)
        FROM ${TABLE_NAME}
        GROUP BY 1,2
        ORDER BY 3 DESC
        LIMIT 5;
    " 2>/dev/null
}

# Executar consultas
check_table "features"
check_table "md_candles"

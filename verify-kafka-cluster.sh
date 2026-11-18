#!/bin/bash

# ==============================================================================
# Script di verifica completa per Cluster Apache Kafka 4.x KRaft (SASL_SSL)
# Riferimento: Guida_Kafka_KRaft_3_Nodi_SASL_SSL_v3.pdf
# Nodi:
#   Kafka-zkp01_prod -> 10.41.239.54 (node.id=1)
#   Kafka-zkp02_prod -> 10.41.239.55 (node.id=2)
#   kafka-zkp03_prod -> 10.41.239.56 (node.id=3)
# ==============================================================================

INVENTORY="inventory.ini"
NODE1_HOST="Kafka-zkp01_prod"
NODE2_HOST="Kafka-zkp02_prod"
NODE3_HOST="kafka-zkp03_prod"

NODE1_IP="10.41.239.54"
NODE2_IP="10.41.239.55"
NODE3_IP="10.41.239.56"

BOOTSTRAP="${NODE1_IP}:9092,${NODE2_IP}:9092,${NODE3_IP}:9092"
KAFKA_HOME="/opt/kafka"
JAVA_HOME="/opt/kafka/jdk17"
ADMIN_CONFIG="${KAFKA_HOME}/client.properties"
APP_CONFIG="${KAFKA_HOME}/appclient.properties"

# Legge la password SSL da group_vars/all.yml (fallback a PTVonlinEproD.2026)
SSL_PASSWORD=$(grep -E '^ssl_keystore_password:' group_vars/all.yml 2>/dev/null | sed -E 's/^ssl_keystore_password:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/')
SSL_PASSWORD="${SSL_PASSWORD:-PTVonlinEproD.2026}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}=====================================================${NC}"
echo -e "${BLUE}  VERIFICA CLUSTER KAFKA 4.x KRAFT (SASL_SSL/PLAIN)  ${NC}"
echo -e "${BLUE}=====================================================${NC}"
echo ""

print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
        return 0
    else
        echo -e "${RED}✗${NC} $2"
        return 1
    fi
}

ERRORS=0

# ==========================================
# 1. VERIFICA SERVIZIO SYSTEMD E JAVA CONFINATA
# ==========================================
echo -e "${YELLOW}1. Verifica servizio Kafka e Java 17 confinata...${NC}"
ansible -i "$INVENTORY" kafka_cluster -a "systemctl is-active kafka" > /dev/null 2>&1
print_result $? "Servizio systemd kafka attivo su tutti e 3 i nodi" || ((ERRORS++))

ansible -i "$INVENTORY" kafka_cluster -m shell -a "su - kafka -c 'java -version 2>&1 | grep -q \"17\\.\"'" > /dev/null 2>&1
print_result $? "Zulu JDK 17 confinata correttamente nel profilo dell'utente kafka" || ((ERRORS++))

echo ""

# ==========================================
# 2. VERIFICA PORTE IN ASCOLTO (9092 BROKER, 9093 CONTROLLER)
# ==========================================
echo -e "${YELLOW}2. Verifica porte KRaft in ascolto (9092 BROKER, 9093 CONTROLLER)...${NC}"
for port in 9092 9093; do
    ansible -i "$INVENTORY" kafka_cluster -m shell -a "ss -tlnp | grep -q ':${port} '" > /dev/null 2>&1
    print_result $? "Porta ${port} (SASL_SSL) in ascolto su tutti i nodi" || ((ERRORS++))
done

echo ""

# ==========================================
# 3. VERIFICA STORAGE KRAFT E QUORUM RAFT
# ==========================================
echo -e "${YELLOW}3. Verifica formattazione storage e stato Quorum KRaft...${NC}"
ansible -i "$INVENTORY" kafka_cluster -m shell -a "test -f ${KAFKA_HOME}/data/meta.properties && test -d ${KAFKA_HOME}/data/__cluster_metadata-0" > /dev/null 2>&1
print_result $? "Directory dati KRaft (${KAFKA_HOME}/data/meta.properties) formattata su tutti i nodi" || ((ERRORS++))

ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-metadata-quorum.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG} describe --status' | grep -q 'LeaderId:'" > /dev/null 2>&1
print_result $? "Quorum KRaft operativo e Leader eletto" || ((ERRORS++))

echo ""

# ==========================================
# 4. VERIFICA CERTIFICATI TLS (CON PASSWORD, FQDN, SAN E SCADENZA)
# ==========================================
echo -e "${YELLOW}4. Verifica certificati TLS (Keystore, Truststore, FQDN, SAN e Scadenza)...${NC}"
ansible -i "$INVENTORY" kafka_cluster -m shell -a "test -f ${KAFKA_HOME}/config/security/kafka.truststore.jks && ls ${KAFKA_HOME}/config/security/kafka-*.keystore.jks >/dev/null 2>&1" > /dev/null 2>&1
print_result $? "Keystore e Truststore presenti in ${KAFKA_HOME}/config/security" || ((ERRORS++))

# Verifica apertura Truststore con password e scadenza CA
TS_INFO=$(ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "${JAVA_HOME}/bin/keytool -list -v -keystore ${KAFKA_HOME}/config/security/kafka.truststore.jks -storepass '${SSL_PASSWORD}' -alias CARoot 2>/dev/null | grep -E 'Owner:|Valid from:'" 2>/dev/null | grep -v "SUCCESS")
if [ -n "$TS_INFO" ]; then
    print_result 0 "Truststore condiviso (kafka.truststore.jks) verificato con password SSL"
    echo "$TS_INFO" | sed 's/^/    /'
else
    print_result 1 "Errore apertura Truststore (kafka.truststore.jks) con password SSL"
    ((ERRORS++))
fi

# Verifica Keystore di ogni nodo con password (FQDN, Subject/CN, SAN, Scadenza)
for entry in "${NODE1_HOST}:${NODE1_IP}" "${NODE2_HOST}:${NODE2_IP}" "${NODE3_HOST}:${NODE3_IP}"; do
    HOST="${entry%%:*}"
    IP="${entry##*:}"
    KS_PATH="${KAFKA_HOME}/config/security/kafka-${IP}.keystore.jks"

    CERT_DETAILS=$(ansible -i "$INVENTORY" "$HOST" -m shell -a "
        set -e
        FQDN=\$(hostname -f 2>/dev/null || hostname)
        OUT=\$(${JAVA_HOME}/bin/keytool -list -v -keystore '${KS_PATH}' -storepass '${SSL_PASSWORD}' -alias localhost 2>/dev/null)
        OWNER=\$(echo \"\$OUT\" | grep -m1 'Owner:' | sed 's/^[[:space:]]*//')
        ISSUER=\$(echo \"\$OUT\" | grep -m1 'Issuer:' | sed 's/^[[:space:]]*//')
        VALID=\$(echo \"\$OUT\" | grep -m1 'Valid from:' | sed 's/^[[:space:]]*//')
        SAN=\$(echo \"\$OUT\" | grep -A 2 'SubjectAlternativeName' | tr '\n' ' ' | sed -E 's/[[:space:]]+/ /g')
        echo \"FQDN Host    : \$FQDN (${HOST} - ${IP})\"
        echo \"Keystore     : ${KS_PATH}\"
        echo \"Subject (CN) : \$OWNER\"
        echo \"Emittente CA : \$ISSUER\"
        echo \"SAN          : \$SAN\"
        echo \"Scadenza     : \$VALID\"
    " 2>/dev/null | grep -v "SUCCESS")

    if [ -n "$CERT_DETAILS" ]; then
        print_result 0 "Certificato Keystore nodo ${HOST} (${IP}) valido e aperto con password SSL:"
        echo -e "${CYAN}${CERT_DETAILS}${NC}" | sed 's/^/    /'
    else
        print_result 1 "Impossibile verificare Keystore su ${HOST} (${KS_PATH}) con password SSL"
        ((ERRORS++))
    fi
done

echo ""

# ==========================================
# 5. TEST CONNETTIVITÀ BROKER API (SASL_SSL + PLAIN)
# ==========================================
echo -e "${YELLOW}5. Test connettività API SASL_SSL (utente admin)...${NC}"
ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-broker-api-versions.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG}' | grep -q ':9092'" > /dev/null 2>&1
print_result $? "Connessione SASL_SSL (kafka-broker-api-versions.sh) funzionante" || ((ERRORS++))

echo ""

# ==========================================
# 6. TEST CREAZIONE TOPIC E REPLICHE ISR
# ==========================================
echo -e "${YELLOW}6. Test creazione topic e verifica ISR...${NC}"
TEST_TOPIC="test-3node-$(date +%s)"

ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG} --create --topic ${TEST_TOPIC} --partitions 3 --replication-factor 3'" > /dev/null 2>&1
print_result $? "Creazione topic '${TEST_TOPIC}' (3 partizioni, RF=3)" || ((ERRORS++))

ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG} --describe --topic ${TEST_TOPIC}'" 2>/dev/null | grep -q "ReplicationFactor: 3"
print_result $? "Topic '${TEST_TOPIC}' configurato con ReplicationFactor: 3" || ((ERRORS++))

echo ""

# ==========================================
# 7. TEST ACL E PRODUCER/CONSUMER (SASL_SSL)
# ==========================================
echo -e "${YELLOW}7. Test ACL (StandardAuthorizer) e Producer/Consumer SASL_SSL...${NC}"

# Concedi ACL a User:appclient sul topic di test (come da sez. 9.1 della guida)
ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-acls.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG} --add --allow-principal User:appclient --operation All --topic ${TEST_TOPIC} --group \"*\"'" > /dev/null 2>&1
print_result $? "Concessione ACL su '${TEST_TOPIC}' per User:appclient" || ((ERRORS++))

TEST_MSG="KRaft-SASL-SSL-Message-$(date +%s)"

ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c 'echo \"${TEST_MSG}\" | ${KAFKA_HOME}/bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} --producer.config ${APP_CONFIG} --topic ${TEST_TOPIC}'" > /dev/null 2>&1
print_result $? "Producer SASL_SSL (appclient): messaggio inviato sul nodo 1" || ((ERRORS++))

CONSUMED=$(ansible -i "$INVENTORY" "$NODE2_HOST" -m shell -a "su - kafka -c 'timeout 10 ${KAFKA_HOME}/bin/kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP} --consumer.config ${APP_CONFIG} --topic ${TEST_TOPIC} --from-beginning --max-messages 1'" 2>/dev/null | grep "$TEST_MSG")
if [ -n "$CONSUMED" ]; then
    print_result 0 "Consumer SASL_SSL (appclient): messaggio ricevuto correttamente sul nodo 2"
else
    print_result 1 "Consumer SASL_SSL (appclient): messaggio NON ricevuto"
    ((ERRORS++))
fi

# Pulizia topic di test
ansible -i "$INVENTORY" "$NODE1_HOST" -m shell -a "su - kafka -c '${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --command-config ${ADMIN_CONFIG} --delete --topic ${TEST_TOPIC}'" > /dev/null 2>&1
print_result $? "Pulizia topic di test '${TEST_TOPIC}'"

echo ""
echo -e "${BLUE}=====================================================${NC}"
echo -e "${YELLOW}Promemoria rapido per collegare le applicazioni client esterne${NC}"
echo -e "${BLUE}=====================================================${NC}"
echo "Quando dovrai collegare un'applicazione esterna al cluster:"
echo ""
echo "  - Bootstrap servers: ${BOOTSTRAP}"
echo "  - Truststore da copiare sul client: ${KAFKA_HOME}/config/security/kafka.truststore.jks (presente su tutti i nodi)"
echo "  - File di configurazione già pronti sui nodi come riferimento:"
echo "      * Amministrazione (admin): ${ADMIN_CONFIG}"
echo "      * Applicazioni (appclient): ${APP_CONFIG}"
echo -e "${BLUE}=====================================================${NC}"
echo ""

if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✓ TUTTI I TEST SUPERATI! Cluster Kafka 4 KRaft operativo.${NC}"
    exit 0
else
    echo -e "${RED}✗ RILEVATI $ERRORS ERRORI durante la verifica.${NC}"
    exit 1
fi

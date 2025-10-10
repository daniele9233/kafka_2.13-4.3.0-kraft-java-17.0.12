
#!/bin/bash

# Script di verifica completa Kafka Cluster con TLS
# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

INVENTORY="inventory.ini"
BOOTSTRAP_PLAIN="10.205.166.218:9092,10.205.166.214:9092,10.205.166.215:9092"
BOOTSTRAP_SSL="10.205.166.218:9093,10.205.166.214:9093,10.205.166.215:9093"

echo -e "${BLUE}=========================================="
echo "KAFKA CLUSTER - VERIFICA COMPLETA"
echo -e "==========================================${NC}\n"

# Funzione per stampare risultati
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        return 1
    fi
}

# Contatore errori
ERRORS=0

# ==========================================
# 1. VERIFICA SERVIZI
# ==========================================
echo -e "${YELLOW}1. Verifica servizi Zookeeper e Kafka...${NC}"
ansible -i $INVENTORY kafka_cluster -a "systemctl is-active zookeeper" --become > /dev/null 2>&1
print_result $? "Zookeeper attivo su tutti i nodi" || ((ERRORS++))

ansible -i $INVENTORY kafka_cluster -a "systemctl is-active kafka" --become > /dev/null 2>&1
print_result $? "Kafka attivo su tutti i nodi" || ((ERRORS++))

echo ""

# ==========================================
# 2. VERIFICA PORTE
# ==========================================
echo -e "${YELLOW}2. Verifica porte in ascolto...${NC}"
for port in 2181 9092 9093; do
    ansible -i $INVENTORY kafka_cluster -m shell -a "ss -tlnp | grep :$port" --become > /dev/null 2>&1
    print_result $? "Porta $port in ascolto su tutti i nodi" || ((ERRORS++))
done

echo ""

# ==========================================
# 3. VERIFICA ZOOKEEPER
# ==========================================
echo -e "${YELLOW}3. Verifica stato Zookeeper...${NC}"
ansible -i $INVENTORY kafka_cluster -a "/opt/zookeeper/bin/zkServer.sh status" --become 2>/dev/null | grep -q "Mode:"
print_result $? "Zookeeper cluster operativo" || ((ERRORS++))

ansible -i $INVENTORY kafkazoo1 -m shell -a "echo 'ls /brokers/ids' | /opt/zookeeper/bin/zkCli.sh -server localhost:2181 2>/dev/null | grep '\[1, 2, 3\]'" --become > /dev/null 2>&1
print_result $? "Tutti e 3 i broker registrati in Zookeeper" || ((ERRORS++))

echo ""

# ==========================================
# 4. VERIFICA CERTIFICATI SSL
# ==========================================
echo -e "${YELLOW}4. Verifica certificati SSL...${NC}"
ansible -i $INVENTORY kafka_cluster -m shell -a "test -f /opt/kafka/config/certs/kafka.server.keystore.jks && test -f /opt/kafka/config/certs/kafka.server.truststore.jks && echo OK" --become 2>/dev/null | grep -q "OK"
print_result $? "Certificati SSL presenti su tutti i nodi" || ((ERRORS++))

ansible -i $INVENTORY kafka_cluster -m shell -a "cd /opt/kafka/config/certs && /opt/jdk17/bin/keytool -list -v -keystore kafka.server.keystore.jks -storepass changeit 2>/dev/null | grep -q 'SubjectAlternativeName'" --become > /dev/null 2>&1
print_result $? "Certificati contengono Subject Alternative Names" || ((ERRORS++))

echo ""

# ==========================================
# 5. TEST CONNESSIONE PLAINTEXT (9092)
# ==========================================
echo -e "${YELLOW}5. Test connessione PLAINTEXT (porta 9092)...${NC}"
ansible -i $INVENTORY kafkazoo1 -a "kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_PLAIN" --become > /dev/null 2>&1
print_result $? "Connessione PLAINTEXT funzionante" || ((ERRORS++))

TOPIC_LIST=$(ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --list --bootstrap-server 10.205.166.218:9092" --become 2>/dev/null | grep -v "kafkazoo1")
print_result $? "Lista topic PLAINTEXT" || ((ERRORS++))

echo ""

# ==========================================
# 6. TEST CONNESSIONE SSL (9093)
# ==========================================
echo -e "${YELLOW}6. Test connessione SSL (porta 9093)...${NC}"
ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --list --bootstrap-server 10.205.166.218:9093 --command-config /opt/kafka/config/client-ssl.properties" --become > /dev/null 2>&1
print_result $? "Connessione SSL funzionante" || ((ERRORS++))

echo ""

# ==========================================
# 7. TEST CREAZIONE TOPIC
# ==========================================
echo -e "${YELLOW}7. Test creazione topic...${NC}"
TEST_TOPIC="health-check-$(date +%s)"

# Crea topic su PLAINTEXT
ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --create --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9092 --replication-factor 3 --partitions 3" --become > /dev/null 2>&1
print_result $? "Creazione topic '$TEST_TOPIC'" || ((ERRORS++))

# Verifica topic
ansible -i $INVENTORY kafkazoo1 -m shell -a "kafka-topics --describe --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9092" --become 2>/dev/null | grep -q "ReplicationFactor: 3"
print_result $? "Topic ha replication factor 3" || ((ERRORS++))

ansible -i $INVENTORY kafkazoo1 -m shell -a "kafka-topics --describe --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9092" --become 2>/dev/null | grep -q "Isr: 1,2,3\|Isr: 1,3,2\|Isr: 2,1,3\|Isr: 2,3,1\|Isr: 3,1,2\|Isr: 3,2,1"
print_result $? "Tutte le repliche in-sync (ISR)" || ((ERRORS++))

echo ""

# ==========================================
# 8. TEST PRODUCER/CONSUMER PLAINTEXT
# ==========================================
echo -e "${YELLOW}8. Test Producer/Consumer PLAINTEXT...${NC}"
TEST_MSG="Test message $(date +%s)"

# Produce
ansible -i $INVENTORY kafkazoo1 -m shell -a "echo '$TEST_MSG' | kafka-console-producer --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9092" --become > /dev/null 2>&1
print_result $? "Producer PLAINTEXT: messaggio inviato" || ((ERRORS++))

# Consume
CONSUMED=$(ansible -i $INVENTORY kafkazoo2 -a "timeout 5 kafka-console-consumer --topic $TEST_TOPIC --from-beginning --bootstrap-server 10.205.166.214:9092 --max-messages 1" --become 2>/dev/null | grep "$TEST_MSG")
if [ ! -z "$CONSUMED" ]; then
    print_result 0 "Consumer PLAINTEXT: messaggio ricevuto"
else
    print_result 1 "Consumer PLAINTEXT: messaggio NON ricevuto"
    ((ERRORS++))
fi

echo ""

# ==========================================
# 9. TEST PRODUCER/CONSUMER SSL
# ==========================================
echo -e "${YELLOW}9. Test Producer/Consumer SSL...${NC}"
TEST_MSG_SSL="SSL test message $(date +%s)"

# Produce SSL
ansible -i $INVENTORY kafkazoo1 -m shell -a "echo '$TEST_MSG_SSL' | kafka-console-producer --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9093 --producer.config /opt/kafka/config/client-ssl.properties" --become > /dev/null 2>&1
print_result $? "Producer SSL: messaggio inviato" || ((ERRORS++))

# Consume SSL
CONSUMED_SSL=$(ansible -i $INVENTORY kafkazoo3 -a "timeout 5 kafka-console-consumer --topic $TEST_TOPIC --from-beginning --bootstrap-server 10.205.166.215:9093 --consumer.config /opt/kafka/config/client-ssl.properties --max-messages 10" --become 2>/dev/null | grep "$TEST_MSG_SSL")
if [ ! -z "$CONSUMED_SSL" ]; then
    print_result 0 "Consumer SSL: messaggio ricevuto"
else
    print_result 1 "Consumer SSL: messaggio NON ricevuto"
    ((ERRORS++))
fi

echo ""

# ==========================================
# 10. TEST ALTA DISPONIBILITÀ
# ==========================================
echo -e "${YELLOW}10. Test Alta Disponibilità (failover)...${NC}"
HA_TOPIC="ha-test-$(date +%s)"

# Crea topic
ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --create --topic $HA_TOPIC --bootstrap-server 10.205.166.218:9092 --replication-factor 3 --partitions 1" --become > /dev/null 2>&1
print_result $? "Creato topic per test HA" || ((ERRORS++))

# Invia messaggio
ansible -i $INVENTORY kafkazoo1 -m shell -a "echo 'HA Test Message' | kafka-console-producer --topic $HA_TOPIC --bootstrap-server 10.205.166.218:9092" --become > /dev/null 2>&1
print_result $? "Messaggio inviato per test HA" || ((ERRORS++))

# Ferma broker 1
echo -e "   ${BLUE}Simulazione failure: fermo kafkazoo1...${NC}"
ansible -i $INVENTORY kafkazoo1 -m systemd -a "name=kafka state=stopped" --become > /dev/null 2>&1
sleep 5

# Verifica che gli altri broker continuino a funzionare
ansible -i $INVENTORY kafkazoo2 -a "kafka-topics --describe --topic $HA_TOPIC --bootstrap-server 10.205.166.214:9092" --become > /dev/null 2>&1
print_result $? "Cluster operativo con broker 1 down" || ((ERRORS++))

# Leggi messaggio dal broker 2
CONSUMED_HA=$(ansible -i $INVENTORY kafkazoo2 -a "timeout 5 kafka-console-consumer --topic $HA_TOPIC --from-beginning --bootstrap-server 10.205.166.214:9092,10.205.166.215:9092 --max-messages 1" --become 2>/dev/null | grep "HA Test Message")
if [ ! -z "$CONSUMED_HA" ]; then
    print_result 0 "Dati accessibili con broker 1 down (HA funzionante)"
else
    print_result 1 "Dati NON accessibili con broker 1 down"
    ((ERRORS++))
fi

# Riavvia broker 1
echo -e "   ${BLUE}Riavvio kafkazoo1...${NC}"
ansible -i $INVENTORY kafkazoo1 -m systemd -a "name=kafka state=started" --become > /dev/null 2>&1
sleep 10

ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --describe --topic $HA_TOPIC --bootstrap-server 10.205.166.218:9092" --become > /dev/null 2>&1
print_result $? "Broker 1 rientrato nel cluster" || ((ERRORS++))

echo ""

# ==========================================
# 11. VERIFICA CONFIGURAZIONE CLUSTER
# ==========================================
echo -e "${YELLOW}11. Verifica configurazione cluster...${NC}"

# Verifica numero broker
NUM_BROKERS=$(ansible -i $INVENTORY kafkazoo1 -m shell -a "kafka-broker-api-versions --bootstrap-server 10.205.166.218:9092 2>/dev/null | grep -c 'id:'" --become 2>/dev/null | tail -1)
if [ "$NUM_BROKERS" == "3" ]; then
    print_result 0 "Cluster con 3 broker attivi"
else
    print_result 1 "Cluster NON ha 3 broker attivi (trovati: $NUM_BROKERS)"
    ((ERRORS++))
fi

# Verifica min.insync.replicas
ansible -i $INVENTORY kafkazoo1 -a "grep 'min.insync.replicas=2' /opt/kafka/config/server.properties" --become > /dev/null 2>&1
print_result $? "min.insync.replicas=2 configurato" || ((ERRORS++))

echo ""

# ==========================================
# 12. CLEANUP TOPIC DI TEST
# ==========================================
echo -e "${YELLOW}12. Cleanup topic di test...${NC}"
ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --delete --topic $TEST_TOPIC --bootstrap-server 10.205.166.218:9092" --become > /dev/null 2>&1
print_result $? "Topic '$TEST_TOPIC' eliminato" || ((ERRORS++))

ansible -i $INVENTORY kafkazoo1 -a "kafka-topics --delete --topic $HA_TOPIC --bootstrap-server 10.205.166.218:9092" --become > /dev/null 2>&1
print_result $? "Topic '$HA_TOPIC' eliminato" || ((ERRORS++))

echo ""

# ==========================================
# RIEPILOGO FINALE
# ==========================================
echo -e "${BLUE}=========================================="
echo "RIEPILOGO VERIFICA"
echo -e "==========================================${NC}"

if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✓ TUTTI I TEST SUPERATI!${NC}"
    echo -e "${GREEN}Il cluster Kafka è completamente operativo e production-ready!${NC}"
    echo ""
    echo "Configurazione cluster:"
    echo "  - 3 nodi Kafka (broker ID: 1, 2, 3)"
    echo "  - 3 nodi Zookeeper"
    echo "  - TLS/SSL abilitato (porta 9093)"
    echo "  - PLAINTEXT abilitato (porta 9092)"
    echo "  - Replication factor: 3"
    echo "  - Min ISR: 2"
    echo "  - Alta disponibilità testata e funzionante"
    exit 0
else
    echo -e "${RED}✗ $ERRORS TEST FALLITI${NC}"
    echo -e "${RED}Verificare i log per maggiori dettagli.${NC}"
    exit 1
fi

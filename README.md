# Cluster Apache Kafka 4.x KRaft a 3 Nodi con Sicurezza SASL_SSL (Ansible)

Playbook Ansible per il deploy automatizzato di un cluster **Apache Kafka 4.x (4.3.0)** in modalità **KRaft combined** (`broker,controller`, senza ZooKeeper) su **Ubuntu 26**, con crittografia **TLS (CA interna, keystore JKS per nodo, truststore JKS condiviso)**, autenticazione **SASL/PLAIN**, autorizzazione **StandardAuthorizer (ACL)** e **Java 17 (Zulu JDK 17 LTS) confinata all'utente `kafka`**.

Riferimento architetturale: [`Guida_Kafka_KRaft_3_Nodi_SASL_SSL_v3.pdf`](./Guida_Kafka_KRaft_3_Nodi_SASL_SSL_v3.pdf)

---

## 1. Architettura del Cluster

| Host (Inventory)   | IP              | `node.id` | Ruoli KRaft           | Porte (`SASL_SSL`)                     |
| ------------------ | --------------- | --------- | --------------------- | -------------------------------------- |
| `Kafka-zkp01_prod` | `10.41.239.54`  | `1`       | `broker,controller`   | `9092` (BROKER), `9093` (CONTROLLER)   |
| `Kafka-zkp02_prod` | `10.41.239.55`  | `2`       | `broker,controller`   | `9092` (BROKER), `9093` (CONTROLLER)   |
| `kafka-zkp03_prod` | `10.41.239.56`  | `3`       | `broker,controller`   | `9092` (BROKER), `9093` (CONTROLLER)   |

### Layout su partizione `/opt` (già montata sui server)

- **Home utente `kafka` e installazione Kafka**: `/opt/kafka`
- **JVM Zulu 17 LTS confinata all'utente `kafka`**: `/opt/kafka/jdk17` (configurata in `/opt/kafka/.bashrc`, `/opt/kafka/.profile` e nel servizio `systemd`, senza alterare la Java di sistema)
- **Dati e metadati KRaft**: `/opt/kafka/data` (`meta.properties`, `__cluster_metadata-0`)
- **Log Kafka**: `/opt/kafka/logs`
- **Certificati TLS (CA, Keystore, Truststore)**: `/opt/kafka/config/security`
- **File proprietà client preconfigurati**:
  - `/opt/kafka/client.properties` (utente superuser `admin`)
  - `/opt/kafka/appclient.properties` (utente applicativo `appclient`)

---

## 2. File Globale delle Variabili

Tutte le impostazioni del cluster (IP, porte, versioni, percorsi su `/opt`, password TLS/SASL, utenti JAAS, parametri KRaft e systemd) sono centralizzate in:
- [`group_vars/all.yml`](./group_vars/all.yml)
- [`inventory.ini`](./inventory.ini)

---

## 3. Esecuzione del Playbook

Il playbook è configurato per essere eseguito con l'utente `root`:

```bash
# Verifica sintassi e connettività
ansible-playbook -i inventory.ini playbook.yml --syntax-check
ansible -i inventory.ini kafka_cluster -m ping

# Installazione e configurazione del cluster Kafka 4 KRaft
ansible-playbook -i inventory.ini playbook.yml
```

### Verifica automatica del cluster

```bash
./verify-kafka-cluster.sh
```

### Cleanup completo (attenzione: rimuove dati e installazione in `/opt/kafka`)

```bash
ansible-playbook -i inventory.ini cleanup.yml
```

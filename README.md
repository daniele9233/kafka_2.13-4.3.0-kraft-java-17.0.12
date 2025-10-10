# kafka-zookeeper-ansible-TLS


| Componente      | Porta          | TLS Attivo | Note                               |
| --------------- | -------------- | ---------- | ---------------------------------- |
| Client ↔ Broker | 9093           | ✅ Sì       | Funziona con certificato valido    |
| Broker ↔ Broker | (inter-broker) | ❌ No       | Attualmente usa PLAINTEXT          |
| Certificato     | CN=kafkazoo3   | ✅ Valido   | Emesso da CA interna (self-signed) |

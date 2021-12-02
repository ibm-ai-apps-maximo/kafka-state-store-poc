# About kafka-python-poc
POC to demonstrate implementation of local state store.

# Prerequisites
* docker-compose
* Java 8
* Maven 3

# Start and Configure Kafka services
```
docker-compose up -d
```

## Creating a test topic and changelog topic
```bash
docker exec -it 4da81527e4e5 bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
--zookeeper zookeeper:2181 \
--replication-factor 1 \
--partitions 10 \
--topic test-topic
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
--zookeeper zookeeper:2181 \
--replication-factor 1 \
--partitions 10 \
--config "cleanup.policy=compact"
--topic test-topic-changelog

```

# Running State Store POC

## Build the kafka-state-store-poc artifact
```
    mvn clean install
```

## Run first consumer
```
    mvn exec:java@consumer1
```

## Run producer
```
    mvn exec:java@producer
```

## Run second consumer which should cause rebalance
```
    mvn exec:java@consumer2
```    
# Monitoring

## Monitoring consumer lag
```bash
while true; do ./bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --group my-group && sleep 1; done
```
## Consuming from topics with CLI
```
docker exec -it 8b6a59cd123f bash
cd /opt/bitnami/kafka
bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092
```
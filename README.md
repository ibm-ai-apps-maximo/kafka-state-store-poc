# About kafka-python-poc
POC to demonstrate implementation of local state store.

# Prerequisites
* docker-compose
* Java 9 or greater
* Maven 3

# Start and Configure Kafka services
```
docker-compose up -d
```

## Creating a test topic
```bash
docker exec -it 4da81527e4e5 bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
--zookeeper zookeeper:2181 \
--replication-factor 1 \
--partitions 10 \
--topic test-topic
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
# Monitoring & Kafka Administration

## Viewing logs
All process will write log messages to target/logs/poc.log.  The messages are interweaved and the log provides a single view into the state of the system. Messages are also sent to the consoles of each process.  The log4j2.xml file at src/main/resources controls the behavior of the loggers.

The format of each message in poc.log starts with a timestamp and then the process id. For example:

```
2021-12-02 13:53:29,172 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-3',	key='count',	value='529606'
```
## Confirming state store restoration after rebalance

To confirm that state store restoration is happening correctly. Find a message with "onPartitionsRevoked" then messages with "destory" from the same process id with a newer timestamp will follow. Next find message with "onPartitionsAssigned"  then messages with "destory" from the same process id with a newer timestamp will follow.  Match up the store names of the destory and restore messages to confirm that the values of the key count are equal.

For example let's say you have this log:

```
2021-12-02 13:53:29,169 17080 ERROR c.f.Consumer [co.f4.Main.main()] Consumer.RebalanceListener.onPartitionsRevoked -  partitions: [test-topic-4, test-topic-5, test-topic-2, test-topic-3, test-topic-0, test-topic-1, test-topic-8, test-topic-9, test-topic-6, test-topic-7]
2021-12-02 13:53:29,172 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-3',	key='count',	value='529606'
2021-12-02 13:53:29,216 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-0',	key='count',	value='273646'
2021-12-02 13:53:29,254 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-8',	key='count',	value='213521'
2021-12-02 13:53:29,291 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-9',	key='count',	value='223343'
2021-12-02 13:53:29,326 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-7',	key='count',	value='140352'
2021-12-02 13:53:29,415 17080 ERROR c.f.Consumer [co.f4.Main.main()] Consumer.RebalanceListener.onPartitionsAssigned with partitions: [test-topic-4, test-topic-2, test-topic-7, test-topic-0, test-topic-1]
2021-12-02 13:53:29,427 9296 ERROR c.f.Consumer [co.f4.Main.main()] Consumer.RebalanceListener.onPartitionsAssigned with partitions: [test-topic-5, test-topic-8, test-topic-9, test-topic-6, test-topic-3]
2021-12-02 13:53:31,060 9296 ERROR c.f.RocksDBStateStore [co.f4.Main.main()] restore 	store name='test-topic-3',	key='count',	value='529606'
```

From the log above you may pair the two messages below to confirm the state store was restored correctly on the new consumer (with pid 9296):
```
2021-12-02 13:53:29,172 17080 ERROR c.f.Consumer [co.f4.Main.main()] destory 	store name='test-topic-3',	key='count',	value='529606'
2021-12-02 13:53:31,060 9296 ERROR c.f.RocksDBStateStore [co.f4.Main.main()] restore 	store name='test-topic-3',	key='count',	value='529606'
```

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

## Deleting all topics
```
docker exec -it 8b6a59cd123f bash
cd /opt/bitnami/kafka
bin/kafka-topics.sh --zookeeper zookeeper:2181 --delete --topic 'test-topic.*'
```

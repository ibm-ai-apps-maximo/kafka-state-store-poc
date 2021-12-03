package co.f4;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;

public class Consumer {
    private static final Logger logger = LogManager.getLogger(Consumer.class);

    final Map<String, RocksDBStateStore> stateStoreMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: java co.f4.Consumer [consumer|producer] <topic> <group>");
        }

        String topic = args[1];
        String group = args[2];

        Consumer consumer = new Consumer();
        consumer.run(topic, group);
    }

    public void run(String topic, String consumerGroup) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
            RebalanceListener rebalanceListener = new RebalanceListener();
            consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
            logger.debug("Consumer subscribed to topic='{}' group='{}'", topic, consumerGroup);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    RocksDBStateStore stateStore = getStateStore(record.topic() + "-" + record.partition());
                    byte[] count = stateStore.get("count");
                    String newCount = (count != null
                            ? "" + (Long.parseLong(new String(count, StandardCharsets.UTF_8)) + 1)
                            : "1");
                    stateStore.put("count", newCount.getBytes());
                    logger.trace("Consumer - partition={}, offset={}, key={}, value={}, count={}", record.partition(),
                            record.offset(), record.key(), record.value(), newCount);
                }
            }
        }
    }

    public RocksDBStateStore getStateStore(String key) throws RocksDBException {
        if (stateStoreMap.containsKey(key)) {
            return stateStoreMap.get(key);
        }

        // does not exist so create and put in map
        RocksDBStateStore store = new RocksDBStateStore(key);
        stateStoreMap.put(key, store);
        return store;
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.error("Consumer.RebalanceListener.onPartitionsRevoked - partitions: {}", (partitions != null ? partitions : "null"));
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.error("Consumer.RebalanceListener.onPartitionsAssigned with partitions: {}", (partitions != null ? partitions : "null"));

            Set<String> stateStoreNamesToRetain = new HashSet<String>();
            // close then delete state stores for the partitions that are no longer assigned to the consumer
            for (TopicPartition topicPartition : partitions) {
                String stateStoreName = topicPartition.topic() + "-" + topicPartition.partition();
                stateStoreNamesToRetain.add(stateStoreName);
            }

            Set<String> stateStoreMapKeySet = stateStoreMap.keySet();
            
            // close then delete state stores for the partitions that are no longer assigned to the consumer
            //for (String stateStoreNameKey : stateStoreMapKeySet) {
            for (Iterator<String> iterator = stateStoreMapKeySet.iterator(); iterator.hasNext(); ) {
                String stateStoreNameKey = iterator.next();
                if (stateStoreNamesToRetain.contains(stateStoreNameKey)) {
                    logger.debug("Consumer.RebalanceListener.onPartitionsAssigned - retaining state store '{}'", stateStoreNameKey);
                    continue;
                }

                logger.debug("Consumer.RebalanceListener.onPartitionsAssigned - removing state store from stateStoreMap with key='{}'", stateStoreNameKey);

                RocksDBStateStore stateStore;

                stateStore = stateStoreMap.get(stateStoreNameKey);
                iterator.remove();

                if (stateStore != null) {
                    byte[] value = null;
                    try {
                        value = stateStore.get("count");
                    } catch (Exception e) {
                        logger.error("Exception calling stateStore.get(\"count\")", e);
                    }
                    
                    
                    logger.error("destory \tstore name='{}',\tkey='count',\tvalue='{}'", stateStoreNameKey, (value != null ? new String(value, StandardCharsets.UTF_8) : "null"));
                    try {
                        logger.debug("Consumer.RebalanceListener.onPartitionsAssigned - destroying state store '{}'", stateStoreNameKey);
                        stateStore.destroy();
                    } catch (RocksDBException e) {
                        // cannot recover from this exception so simply log
                        if (logger.isWarnEnabled()) {
                            logger.warn("Failed to destory state store'" + stateStoreNameKey + "'", e);
                        }
                    }
                    logger.debug("Consumer.RebalanceListener.onPartitionsAssigned - closing state store '{}'", stateStoreNameKey);
                    stateStore.close();
                } else {
                    logger.debug("Consumer.RebalanceListener.onPartitionsAssigned - no store in stateStoreMap with key='{}'", stateStoreNameKey);
                }
            }

            logger.error("Consumer.RebalanceListener.onPartitionsAssigned - done");

            // no need to restore here
            // lazy restoration is used by allowing Consumer.getStateStore to create the
            // state store when not in stateStoreMap.  Restoration will occur upon instantation
            // of the store.
        }
    }
}
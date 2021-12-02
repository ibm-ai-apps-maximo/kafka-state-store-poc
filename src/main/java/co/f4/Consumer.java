package co.f4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.RocksDBException;

public class Consumer {
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
            System.out.println("Consumer subscribed to topic='" + topic + "' group='" + consumerGroup + "'");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    RocksDBStateStore stateStore = getStateStore(record.topic() + "-" + record.partition());
                    byte[] count = stateStore.get("count");
                    // long newCount = (count != null ? ByteBuffer.wrap(count).getLong() + 1 : 1);
                    String newCount = (count != null
                            ? "" + (Long.parseLong(new String(count, StandardCharsets.UTF_8)) + 1)
                            : "1");
                    // stateStore.put("count".getBytes(), longToByteArray(newCount));
                    stateStore.put("count", newCount.getBytes());
                    System.out.printf("Consumer - partition=%d, offset=%d, key=%s, value=%s, count=%s\n", record.partition(),
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

    public static byte[] longToByteArray(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static byte[] longToByteArray(Long value) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        byte[] returnValue = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(value);
            out.flush();
            returnValue = bos.toByteArray();
            return returnValue;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
                return returnValue;
            }
        }
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Consumer.RebalanceListener.onPartitionsRevoked -  partitions:" + (partitions != null ? partitions : "null"));

            // close then delete state stores for revoked partitions
            for (TopicPartition topicPartition : partitions) {
                String key = topicPartition.topic() + "-" + topicPartition.partition();
                System.out.println("Consumer.RebalanceListener.onPartitionsRevoked -  removing state store from stateStoreMap with key='" + key + "'");

                RocksDBStateStore stateStore;

                // added thread safety because I have experienced odd scenarios where I suspect
                // 1) onPartitionsRevoked was invoked concurrently (which is not possible based on Kakfa docs and therefore unlikely) or
                // 2) a RockDB native resource has not completed work but has prematurely returned control back to Java counterpart ending onPartitionsRevoked invocation early
                // which would allow reentry into onPartitionsRevoked - this was likelly due to use of older version of RocksDB
                synchronized (stateStoreMap) {
                    stateStore = stateStoreMap.remove(key);
                }

                if (stateStore != null) {
                    byte[] value = null;
                    try {
                        value = stateStore.get("count");
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                    
                    System.err.println("Consumer.RebalanceListener.onPartitionsRevoked - ****** state store from stateStoreMap with key='" + key + "' has key-value: key=count and value=" + new String(value, StandardCharsets.UTF_8));
                    System.err.println(System.nanoTime() + " ****** before destory store name='" + key + "', key='count', value='" + new String(value, StandardCharsets.UTF_8) + "'");
                    System.out.println("Consumer.RebalanceListener.onPartitionsRevoked - ****** state store from stateStoreMap with key='" + key + "' has key-value: key=count and value=" + new String(value, StandardCharsets.UTF_8));
                    System.out.println(System.nanoTime() + " ****** before destory store name='" + key + "', key='count', value='" + new String(value, StandardCharsets.UTF_8) + "'");
                    try {
                        System.out.println("Consumer.RebalanceListener.onPartitionsRevoked -  destorying state store from stateStoreMap with key='" + key + "'");
                        stateStore.destroy();
                    } catch (RocksDBException e) {
                        // cannot recover from this exception so simply log
                        e.printStackTrace();
                    }
                    System.out.println("Consumer.RebalanceListener.onPartitionsRevoked -  closing state store from stateStoreMap with key='" + key + "'");
                    stateStore.close();
                } else {
                    System.out.println("Consumer.RebalanceListener.onPartitionsRevoked - no store in stateStoreMap with key='" + key + "'" );
                }
            }
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Consumer.RebalanceListener.onPartitionsAssigned with partitions:" + (partitions != null ? partitions : "null"));

            // no need to restore here
            // lazy restoration is used by allowing Consumer.getStateStore to create the
            // state store when not in stateStoreMap.  Restoration will occur upon instatantion
            // of the store. 
            
        }
    }
}
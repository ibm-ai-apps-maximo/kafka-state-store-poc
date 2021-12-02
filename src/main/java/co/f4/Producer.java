package co.f4;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

    public class Producer {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: java co.f4.Producer [consumer|producer] <topic>");
        }

        String topic = args[1];

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, "4"); // in bytes, low because messages are small
        //props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // for this poc, we don't care if the send is successul or not
        // the goal is to confirm that the state store works correctly and this
        // can be comfirmed by analyzing the consumed messages only
        SendCallback callback = new SendCallback();
        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            // generate random number between 1 and 10
            String key = Integer.toString(random.nextInt(10 - 1) + 1);            
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, Integer.toString(i));
            //long startTime = System.currentTimeMillis();
            producer.send(record, callback);
            //long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("Sent topic='" + topic + "' key='" + key + "' value='" + i + "'");

        }
        System.out.println("Producer flushing and closing...");
        producer.flush();
        producer.close();
        System.out.println("Producer completed flushing and closing");
    }

    private static class SendCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("Sent message to topic=%s partition=%s offset=%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }
}
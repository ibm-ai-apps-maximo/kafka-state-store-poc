package co.f4;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StateStoreChangelogKeyDeserializer implements Deserializer<StateStoreChangelogKey> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public StateStoreChangelogKey deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, "UTF-8"), StateStoreChangelogKey.class);
        } catch (Exception e) {
            System.out.println("Unable to deserialize message " + e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}

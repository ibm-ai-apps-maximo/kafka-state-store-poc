package co.f4;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StateStoreChangelogKeySerializer implements Serializer<StateStoreChangelogKey> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String arg0, StateStoreChangelogKey key) {
        byte[] retValue = null;
        try {
            retValue = objectMapper.writeValueAsString(key).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retValue;
    }

    @Override
    public void close() {
    }
}

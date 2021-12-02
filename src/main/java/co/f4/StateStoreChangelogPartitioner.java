package co.f4;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class StateStoreChangelogPartitioner implements Partitioner {

    public void configure(Map<String, ?> configs) {        
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partition = 0;

        if (keyBytes == null || !(key instanceof StateStoreChangelogKey)) {
            throw new InvalidRecordException("All messages must have key of StateStoreChangelogKey");
        }

        StateStoreChangelogKey changelogKey = (StateStoreChangelogKey) key;

        if (changelogKey.getStoreName() == null || "".equals(changelogKey.getStoreName())) {
            throw new InvalidRecordException("All message key has null or empty StateStoreChangelogKey.storeName");
        }
        System.out.println("StateStoreChangelogPartitioner - key.storeName='" + changelogKey.getStoreName() + "', key.storeKey='" + changelogKey.getStoreKey() + "', partition='" + partition + "'");

        return Utils.toPositive(Utils.murmur2(changelogKey.getStoreName().getBytes())) % numPartitions;
    }

    public void close() {
    }
}
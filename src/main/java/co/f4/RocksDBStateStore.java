package co.f4;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.DBOptions;

public class RocksDBStateStore implements Closeable {
  private static final String CHANGELOG_TOPIC_SUFFIX = "-cl";
  private final String name;
  private static final Set<String> names = new HashSet<>();

  private boolean closed = false;

  private final boolean restoreTombstonesAsNull;

  private final ColumnFamilyOptions cfOpts;

  // list of column family descriptors, first entry must always be default column
  // family
  private final List<ColumnFamilyDescriptor> cfDescriptors;

  // a list which will hold the handles for the column families once the db is
  // opened
  private final List<ColumnFamilyHandle> columnFamilyHandleList;

  private final DBOptions options;

  private final RocksDB db;

  private final String dbFilePath;
  private final KafkaProducer<String, byte[]> producer;
  private final String changelogTopicName;
  private final TopicPartition changelogTopicPartition;

  /**
   * Creates a state store backed by RocksDB.
   * 
   * @param name                    Name that must be unique within the Java
   *                                process. Depending on your use case, you may
   *                                need to maintain that the name is unique
   *                                within a Kafka cluster. State stores are
   *                                typically tied to the consumer and topic and
   *                                topic partition. In this case, the store name
   *                                should contain the topic name and partition.
   *                                This will ensure that the state store is
   *                                restored correctly after a rebalance of the
   *                                consumer group
   * @param restoreTombstonesAsNull A null value (aka tombstone) is placed in the
   *                                changelog when RocksDBStateStore.delete(btye[]
   *                                key) is invoked. If true then any tombstone in
   *                                the changelog is restored as a null value for
   *                                that key. Otherwise the key-value pair is
   *                                deleted from the state store during restore.
   * @throws RocksDBException
   */
  public RocksDBStateStore(String name, boolean restoreTombstonesAsNull) throws RocksDBException {
    this.name = name;
    this.restoreTombstonesAsNull = restoreTombstonesAsNull;
    
    // ensure name is unique and throw Exception if name already exists in JVM
    if (names.contains(name))
      throw new IllegalArgumentException("Instance already exists with name '" + name + "'");
    names.add(name);

    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();

    cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

    cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor(name.getBytes(), cfOpts));

    columnFamilyHandleList = new ArrayList<>();

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    Path stateDir = Paths.get(System.getProperty("user.dir"), "target", "" + ProcessHandle.current().pid(), "state");
    stateDir.toFile().mkdirs();
    dbFilePath = Paths.get(stateDir.toString(), name).toString();

    // RocksDB configuration used by Kafka can be found at:
    // https://sourcegraph.com/github.com/apache/kafka@2.8.1/-/blob/streams/src/main/java/org/apache/kafka/streams/state/internals/RocksDBStore.java?L126

    db = RocksDB.open(options, dbFilePath, cfDescriptors, columnFamilyHandleList);
    if (db == null) {
      System.out.println("RocksDB instance for state store name '" + name + "' could not be opened.");
      try {
        close();
      } catch (Exception e) {
        System.out.println("RocksDB instance for state store name '" + name + "' could not be closed.");
      }
      throw new RuntimeException("RocksDB instance for state store name '" + name + "' could not be opened.");
    }

    // init Kafka resources
    changelogTopicName = name + CHANGELOG_TOPIC_SUFFIX;
    changelogTopicPartition = new TopicPartition(changelogTopicName, 0); // there will only be one partition
    try {
      createChangelogTopic(changelogTopicName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    producer = createProducer();   
    
    restore();
  }

  public RocksDBStateStore(String name) throws RocksDBException {
    this(name, false);
  }

  public void close() {
    // TODO: don't execute other methods of this class if closed
    if (closed)
      return;

    closed = true;
    names.remove(name);

    producer.flush();
    producer.close();

    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.close();
    }

    if (db != null)
      db.close();

    options.close();
    cfOpts.close();
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }

  public void destroy() throws RocksDBException {
    /*
     * try (Options options = new Options()) {
     * RocksDB.destroyDB(dbFilePath, options);
     * }
     */

    /*
     * for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
     * String cfName = new String(columnFamilyHandle.getName(),
     * StandardCharsets.UTF_8);
     * if (name.equals(cfName)) {
     * System.out.println("destroy - closing columnFamilyHandle.name=\"" + cfName +
     * "\"");
     * columnFamilyHandle.close();
     * }
     * }
     */

    db.dropColumnFamily(columnFamilyHandleList.get(1));
  }

  public void put(String key, byte[] value) throws RocksDBException {
    // TODO: distributed transaction, must rollback so use TransactionDB and
    // producer transactions
    ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(changelogTopicName, key, value);

    try {
      // synchronous send to ensure the record is sent to changelog
      RecordMetadata metadata = producer.send(record).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    db.put(columnFamilyHandleList.get(1), key.getBytes(), value);
  }

  public void delete(String key) throws RocksDBException {
    // TODO: distributed transaction, must rollback so use TransactionDB and
    // producer transactions
    ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(changelogTopicName, key, null);
    try {
      // synchronous send to ensure the record is sent to changelog
      RecordMetadata metadata = producer.send(record).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    db.delete(columnFamilyHandleList.get(1), key.getBytes());
  }

  public byte[] get(String key) throws RocksDBException {
    return db.get(columnFamilyHandleList.get(1), key.getBytes());
  }

  /**
   * Restores the state store from the changelog. WARNING: Not thread safe.
   * 
   * see org/apache/kafka/streams/processor/internals/ProcessorStateManager for
   * how Kafka restores.
   * This class can be used as a guide to add additional features
   * 
   * @throws RocksDBException
   */
  public void restore() throws RocksDBException {
    System.out.println("restore - restoreTombstonesAsNull=" + restoreTombstonesAsNull
        + ", changelogTopicPartition.topic=" + changelogTopicPartition.topic() + ", changelogTopicPartition.partition="
        + changelogTopicPartition.partition());

    try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
      // only one partition but assigning here to re-enforce this design
      consumer.assign(Collections.singleton(changelogTopicPartition));
      consumer.seekToEnd(Collections.singleton(changelogTopicPartition));
      long endOffset = consumer.position(changelogTopicPartition);
      // consumer.poll(Duration.ofMillis(0)); // dummy poll() to join consumer group
      consumer.seekToBeginning(Collections.singletonList(changelogTopicPartition));

      long offset = -1;
      do {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, byte[]> record : records) {
          System.out.printf("restore - partition=%d, offset=%d, key=%s, value=%s\n", record.partition(),
              record.offset(), record.key(), record.value());

          if (record.value() == null) {
            if (restoreTombstonesAsNull) {
              System.out.println("restore - tombstone as null, db.put(\"" + record.key() + "\")");
              db.put(record.key().getBytes(), null);
            } else {
              System.out.println("restore - db.delete(\"" + record.key() + "\")");
              db.delete(record.key().getBytes());
            }
          } else {
            System.out.println("restore - db.put(\"" + record.key() + "\", \"" + record.value() + "\")");
            db.put(record.key().getBytes(), record.value());
          }
          offset = record.offset();
        }
      } while (offset != -1 && offset < endOffset);
    }

    // TODO: remove this code that is for test verification only
    byte[] count = get("count");
    System.err.println("RocksDBStateStore.restore - ****** name='" + name + "' key='count' value='"
        + (count != null ? new String(count, StandardCharsets.UTF_8) : "null") + "'");
    System.err.println(System.nanoTime() + " ****** after restore store name='" + name + "', key='count', value='"
        + (count != null ? new String(count, StandardCharsets.UTF_8) : "null") + "'");
    System.out.println("RocksDBStateStore.restore - ****** name='" + name + "' key='count' value='"
        + (count != null ? new String(count, StandardCharsets.UTF_8) : "null") + "'");
    System.out.println(System.nanoTime() + " ****** after restore store name='" + name + "', key='count', value='"
        + (count != null ? new String(count, StandardCharsets.UTF_8) : "null") + "'");
  }

  private void createChangelogTopic(String topicName) throws InterruptedException, ExecutionException {
    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    AdminClient adminClient = AdminClient.create(config);

    ListTopicsResult listTopics = adminClient.listTopics();
    Set<String> topicNames = listTopics.names().get();
    
    if (topicNames.contains(topicName) == false) {
      List<NewTopic> topicList = new ArrayList<NewTopic>();
      Map<String, String> topicConfigs = new HashMap<String, String>();
      // compact the log see: https://kafka.apache.org/documentation/#compaction
      // props that start with "log." are server default properties
      //topicConfigs.put("log.cleanup.policy", "compact");

      // see https://kafka.apache.org/documentation.html#topicconfigs
      topicConfigs.put("cleanup.policy", "compact");
      topicConfigs.put("min.compaction.lag.ms", "10");
      topicConfigs.put("max.compaction.lag.ms", "20");
      topicConfigs.put("min.cleanable.dirty.ratio", "0.01");
      topicConfigs.put("segment.ms", "100");
      topicConfigs.put("retention.ms", "-1"); // must retain all messages
      topicConfigs.put("delete.retention.ms", "1000");
      // A typical scenario would be to create a topic with a replication factor of 3, set min.insync.replicas to 2, and produce with acks of "all". This will ensure that the producer raises an exception if a majority of replicas do not receive a write.
      // TODO: enable min.insync.replicas when on larger cluster 
      //topicConfigs.put("min.insync.replicas", "2");

      int topicPartitions = 1; // must be one to ensure that only one consumer will restore
      Short replicationFactor = 1;
      NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor).configs(topicConfigs);
      topicList.add(newTopic);
      System.out.println("RocksDBStateStore.createChangelogTopic - creating topic=" + newTopic);
      adminClient.createTopics(topicList);
    } else {
      System.out.println("RocksDBStateStore.createChangelogTopic - creating topic with name='" + topicName + "' exists");
    }
  }

  private KafkaProducer<String, byte[]> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    // props.put(ProducerConfig.BATCH_SIZE_CONFIG, "4"); // in bytes, low because
    // messages are small
    // props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<String, byte[]>(props);
    return kafkaProducer;
  }

  private KafkaConsumer<String, byte[]> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    //props.put(ConsumerConfig.GROUP_ID_CONFIG, name);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // force a read from beginning of topic
    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(props);
    return kafkaConsumer;
  }
}

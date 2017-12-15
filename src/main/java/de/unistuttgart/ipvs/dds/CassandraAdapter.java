package de.unistuttgart.ipvs.dds;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import de.unistuttgart.ipvs.dds.avro.TemperatureData;
import de.unistuttgart.ipvs.dds.avro.TemperatureThresholdExceeded;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CassandraAdapter {
    private final static Logger logger = LogManager.getLogger(CassandraAdapter.class);

    public static void main(String[] args) {
        /* Zookeeper server URLs */
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        /* URL of the Schema Registry */
        final String schemaRegistry = args.length > 1 ? args[1] : "http://localhost:8081";

        /* The Kafka topic from which to read messages */
        final String inputTopic = args.length > 2 ? args[2] : "";
        if (!inputTopic.equals("temperature-data") && !inputTopic.equals("temperature-threshold-events")) {
            logger.error("Input topic must be one of 'temperature-data' or 'temperature-threshold-events'");
            System.exit(1);
        }

        final String cassandraHost = args.length > 3 ? args[3] : "localhost:9042";

        final Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        final Session session = cluster.connect();

        logger.info("Connected to Cassandra cluster named '" + session.getCluster().getClusterName() + "'");

        logger.info("Creating 'dds' keyspace");
        if (!session.execute("CREATE KEYSPACE IF NOT EXISTS dds WITH replication = {'class':'SimpleStrategy','replication_factor':3};").wasApplied()) {
            logger.error("Failed to apply CREATE KEYSPACE (note: check if it actually failed through the Cassandra console)");
            System.exit(1);
        }

        final String createTableQuery;
        if (inputTopic.equals("temperature-data")) {
            createTableQuery = "CREATE TABLE IF NOT EXISTS dds.temperature_data (id text, ts timestamp, temp double, unit text, deltat int, PRIMARY KEY (id, ts));";
        } else {
            createTableQuery = "CREATE TABLE IF NOT EXISTS dds.temperature_threshold_events (id text, ts timestamp, threshold double, avg double, max double, duration int, deltat int, PRIMARY KEY (id, ts));";
        }
        logger.info("Running '" + createTableQuery + "'");
        if (!session.execute(createTableQuery).wasApplied()) {
            logger.error("Failed to apply '" + createTableQuery + "' (note: check it it actually failed through the Cassandra console)");
            System.exit(1);
        }

        /* ID of the consumer group with which to register with Kafka */
        final String groupId = "cassandra-adapter-" + inputTopic;

        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        final SpecificAvroSerde<?> valueSerde;
        if (inputTopic.equals("temperature-data")) {
            valueSerde = CassandraAdapter.<TemperatureData>createSerde(schemaRegistry);
        } else {
            valueSerde = CassandraAdapter.<TemperatureThresholdExceeded>createSerde(schemaRegistry);
        }
        final KafkaConsumer<String, ?> consumer = new KafkaConsumer<>(
                consumerProperties,
                Serdes.String().deserializer(),
                valueSerde.deserializer()
        );
        consumer.subscribe(Collections.singleton(inputTopic));

        final SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            while (true) {
                final ConsumerRecords<String, ?> consumerRecords = consumer.poll(1000);
                if (consumerRecords.count() == 0) {
                    continue;
                }
                consumerRecords.forEach(record -> {
                    logger.info("Received record with key " + record.key());
                    final String query;
                    final long currentTimestamp = System.currentTimeMillis();
                    if (inputTopic.equals("temperature-data")) {
                        final TemperatureData value = (TemperatureData)record.value();
                        // generate cassandra query
                        query = "INSERT INTO dds.temperature_data (id, ts, temp, unit, deltat) VALUES ('" +
                                record.key() + "','" +
                                tsFormat.format(new Date(value.getTimestamp())) + "'," +
                                value.getTemperature().toString() + ",'" +
                                value.getUnit().toString() + "'," +
                                (currentTimestamp - value.getTimestamp()) + ");";
                    } else {
                        final TemperatureThresholdExceeded value = (TemperatureThresholdExceeded)record.value();
                        // generate cassandra query
                        query = "INSERT INTO dds.temperature_threshold_events (id, ts, threshold, avg, max, duration, deltat) VALUES ('" +
                                record.key() + "','" +
                                tsFormat.format(new Date(value.getTimestamp())) + "'," +
                                value.getTemperatureThreshold().toString() + "," +
                                value.getAverageTransgression().toString() + "," +
                                value.getMaxTransgression().toString() + "," +
                                value.getExceededForMs().toString() + "," +
                                (currentTimestamp - value.getTimestamp()) + ");";
                    }
                    // Execute query
                    logger.info("Executing '" + query + "'");
                    if (!session.execute(query).wasApplied()) {
                        logger.warn("Failed to apply '" + query + "'");
                    }
                });
            }
        } catch (Exception e) {
            logger.error("Caught Exception", e);
        } finally {
            consumer.close();
            session.close();
            cluster.close();
            logger.info("Done");
        }
    }

    /**
     * Creates a serialiser/deserialiser for the given type, registering the Avro schema with the schema registry.
     *
     * @param schemaRegistryUrl the schema registry to register the schema with
     * @param <T>               the type for which to create the serialiser/deserialiser
     * @return                  the matching serialiser/deserialiser
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> createSerde(final String schemaRegistryUrl) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl
        );
        serde.configure(serdeConfig, false);
        return serde;
    }
}

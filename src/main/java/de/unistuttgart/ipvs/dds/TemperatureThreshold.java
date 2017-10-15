package de.unistuttgart.ipvs.dds;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import de.unistuttgart.ipvs.dds.avro.TemperatureThresholdAggregate;
import de.unistuttgart.ipvs.dds.avro.TemperatureThresholdExceeded;
import de.unistuttgart.ipvs.dds.avro.TemperatureUnit;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.unistuttgart.ipvs.dds.avro.TemperatureData;

public class TemperatureThreshold {
    private final static Logger logger = LogManager.getLogger(TemperatureThreshold.class);

    public static void main(String[] args) {
        /* Zookeeper server URLs */
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        logger.info("bootstrapServers: " + bootstrapServers);
        /* URL of the Schema Registry */
        final String schemaRegistry = args.length > 1 ? args[1] : "http://localhost:8081";
        logger.info("schemaRegistry: " + schemaRegistry);

        /* The temperature threshold at which threshold events will be triggered */
        final double threshold = args.length > 2 ? Double.parseDouble(args[2]) : 100.0;
        logger.info("temperature threshold: " + threshold);
        /* The temperature unit of the threshold */
        final TemperatureUnit temperatureUnit = args.length > 3 ? TemperatureUnit.valueOf(args[3]) : TemperatureUnit.C;
        logger.info("temperature unit: " + temperatureUnit);
        /* Hysteresis to prevent triggering events over and over again just because of data fluctuation */
        final double hysteresis = args.length > 4 ? Double.parseDouble(args[4]) : 0.5;
        logger.info("hysteresis: " + hysteresis);

        /* Client ID with which to register with Kafka */
        final String applicationId = "temperature-threshold";
        logger.info("Using applicationId " + applicationId);

        /* The Kafka topic from which to read messages */
        final String inputTopic = "temperature-data";
        /* The Kafka topic to which to send messages */
        final String outputTopic = "temperature-threshold-events";

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Disable caching for intermediate aggregation results
        streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Reduce the commit interval for the streams to reduce the number of duplicates when the job crashes.
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final SpecificAvroSerde<TemperatureData> temperatureDataSerde = createSerde(schemaRegistry);
        final SpecificAvroSerde<TemperatureThresholdAggregate> thresholdAggregateSerde = createSerde(schemaRegistry);
        final SpecificAvroSerde<TemperatureThresholdExceeded> thresholdExceededSerde = createSerde(schemaRegistry);

        final TemperatureThresholdAggregate.Builder aggregateBuilder = TemperatureThresholdAggregate.newBuilder();
        aggregateBuilder.setIsExceeding(false);
        aggregateBuilder.setIsFirst(false);
        aggregateBuilder.setFirstTransgression(0);
        aggregateBuilder.setLatestTransgression(0);
        aggregateBuilder.setLatestTransgressionTemperature(0);
        aggregateBuilder.setAverageTransgressionTemperature(0);
        aggregateBuilder.setMaxTransgressionTemperature(0);

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, TemperatureData> input = builder.stream(
                Serdes.String(),
                temperatureDataSerde,
                inputTopic
        );

        input
            .mapValues(convertTo(temperatureUnit))
            .groupByKey(Serdes.String(), temperatureDataSerde)
            .aggregate(aggregateBuilder::build, detectThreshold(threshold, hysteresis), thresholdAggregateSerde)
            .toStream()
            .filter((k, v) -> v.getIsFirst())
            .mapValues(value -> new TemperatureThresholdExceeded(
                    value.getLatestTransgression(),
                    threshold,
                    value.getAverageTransgressionTemperature(),
                    value.getMaxTransgressionTemperature(),
                    value.getLatestTransgression() - value.getFirstTransgression()
            ))
            .to(Serdes.String(), thresholdExceededSerde, outputTopic);

        final KafkaStreams streams = new KafkaStreams(builder, streamsProperties);
        streams.start();
    }

    private static ValueMapper<TemperatureData, TemperatureData> convertTo(TemperatureUnit unit) {
        switch (unit) {
            case C: return (value -> {
                switch (value.getUnit()) {
                    case F:
                        value.setTemperature(value.getTemperature() * 5d / 9d + 32d);
                        break;
                    case K:
                        value.setTemperature(value.getTemperature() + 273.15d);
                        break;
                }
                value.setUnit(TemperatureUnit.C);
                return value;
            });
            case K: return (value -> {
                switch (value.getUnit()) {
                    case C:
                        value.setTemperature(value.getTemperature() - 273.15d);
                        break;
                    case F:
                        value.setTemperature(value.getTemperature() * 9d / 5d - 459.67);
                        break;
                }
                value.setUnit(TemperatureUnit.K);
                return value;
            });
            case F: return (value -> {
                switch (value.getUnit()) {
                    case C:
                        value.setTemperature((value.getTemperature() - 32d) * 5d / 9d);
                        break;
                    case K:
                        value.setTemperature((value.getTemperature() + 459.67d) * 5d / 9d);
                        break;
                }
                value.setUnit(TemperatureUnit.F);
                return value;
            });
            default:
                throw new IllegalArgumentException("Converting to " + unit.name() + " is not yet supported");
        }
    }

    private static Aggregator<String, TemperatureData, TemperatureThresholdAggregate> detectThreshold(
            double temperatureThreshold, double temperatureHysteresis
    ) {
        return (key, value, aggregate) -> {
            aggregate.setIsFirst(false);
            if (aggregate.getIsExceeding()) {
                if (value.getTemperature() < temperatureThreshold - temperatureHysteresis) {
                    logger.debug(key
                            + " no longer exceeding the threshold after "
                            + (value.getTimestamp() - aggregate.getFirstTransgression())
                            + "ms"
                    );
                    aggregate.setIsExceeding(false);
                    aggregate.setIsFirst(true);
                } else {
                    final long firstTransgression = aggregate.getFirstTransgression();
                    final long lastTransgression = aggregate.getLatestTransgression();
                    final long transgressionDuration = value.getTimestamp() - firstTransgression;

                    // Calculate the weighted average of the transgression
                    final double avgSinceLast = (aggregate.getLatestTransgressionTemperature() + value.getTemperature()) / 2;
                    final double avgSinceLastWeighted = avgSinceLast * (value.getTimestamp() - lastTransgression) / transgressionDuration;
                    final double previousAvg = aggregate.getAverageTransgressionTemperature();
                    final double previousAvgWeighted = previousAvg * (lastTransgression - firstTransgression) / transgressionDuration;
                    final double weightedAvg = avgSinceLastWeighted + previousAvgWeighted;

                    aggregate.setAverageTransgressionTemperature(weightedAvg);
                    aggregate.setLatestTransgression(value.getTimestamp());
                    aggregate.setMaxTransgressionTemperature(Math.max(aggregate.getMaxTransgressionTemperature(), value.getTemperature()));
                    aggregate.setLatestTransgression(value.getTimestamp());
                }
            } else if (value.getTemperature() > temperatureThreshold) {
                logger.debug(key
                        + " is exceeding the threshold at "
                        + value.getTemperature()
                        + value.getUnit().toString()
                );
                aggregate.setIsExceeding(true);
                aggregate.setIsFirst(true);
                aggregate.setFirstTransgression(value.getTimestamp());
                aggregate.setLatestTransgression(value.getTimestamp());
                aggregate.setAverageTransgressionTemperature(value.getTemperature());
                aggregate.setMaxTransgressionTemperature(value.getTemperature());
                aggregate.setLatestTransgressionTemperature(value.getTemperature());
            }
            return aggregate;
        };
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

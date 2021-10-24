package com.example.micrometer1_7_4kafkastreamscpuusage;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String INPUT_TOPIC = "foo";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample-app");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 6);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-" + System.currentTimeMillis());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic mySpringKafkaMessageTopic() {
        return TopicBuilder.name(INPUT_TOPIC)
                .partitions(12)
                .replicas(1)
                .build();
    }

    @Bean
    public StreamsBuilderFactoryBeanCustomizer streamsBuilderCustomizer(MeterRegistry meterRegistry) {
        return streamsBuilderFactoryBean ->
                streamsBuilderFactoryBean.setKafkaStreamsCustomizer(
                        kafkaStreams -> new KafkaStreamsMetrics(kafkaStreams).bindTo(meterRegistry)
                );
    }

    @Bean
    public MeterFilter kafkaMetricsFilter() {
        return new MeterFilter() {

            /**
             * Based on the most critical metrics mentioned in "Kafka: The Definitive Guide" (https://www.confluent.io/resources/kafka-the-definitive-guide/)
             */
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                String name = id.getName();
                if (name.startsWith("kafka")) {

                    // time taken for a fetch request (OBS: includes fetch.min.bytes and fetch.max.wait.ms)
                    // if high it indicates network/broker problems
                    if (name.contains("fetch.latency")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // time taken from KafkaProducer.send() until response from broker
                    // if high it indicates network/broker problems
                    if (name.contains("request.latency")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // time taken for a commit request
                    if (name.contains("commit.latency")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // errors from KafkaProducer.send() after max number of retries
                    // should always be zero - if not messages are dropped
                    if (name.contains("record.error")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // lag reported by the consumer for the partition that most behind
                    // offset-lag should ideally be monitored through the consumer-group
                    if (name.contains("records.lag")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // fraction of time I/O threads spent waiting (disk, network, etc)
                    if (name.contains("io.wait")) {
                        return MeterFilterReply.ACCEPT;
                    }

                    // ignore all other kafka-related metrics
                    return MeterFilterReply.DENY;
                }

                // neutral = other filters can accept/deny
                return MeterFilterReply.NEUTRAL;
            }
        };
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream(INPUT_TOPIC);
        // repartition to trigger a lot of task-creation (this would be the equivalent of consuming from multiple topics)
        stream.repartition(Repartitioned.as("foo-repartition-1"))
                .repartition(Repartitioned.as("foo-repartition-2"))
                .repartition(Repartitioned.as("foo-repartition-3"))
                .repartition(Repartitioned.as("foo-repartition-4"))
                .repartition(Repartitioned.as("foo-repartition-5"))
                .toTable(Named.as("foo-table"));
        return stream;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() throws ExecutionException, InterruptedException {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send(INPUT_TOPIC, UUID.randomUUID().toString(), UUID.randomUUID().toString()).get();
        }
        LOGGER.info("Produced som data to input-topic");

        return kafkaTemplate;
    }
}

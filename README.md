Demonstrating high cpu-usage in `KafkaStreamsMetrics` for micrometer 1.7.4

Needs kafka running on localhost:9092 (https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html)

Start two instances of the application: `./mvnw spring-boot:run`

Restart one instance occasionally to trigger rebalances.

`io.micrometer.core.instrument.binder.kafka.KafkaMetrics.registeredMeters` should increase for each scheduled call to `checkAndBindMetrics`
package com.example.micrometer1_7_4kafkastreamscpuusage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
public class Micrometer174KafkaStreamsCpuUsageApplication {

    public static void main(String[] args) {
        SpringApplication.run(Micrometer174KafkaStreamsCpuUsageApplication.class, args);
    }

}

package com.example.demo.kafkastreamsconfig;

import lombok.Getter;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@Getter
public class MyKafkaStreamsConfig {


    @Bean
    public KafkaStreamsConfig kafkaStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ABCDEF");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:30002");

        return new KafkaStreamsConfig(props);
    }




}

package com.example.demo.kafkastreamsconfig;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Properties;

@Data
public class KafkaStreamsConfig {

    private Properties props;

    public KafkaStreamsConfig(Properties props) {
        this.props = props;
    }
}

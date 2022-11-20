package com.example.demo.kafkastreamsconfig;

import com.example.demo.MyController;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaInjectListener {

    private final ObjectProvider<MyController> provider;
    private final KafkaStreams kafkaStreams;

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {

        MyController myController = provider.getObject();

        // set stateListener
        kafkaStreams.setStateListener((newState, oldState) -> {
            log.info("kafka Streams State is changed. {} -> {} ", oldState, newState);
            myController.changeKafkaStreamsState(newState);
        });
        myController.configKafkaStreams(kafkaStreams);

        // kafka streams start
        kafkaStreams.start();
    }



}
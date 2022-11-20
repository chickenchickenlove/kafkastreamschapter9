package com.example.demo.kafkastreamsconfig;

import com.example.demo.CountValueTransformer;
import com.example.demo.domain.StockTransaction;
import com.example.demo.util.GsonDeserializer;
import com.example.demo.util.GsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

@Component
public class KafkaStreamsTopology {

    private final KafkaStreamsConfig kafkaStreamsConfig;

    @Autowired
    public KafkaStreamsTopology(KafkaStreamsConfig kafkaStreamsConfig) {
        this.kafkaStreamsConfig = kafkaStreamsConfig;
    }

    @Bean(name = "kafkaStreamComponent")
    public KafkaStreams kafkaStreams() {

        Properties props = kafkaStreamsConfig.getProps();

        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);


        String storeName = "hello";

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // store 추가
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<String, Integer>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, Serdes.Integer());
        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);


        KStream<String, StockTransaction> stream = streamsBuilder.stream("stock-transactions",
                Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST));

        KStream<String, StockTransaction> stringObjectKStream = stream.transformValues(
                () -> new CountValueTransformer(storeName),
                "hello");


        final Topology topology = streamsBuilder.build();
        return new KafkaStreams(topology, props);
    }



}

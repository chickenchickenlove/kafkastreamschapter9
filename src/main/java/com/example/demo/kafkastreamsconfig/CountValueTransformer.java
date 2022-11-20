package com.example.demo.kafkastreamsconfig;

import com.example.demo.domain.StockTransaction;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.aop.scope.ScopedProxyUtils;

public class CountValueTransformer implements ValueTransformer<StockTransaction, StockTransaction> {


    private KeyValueStore<String, Integer> myStore;
    private String storeName;

    public CountValueTransformer(String storeName) {
        this.storeName = storeName;
    }


    @Override
    public void init(ProcessorContext context) {
        myStore = context.getStateStore(storeName);
    }

    @Override
    public StockTransaction transform(StockTransaction value) {
        String key = value.getSymbol();
        Integer storeValue = myStore.get(key);
        if (storeValue == null) {
            storeValue = 0;
        }
        storeValue ++;
        myStore.put(key, storeValue);
//        System.out.println("my store name = " + myStore.name());
//        System.out.println("storeValue = " + storeValue);
        return value;
    }

    @Override
    public void close() {

    }
}

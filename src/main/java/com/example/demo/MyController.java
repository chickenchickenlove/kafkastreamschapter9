package com.example.demo;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

@RestController
@Slf4j
public class MyController {

    private KafkaStreams.State kafkaStreamsState = KafkaStreams.State.NOT_RUNNING;
    private Gson gson;
    private KafkaStreams kafkaStreams;

    public void configKafkaStreams(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public MyController() {
        this.gson = new Gson();
    }


    @GetMapping("/fetch/{storeName}/{key}")
    public String fetchKeyValue(@PathVariable String key, @PathVariable String storeName) {

        while (!isKafkaStreamsRunning()) {
            log.info("Kafka Streams state is {}", this.kafkaStreamsState);
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {
                log.error("e.getMessage {}", e.getMessage());
            }
        }

        HashMap<String, Integer> ret = new HashMap<>();

        ReadOnlyKeyValueStore<String, Integer> keyValueStore = getKeyValueStore(storeName);
        KeyValueIterator<String, Integer> allKeyValue = keyValueStore.all();

        if (!allKeyValue.hasNext()) {
            return "";
        }

        while (allKeyValue.hasNext()) {
            KeyValue<String, Integer> keyValue = allKeyValue.next();
            if (keyValue.key.equals(key)) {
                ret.put(keyValue.key, keyValue.value);
                return gson.toJson(ret);
            }
        }
        return "";
    }

    @GetMapping("/fetch/{storeName}/ALL")
    public String fetchAllKeyValue(@PathVariable String storeName) {

        while (!isKafkaStreamsRunning()) {
            log.info("Kafka Streams state is {}", this.kafkaStreamsState);
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {
                log.error("e.getMessage {}", e.getMessage());
            }
        }

        HashMap<String, Integer> ret = new HashMap<>();
        ReadOnlyKeyValueStore<String, Integer> keyValueStore = getKeyValueStore(storeName);
        KeyValueIterator<String, Integer> allKeyValue = keyValueStore.all();

        if (!allKeyValue.hasNext()) {
            return "";
        }

        while (allKeyValue.hasNext()) {
            KeyValue<String, Integer> keyValue = allKeyValue.next();
            ret.put(keyValue.key, keyValue.value);
        }
        return gson.toJson(ret);
    }

    @GetMapping("/hello/{storeName}/{key}")
    @ResponseBody
    public String getKeyValue(@PathVariable String key, @PathVariable String storeName) {

        while (!isKafkaStreamsRunning()) {
            log.info("Kafka Streams state is {}", this.kafkaStreamsState);
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {
                log.error("e.getMessage {}", e.getMessage());
            }
        }

        HashMap<String, Integer> ret = new HashMap<>();

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore("hello");
        for (StreamsMetadata streamsMetadatum : streamsMetadata) {
            HostInfo hostInfo = streamsMetadatum.hostInfo();
            String host = hostInfo.host();
            int port = hostInfo.port();
            String url = String.format("http://%s:%s/fetch/%s/%s", host, 8081, storeName,key);

            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String> exchange = restTemplate.exchange(url,HttpMethod.GET,null, new ParameterizedTypeReference<>() {
            });

            if (!StringUtils.hasText(exchange.getBody())) {
                continue;
            }

            HashMap<String, Integer> hashMap = gson.fromJson(exchange.getBody(), HashMap.class);
            Set<Map.Entry<String, Integer>> entries = hashMap.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                if (entry.getKey().equals(key)) {
                    String s = String.valueOf(entry.getValue());
                    int idx = s.indexOf(".");
                    String substring = s.substring(0, idx);
                    ret.put(entry.getKey(), Integer.valueOf(substring));
                }
            }
        }
        return gson.toJson(ret);
    }

    @GetMapping("/hello/{storeName}/ALL")
    @ResponseBody
    public String getAllKeyValue(@PathVariable String storeName) {

        while (!isKafkaStreamsRunning()) {
            log.info("Kafka Streams state is {}", this.kafkaStreamsState);
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {
                log.error("e.getMessage {}", e.getMessage());
            }
        }

        HashMap<String, Integer> ret = new HashMap<>();
        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore("hello");
        for (StreamsMetadata streamsMetadatum : streamsMetadata) {
            HostInfo hostInfo = streamsMetadatum.hostInfo();
            String host = hostInfo.host();
            int port = hostInfo.port();
            String url = String.format("http://%s:%s/fetch/%s/ALL", host, 8081, storeName);
            log.info("url = {}", url);

            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<String> exchange = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<String>() {
                    });

            String body = exchange.getBody();
            if (!StringUtils.hasText(body)) {
                continue;
            }

            HashMap<String, Integer> hashMap = gson.fromJson(exchange.getBody(), HashMap.class);

            Set<Map.Entry<String, Integer>> entries = hashMap.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {

                String s = String.valueOf(entry.getValue());
                int idx = s.indexOf(".");
                String substring = s.substring(0, idx);
                ret.put(entry.getKey(), Integer.valueOf(substring));
            }
        }
        return gson.toJson(ret);
    }

    @GetMapping("/show/{storeName}")
    public String showKafkaStreamMetaData(@PathVariable String storeName) {
        HashMap<String, String> ret = new HashMap<>();
        Collection<StreamsMetadata> hello = kafkaStreams.streamsMetadataForStore(storeName);
        for (StreamsMetadata streamsMetadata : hello) {
            String value = gson.toJson(streamsMetadata.topicPartitions());
            ret.put(streamsMetadata.host(), value);
        }

        return gson.toJson(ret);
    }


    private ReadOnlyKeyValueStore getKeyValueStore(String storeName) {
        ReadOnlyKeyValueStore<Object, Object> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        return store;
    }

    public void changeKafkaStreamsState(KafkaStreams.State state) {
        this.kafkaStreamsState = state;
    }

    private boolean isKafkaStreamsRunning() {
        return RUNNING == this.kafkaStreamsState;
    }



}

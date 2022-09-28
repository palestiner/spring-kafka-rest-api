package com.palestiner.springkafkarestapi.cache;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class BufferedRecords<K, V> {

    private final ConcurrentMap<String, List<ConsumerRecord<K, V>>> cache = new ConcurrentHashMap<>();

    public Optional<String> containsKey(String groupId, String topic) {
        return cache.keySet().stream()
                .filter(key -> key.startsWith(groupId + "-" + topic + "-"))
                .findFirst();
    }

    public void put(String consumerKey, List<ConsumerRecord<K, V>> records) {
        cache.put(consumerKey, records);
    }

    public List<ConsumerRecord<K, V>> get(String consumerKey) {
        return cache.get(consumerKey);
    }
}

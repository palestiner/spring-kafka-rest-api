package com.palestiner.springkafkarestapi.service;

import com.palestiner.springkafkarestapi.cache.BufferedRecords;
import com.palestiner.springkafkarestapi.config.AppConfig;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

@Service
public class DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);

    private final ConsumerFactory<String, String> kafkaConsumerFactory;

    private final AppConfig appConfig;

    private final BufferedRecords<String, String> cache;

    public DefaultConsumer(
            ConsumerFactory<String, String> kafkaConsumerFactory,
            AppConfig appConfig,
            BufferedRecords<String, String> cache
    ) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.appConfig = appConfig;
        this.cache = cache;
    }

    public ResponseEntity<JSONObject> consume(String groupId, String topic) throws ParseException {
        String groupInstanceId = UUID.randomUUID().toString();
        String consumerKey = consumerKey(groupId, topic, groupInstanceId);
        Optional<String> optionalS = cache.containsKey(groupId, topic);
        if (optionalS.isPresent()) {
            consumerKey = optionalS.get();
            groupInstanceId = groupInstanceId(consumerKey, groupId, topic);
        }

        System.out.println("CONSUME consumerKey = " + consumerKey);

        Map<String, Object> configurationProperties = kafkaConsumerFactory.getConfigurationProperties();
        Properties properties = new Properties();
        properties.put("group.instance.id", groupInstanceId);
        properties.putAll(configurationProperties);

        List<ConsumerRecord<String, String>> records = cache.get(consumerKey);
        JSONObject result = parseMessage(appConfig.getMessageFound());
        long offset = -1;
        if (records == null || records.isEmpty()) {
            try (Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer(groupId, "", "", properties)) {
                consumer.subscribe(Pattern.compile(topic));
                records = singleElementPoll(consumer, Duration.ofSeconds(20));
                cache.put(consumerKey, records);
            }
        }
        if (!records.isEmpty()) {
            ConsumerRecord<String, String> record = records.get(0);
            String message = record.value();
            offset = record.offset();
            result.appendField("Payload", parseMessage(message));
        } else {
            result.appendField("StatusMessage", appConfig.getNoMessages().get("status-text"));
        }

        return ResponseEntity.ok()
                .header("kafka-message-id", String.valueOf(offset))
                .body(result);
    }

    private String consumerKey(String groupId, String topic, String groupInstanceId) {
        return String.format("%s-%s-%s", groupId, topic, groupInstanceId);
    }

    private String groupInstanceId(String clientId, String groupId, String topic) {
        return clientId.replaceAll(String.format("%s-%s-", groupId, topic), "");
    }

    private List<ConsumerRecord<String, String>> singleElementPoll(
            Consumer<String, String> consumer,
            Duration duration
    ) {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        consumer.poll(duration).forEach(list::add);
        return list;
    }

    public ResponseEntity<JSONObject> commit(String groupId, String topic, int offset) throws ParseException {
        String groupInstanceId = UUID.randomUUID().toString();
        String consumerKey = consumerKey(groupId, topic, groupInstanceId);
        Optional<String> optionalS = cache.containsKey(groupId, topic);
        if (optionalS.isPresent()) {
            consumerKey = optionalS.get();
            groupInstanceId = groupInstanceId(consumerKey, groupId, topic);
        }

        Map<String, Object> configurationProperties = kafkaConsumerFactory.getConfigurationProperties();
        Properties properties = new Properties();
        properties.put("group.instance.id", groupInstanceId);
        properties.put("max.poll.records", 1);
        properties.putAll(configurationProperties);

        try (Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer(groupId, "", "", properties)) {
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.subscribe(Collections.singletonList(topic));
            Iterator<ConsumerRecord<String, String>> iterator = consumer.poll(Duration.ofSeconds(10)).iterator();
            if (!iterator.hasNext()) {
                throw new RuntimeException("No messages to commit");
            } else if (iterator.next().offset() != offset) {
                throw new IllegalArgumentException("Invalid offset for " + groupId + " consumer.");
            }
            consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1L)));
            List<ConsumerRecord<String, String>> consumerRecords = cache.get(consumerKey);
            if (consumerRecords != null && !consumerRecords.isEmpty()) consumerRecords.remove(0);
            return ResponseEntity.ok(parseMessage(appConfig.getCommitMessage()));
        } catch (KafkaException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit failed, marking pool as invalid. Exception was {}.", e.getMessage());
            }
            throw e;
        }
    }

    private JSONObject parseMessage(String message) throws ParseException {
        return (JSONObject) new JSONParser(-1).parse(message);
    }
}

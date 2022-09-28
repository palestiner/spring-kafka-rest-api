package com.palestiner.springkafkarestapi.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${sasl.mechanism}")
    String saslMechanism;

    @Value("${sasl.jaas.config}")
    String saslJaasConfig;

    @Bean
    public KafkaAdmin admin(
            @Value("${bootstrap.servers}") String bootstrapServers,
            @Value("${security.protocol}") String securityProtocol
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        configs.put("sasl.mechanism", saslMechanism);
        configs.put("sasl.jaas.config", saslJaasConfig);
        configs.put("enable.auto.commit", "false");
        return new KafkaAdmin(configs);
    }

    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory(
            @Value("${key.deserializer}") String keyDeserializer,
            @Value("${value.deserializer}") String valueDeserializer,
            @Value("${max.poll.interval.ms:300000}") String maxPollIntervalMs,
            KafkaAdmin admin
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        configs.put(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        configs.put("sasl.mechanism", saslMechanism);
        configs.put("sasl.jaas.config", saslJaasConfig);
//        configs.put("group.instance.id", "1");
        configs.put("auto.offset.reset", "earliest");
        configs.putAll(admin.getConfigurationProperties());
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        return factory;
    }
}

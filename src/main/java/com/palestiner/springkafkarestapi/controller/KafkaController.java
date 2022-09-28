package com.palestiner.springkafkarestapi.controller;

import com.palestiner.springkafkarestapi.service.DefaultConsumer;
import net.minidev.json.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/topics")
public class KafkaController {

    @Autowired
    private DefaultConsumer consumer;

    @GetMapping("/{topic}/consumers/{groupId}/messages")
    public Mono<ResponseEntity<?>> consume(@PathVariable String topic, @PathVariable String groupId)
            throws ParseException {
        return Mono.just(consumer.consume(groupId, topic));
    }

    @DeleteMapping("/{topic}/consumers/{groupId}/messages/{kafka-message-id}")
    public Mono<ResponseEntity<?>> commit(
            @PathVariable String topic,
            @PathVariable String groupId,
            @PathVariable("kafka-message-id") int offset
    ) throws ParseException {
        return Mono.just(consumer.commit(groupId, topic, offset));
    }
}

package com.palestiner.springkafkarestapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

import java.util.Map;

@ConfigurationProperties(prefix = "app")
public class AppConfig {
    private final Map<String, String> noMessages;
    private final String commitMessage;
    private final String messageFound;

    @ConstructorBinding
    public AppConfig(Map<String, String> noMessages, String commitMessage, String messageFound) {
        this.noMessages = noMessages;
        this.commitMessage = commitMessage;
        this.messageFound = messageFound;
    }

    public Map<String, String> getNoMessages() {
        return noMessages;
    }

    public String getCommitMessage() {
        return commitMessage;
    }

    public String getMessageFound() {
        return messageFound;
    }
}

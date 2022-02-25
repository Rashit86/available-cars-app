package com.pet.project.availablecarsapp;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
@EnableKafkaStreams
public class AvailableCarsApp {

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopic;

    @Value("${available-cars-app.topic-name.user-exceptions}")
    private String userExceptionsTopic;

    public static void main(String[] args) {
        SpringApplication.run(AvailableCarsApp.class, args);
    }

    @Bean
    NewTopic createRentedCarsTopic() {
        return TopicBuilder
                .name(rentedCarsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic createUserExceptionsTopic() {
        return TopicBuilder
                .name(userExceptionsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public MessageSource messageSource() {
        ReloadableResourceBundleMessageSource messageSource
                = new ReloadableResourceBundleMessageSource();

        messageSource.setBasename("classpath:messages");
        messageSource.setDefaultEncoding("UTF-8");
        return messageSource;
    }
}


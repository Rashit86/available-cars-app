package com.pet.project.availablecarsapp;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
@EnableKafkaStreams
public class AvailableCarsApp {

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopic;

    @Value("${available-cars-app.topic-name.user-exception}")
    private String userExceptionTopic;

    public static void main(String[] args) {
        SpringApplication.run(AvailableCarsApp.class, args);
    }

    @Bean
    NewTopic createRentedCarsTopic(){
        return TopicBuilder
                .name(rentedCarsTopic)
                .partitions(1)
                //TODO: если ставить больше 1 реплики, то топик не создается.
                // Видимо потому что у нас создан только один бутстрап сервер
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic createUserExceptionTopic(){
        return TopicBuilder
                .name(userExceptionTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }


}


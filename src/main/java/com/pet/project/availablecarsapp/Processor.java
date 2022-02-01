package com.pet.project.availablecarsapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class Processor {

    @Value("${available-cars-app.topic-name.s3-car}")
    private String s3CarTopic;

    @Value("${available-cars-app.topic-name.car-request}")
    private String carReqTopic;

    @Value("${available-cars-app.topic-name.user-exception}")
    private String userExceptionTopic;

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopic;

    public static final String REQUIRED_ACTION = "requiredAction";
    public static final String RESERVE = "RESERVE";
    public static final String CANCEL_RESERVATION = "CANCEL_RESERVATION";
    public static final String USER_NAME = "userName";
    public static final String CAR_MODEL = "carModel";
    public static final String TIME = "time";
    public static final String IS_RESERVED = "isReserved";
    public static final String RENTER_NAME = "renterName";
    public static final String FOR_KAFKA = "for_kafka";

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    @Autowired
    public void process(StreamsBuilder builder) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStream<String, JsonNode> s3Cars = builder.stream(s3CarTopic, Consumed.with(Serdes.String(), jsonSerde));
        KStream<String, JsonNode> carRequests = builder.stream(carReqTopic, Consumed.with(Serdes.String(), jsonSerde));

        // create the initial json object for car requests
        ObjectNode initialCarRequest = JsonNodeFactory.instance.objectNode();
        initialCarRequest.put(TIME, Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> carRequestTable =
                carRequests
                        .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                        .aggregate(
                                () -> initialCarRequest,
                                (key, value, aggregate) -> getLastRequest(value, aggregate),
                                Materialized.with(Serdes.String(), jsonSerde)
                        );

        ValueJoiner<JsonNode, JsonNode, JsonNode> s3RequestJoiner = (carRequest, s3Car) -> {
            ObjectNode joinedRequest = JsonNodeFactory.instance.objectNode();
            joinedRequest.set(CAR_MODEL, carRequest.get(CAR_MODEL));
            joinedRequest.set(TIME, carRequest.get(TIME));
            joinedRequest.set(USER_NAME, carRequest.get(USER_NAME));
            joinedRequest.set(REQUIRED_ACTION, carRequest.get(REQUIRED_ACTION));
            return joinedRequest;
        };

        KTable<String, JsonNode> joinedCars = carRequestTable.join(s3Cars.toTable(), s3RequestJoiner);

        //дальше эту таблицу нужно сгруппировать и сложить хорошие штуки в один топик
        ObjectNode initialCarState = JsonNodeFactory.instance.objectNode();

        KTable<String, JsonNode> carsTable =
                joinedCars
                        .toStream()
                        .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                        .aggregate(
                                () -> initialCarState,
                                (key, value, aggregate) -> getCarState(value, aggregate),
                                Materialized.with(Serdes.String(), jsonSerde)
                        );

        carsTable.toStream()
                .filter(((key, value) -> value.get(FOR_KAFKA).asBoolean()))
                .to(rentedCarsTopic, Produced.with(Serdes.String(), jsonSerde));

    }

    private static JsonNode getLastRequest(JsonNode value, JsonNode aggregate) {
        ObjectNode lastRequest = JsonNodeFactory.instance.objectNode();
        lastRequest.set(USER_NAME, value.get(USER_NAME));
        lastRequest.set(CAR_MODEL, value.get(CAR_MODEL));

        Long valueEpoch = Instant.parse(value.get(TIME).asText()).toEpochMilli();
        long aggregateEpoch = Instant.parse(aggregate.get(TIME).asText()).toEpochMilli();
        Instant lastRequestInstant = Instant.ofEpochMilli(Math.max(valueEpoch, aggregateEpoch));
        lastRequest.put(TIME, lastRequestInstant.toString());
        lastRequest.set(REQUIRED_ACTION,
                valueEpoch.compareTo(aggregateEpoch) < 0 ? aggregate.get(REQUIRED_ACTION) : value.get(REQUIRED_ACTION));

        return lastRequest;
    }

    private static JsonNode getCarState(JsonNode value, JsonNode aggregate) {
        ObjectNode carState = JsonNodeFactory.instance.objectNode();

        if (aggregate.get(RENTER_NAME) == null || aggregate.get(RENTER_NAME).equals(NullNode.getInstance())) {
            carState.set(CAR_MODEL, value.get(CAR_MODEL));
            carState.set(RENTER_NAME, value.get(USER_NAME));
            carState.put(IS_RESERVED, value.get(REQUIRED_ACTION).asText().equals(RESERVE));
            carState.set(TIME, value.get(TIME));
            carState.put(FOR_KAFKA, true);
        } else {
            if (value.get(USER_NAME).equals(aggregate.get(RENTER_NAME))) {
                createCarState(value, aggregate, carState);
            } else {
                if (!aggregate.get(IS_RESERVED).asBoolean()) {
                    createCarState(value, aggregate, carState);
                } else {
                    logger.info("User ({}) tried to book car ({}) already reserved by another user ({})",
                            value.get(USER_NAME), value.get(CAR_MODEL), aggregate.get(RENTER_NAME));
                    copyAggregatedCar(aggregate, carState);
                }
            }
        }
        return carState;
    }

    private static void createCarState(JsonNode value, JsonNode aggregate, ObjectNode carState) {
        if (aggregate.get(IS_RESERVED).asBoolean()) {
            if (value.get(REQUIRED_ACTION).asText().equals(RESERVE)) {
                logger.info("User ({}) already reserved this car ({})",
                        value.get(USER_NAME), value.get(CAR_MODEL));
                copyAggregatedCar(aggregate, carState);
            } else {
                carState.set(CAR_MODEL, value.get(CAR_MODEL));
                carState.set(RENTER_NAME, null);
                carState.put(IS_RESERVED, false);
                carState.set(TIME, value.get(TIME));
                carState.put(FOR_KAFKA, true);
            }
        } else {
            if (value.get(REQUIRED_ACTION).asText().equals(RESERVE)) {
                carState.set(CAR_MODEL, value.get(CAR_MODEL));
                carState.set(RENTER_NAME, value.get(USER_NAME));
                carState.put(IS_RESERVED, true);
                carState.set(TIME, value.get(TIME));
                carState.put(FOR_KAFKA, true);
            } else {
                logger.info("User ({}) already canceled the reservation if this car ({})",
                        value.get(USER_NAME), value.get(CAR_MODEL));
                copyAggregatedCar(aggregate, carState);
            }
        }
    }

    private static void copyAggregatedCar(JsonNode aggregate, ObjectNode carState) {
        carState.set(CAR_MODEL, aggregate.get(CAR_MODEL));
        carState.set(RENTER_NAME, aggregate.get(RENTER_NAME));
        carState.set(IS_RESERVED, aggregate.get(IS_RESERVED));
        carState.set(TIME, aggregate.get(TIME));
        carState.put(FOR_KAFKA, false);
    }
}

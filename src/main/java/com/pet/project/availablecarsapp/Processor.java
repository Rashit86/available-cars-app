package com.pet.project.availablecarsapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class Processor {

    @Value("${available-cars-app.topic-name.s3-car}")
    private String s3CarTopic;

    @Value("${available-cars-app.topic-name.car-request}")
    private String carReqTopic;

    @Value("${available-cars-app.topic-name.user-exceptions}")
    private String userExceptionsTopic;

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopic;

    private static final LocalDateTime initDateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private static final String DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER = "yyyy-MM-dd'T'HH:mm:ss";
    private static final DateTimeFormatter DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER_FORMATTER = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER);

    public static final String REQUIRED_ACTION = "requiredAction";
    public static final String RESERVE = "RESERVE";
    public static final String USER_NAME = "userName";
    public static final String CAR_MODEL = "carModel";
    public static final String DATE_TIME = "dateTime";
    public static final String IS_RESERVED = "isReserved";
    public static final String RENTER_NAME = "renterName";
    public static final String MESSAGE = "message";

    @Autowired
    public void process(StreamsBuilder builder) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStream<String, JsonNode> s3Cars = builder.stream(s3CarTopic, Consumed.with(Serdes.String(), jsonSerde));
        KStream<String, JsonNode> carRequests = builder.stream(carReqTopic, Consumed.with(Serdes.String(), jsonSerde));

        // create the initial json object for car requests
        ObjectNode initialCarRequest = JsonNodeFactory.instance.objectNode();
        initialCarRequest.put(DATE_TIME, initDateTime.format(DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER_FORMATTER));

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
            joinedRequest.set(DATE_TIME, carRequest.get(DATE_TIME));
            joinedRequest.set(USER_NAME, carRequest.get(USER_NAME));
            joinedRequest.set(REQUIRED_ACTION, carRequest.get(REQUIRED_ACTION));
            return joinedRequest;
        };

        KTable<String, JsonNode> joinedCars = carRequestTable.join(s3Cars.toTable(), s3RequestJoiner);

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
                .filter(((key, value) -> value.get(MESSAGE) == null))
                .to(rentedCarsTopic, Produced.with(Serdes.String(), jsonSerde));

        carsTable.toStream()
                .filter(((key, value) -> value.get(MESSAGE) != null))
                .mapValues(value -> {
                    ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
                    objectNode.set(MESSAGE, value.get(MESSAGE));
                    objectNode.set(DATE_TIME, value.get(DATE_TIME));
                    return (JsonNode) objectNode;
                })
                .to(userExceptionsTopic, Produced.with(Serdes.String(), jsonSerde));

    }

    private static JsonNode getLastRequest(JsonNode value, JsonNode aggregate) {
        ObjectNode lastRequest = JsonNodeFactory.instance.objectNode();
        lastRequest.set(USER_NAME, value.get(USER_NAME));
        lastRequest.set(CAR_MODEL, value.get(CAR_MODEL));

        LocalDateTime valueDateTime = LocalDateTime.parse(value.get(DATE_TIME).asText(), DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER_FORMATTER);
        LocalDateTime aggregateDateTime = LocalDateTime.parse(aggregate.get(DATE_TIME).asText(), DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER_FORMATTER);
        lastRequest.set(DATE_TIME, valueDateTime.isAfter(aggregateDateTime) ? value.get(DATE_TIME) : aggregate.get(DATE_TIME));
        lastRequest.set(REQUIRED_ACTION,
                valueDateTime.isAfter(aggregateDateTime) ? value.get(REQUIRED_ACTION) : aggregate.get(REQUIRED_ACTION));

        return lastRequest;
    }

    private static JsonNode getCarState(JsonNode value, JsonNode aggregate) {
        ObjectNode carState = JsonNodeFactory.instance.objectNode();

        if (aggregate.get(IS_RESERVED) == null || (aggregate.get(IS_RESERVED) != null && !aggregate.get(IS_RESERVED).asBoolean())) {
            if (value.get(REQUIRED_ACTION).asText().equals(RESERVE)) {
                carState.set(CAR_MODEL, value.get(CAR_MODEL));
                carState.set(RENTER_NAME, value.get(USER_NAME));
                carState.put(IS_RESERVED, true);
                carState.set(DATE_TIME, value.get(DATE_TIME));
            } else {
                String message = String.format("User %s tried to cancel reservation of not reserved car %s",
                        value.get(USER_NAME), value.get(CAR_MODEL));
                addMsgToAggCar(aggregate, carState, message);
            }
        } else {
            if (value.get(REQUIRED_ACTION).asText().equals(RESERVE)) {
                if (value.get(USER_NAME).equals(aggregate.get(RENTER_NAME))) {
                    String message = String.format("User %s already reserved this car %s",
                            value.get(USER_NAME), value.get(CAR_MODEL));
                    addMsgToAggCar(aggregate, carState, message);
                } else {
                    String message = String.format("User %s tried to reserve car %s already reserved by another user %s",
                            value.get(USER_NAME), value.get(CAR_MODEL), aggregate.get(RENTER_NAME));
                    addMsgToAggCar(aggregate, carState, message);
                }
            } else {
                if (value.get(USER_NAME).equals(aggregate.get(RENTER_NAME))) {
                    carState.set(CAR_MODEL, value.get(CAR_MODEL));
                    carState.set(RENTER_NAME, null);
                    carState.put(IS_RESERVED, false);
                    carState.set(DATE_TIME, value.get(DATE_TIME));
                } else {
                    String message = String.format("User %s tried to cancel car %s reservation already reserved by another user %s",
                            value.get(USER_NAME), value.get(CAR_MODEL), aggregate.get(RENTER_NAME));
                    addMsgToAggCar(aggregate, carState, message);
                }
            }
        }

        return carState;
    }

    private static void addMsgToAggCar(JsonNode aggregate, ObjectNode carState, String message) {
        ObjectNode dateTimeNow = JsonNodeFactory.instance.objectNode();
        dateTimeNow.put(DATE_TIME, LocalDateTime.now(Clock.systemUTC()).format(DATE_TIME_PATTERN_ISO_WITH_TIME_DELIMITER_FORMATTER));

        carState.set(CAR_MODEL, aggregate.get(CAR_MODEL));
        carState.set(RENTER_NAME, aggregate.get(RENTER_NAME));
        carState.set(IS_RESERVED, aggregate.get(IS_RESERVED));
        carState.set(DATE_TIME, aggregate.get(DATE_TIME) != null ? aggregate.get(DATE_TIME) : dateTimeNow.get(DATE_TIME));
        carState.put(MESSAGE, message);
    }
}

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
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class Processor {

    @Value("${available-cars-app.topic-name.s3-car}")
    private String s3CartTopicName;

    @Value("${available-cars-app.topic-name.car-request}")
    private String carReqTopicName;

    @Value("${available-cars-app.topic-name.available-cars}")
    private String availableCarsTopicName;

    public static final String REQUIRED_ACTION = "requiredAction";
    public static final String CANCEL_RESERVATION = "CANCEL_RESERVATION";
    public static final String USER_NAME = "userName";
    public static final String CAR_MODEL = "carModel";
    public static final String TIME = "time";
    public static final String IS_RESERVED = "isReserved";
    public static final String RENTER_NAME = "renterName";

    @Autowired
    public void process(StreamsBuilder builder) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);



        KStream<String, JsonNode> s3Cars = builder.stream(s3CartTopicName, Consumed.with(Serdes.String(), jsonSerde));

        KStream<String, JsonNode> carRequests = builder.stream(carReqTopicName, Consumed.with(Serdes.String(), jsonSerde));

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

        //TODO: удалить last_requests_topic (добавил для отладки)
        carRequestTable.toStream().to("last_requests_topic", Produced.with(Serdes.String(), jsonSerde));


        ValueJoiner<JsonNode, JsonNode, JsonNode> s3Joiner = (s3Car, carRequest) -> {
            if (carRequest == null || carRequest.get(REQUIRED_ACTION).asText().equals(CANCEL_RESERVATION)) {
                return s3Car;
            } else {
                ObjectNode reserved3Car = JsonNodeFactory.instance.objectNode();
                reserved3Car.set(CAR_MODEL, carRequest.get(CAR_MODEL));
                reserved3Car.put(IS_RESERVED, true);
                reserved3Car.set(RENTER_NAME, carRequest.get(USER_NAME));
                return reserved3Car;
            }
        };

        s3Cars
                .leftJoin(carRequestTable, s3Joiner)
                .to(availableCarsTopicName, Produced.with(Serdes.String(), jsonSerde));

        //TODO: возможна ситуация, когда приходит запрос на резервирование авто, но при этом
        // join-а не произойдет,т.к из s3 отправляются только новые записи, и джойнить не с
        // чем. Значит, надо брать данные из availableCarsTopic и джойнить их с запросами

        //FIXME: Вариант 1. Идет зацикливание. available_cars_topic дополняется новыми записями и опять срабатывает join.
        // Почему? request-ы из таблицы же уже прочитаны, новых request-ов там не появилось

//        KStream<String, JsonNode> fromAvailableCars = builder.stream(availableCarsTopicName, Consumed.with(Serdes.String(), jsonSerde));
//
//        ValueJoiner<JsonNode, JsonNode, JsonNode> availableCarsJoiner = (availableCar, carRequest) -> {
//            ObjectNode topicCar = JsonNodeFactory.instance.objectNode();
//            topicCar.set(CAR_MODEL, carRequest.get(CAR_MODEL));
//
//            if (carRequest.get(REQUIRED_ACTION).asText().equals(CANCEL_RESERVATION)) {
//                topicCar.put(IS_RESERVED, false);
//                topicCar.set(RENTER_NAME, null);
//            } else {
//                topicCar.put(IS_RESERVED, true);
//                topicCar.set(RENTER_NAME, carRequest.get(USER_NAME));
//            }
//
//            return topicCar;
//        };
//
//        fromAvailableCars
//                .join(carRequestTable, availableCarsJoiner)
//                .to(availableCarsTopicName, Produced.with(Serdes.String(), jsonSerde));

        //FIXME: Вариант 2. Джойним стримы. A serializer (org.apache.kafka.common.serialization.ByteArraySerializer)
        // is not compatible to the actual key type (key type: java.lang.String). Change the default Serdes in
        // StreamConfig or provide correct Serdes via method parameters.
//        KStream<String, JsonNode> fromAvailableCars = builder.stream(availableCarsTopicName, Consumed.with(Serdes.String(), jsonSerde));
//        KStream<String, JsonNode> lastCarRequests = builder.stream("last_requests_topic", Consumed.with(Serdes.String(), jsonSerde));
//
//        ValueJoiner<JsonNode, JsonNode, JsonNode> availableCarsJoiner = (availableCar, carRequest) -> {
//            ObjectNode topicCar = JsonNodeFactory.instance.objectNode();
//            topicCar.set(CAR_MODEL, carRequest.get(CAR_MODEL));
//
//            if (carRequest.get(REQUIRED_ACTION).asText().equals(CANCEL_RESERVATION)) {
//                topicCar.put(IS_RESERVED, false);
//                topicCar.set(RENTER_NAME, null);
//            } else {
//                topicCar.put(IS_RESERVED, true);
//                topicCar.set(RENTER_NAME, carRequest.get(USER_NAME));
//            }
//
//            return topicCar;
//        };
//
//        fromAvailableCars
//                .join(lastCarRequests, availableCarsJoiner, JoinWindows.of(Duration.ofMinutes(2)))
//                .to(availableCarsTopicName, Produced.with(Serdes.String(), jsonSerde));

    }

    //TODO: проверить может можно обойтись reduce
    private static JsonNode getLastRequest(JsonNode value, JsonNode aggregate) {
        ObjectNode lastRequest = JsonNodeFactory.instance.objectNode();
        lastRequest.set(USER_NAME, value.get(USER_NAME));
        lastRequest.set(CAR_MODEL, value.get(CAR_MODEL));

        Long valueEpoch = Instant.parse(value.get(TIME).asText()).toEpochMilli();
        Long aggregateEpoch = Instant.parse(aggregate.get(TIME).asText()).toEpochMilli();
        Instant lastRequestInstant = Instant.ofEpochMilli(Math.max(valueEpoch, aggregateEpoch));
        lastRequest.put(TIME, lastRequestInstant.toString());
        lastRequest.set(REQUIRED_ACTION,
                valueEpoch.compareTo(aggregateEpoch) < 0 ? aggregate.get(REQUIRED_ACTION) : value.get(REQUIRED_ACTION));

        return lastRequest;
    }
}

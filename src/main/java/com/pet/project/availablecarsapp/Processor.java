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

import java.time.Instant;

@Component
public class Processor {

    @Value("${available-cars-app.topic-name.s3-car}")
    private String s3CarTopicName;

    @Value("${available-cars-app.topic-name.car-request}")
    private String carReqTopicName;

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopicName;

    public static final String REQUIRED_ACTION = "requiredAction";
    public static final String RESERVE = "RESERVE";
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

        KStream<String, JsonNode> s3Cars = builder.stream(s3CarTopicName, Consumed.with(Serdes.String(), jsonSerde));
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

        ValueJoiner<JsonNode, JsonNode, JsonNode> s3Joiner = (carRequest, s3Car) -> {
            ObjectNode reserved3Car = JsonNodeFactory.instance.objectNode();
            reserved3Car.set(CAR_MODEL, carRequest.get(CAR_MODEL));
            if (carRequest.get(REQUIRED_ACTION).asText().equals(RESERVE)) {
                reserved3Car.put(IS_RESERVED, true);
                reserved3Car.set(RENTER_NAME, carRequest.get(USER_NAME));
            } else {
                reserved3Car.put(IS_RESERVED, false);
                reserved3Car.set(RENTER_NAME, carRequest.get(null));

            }
            return reserved3Car;
        };

        //FIXME: если сделать так, то идет зацикливание, т.к читаю и пишу в один и тот же топик
//        carRequestTable.join(s3Cars.toTable(), s3Joiner)
//                .toStream()
//                .to(s3CarTopicName, Produced.with(Serdes.String(), jsonSerde));

        //FIXME: а так работает. Остальное позже напишу
        carRequestTable.join(s3Cars.toTable(), s3Joiner)
                .toStream()
                .to(rentedCarsTopicName, Produced.with(Serdes.String(), jsonSerde));

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

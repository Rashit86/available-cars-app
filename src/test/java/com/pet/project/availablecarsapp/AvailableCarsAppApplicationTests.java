package com.pet.project.availablecarsapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

@SpringBootTest
class AvailableCarsAppApplicationTests {

    public static final String REQUIRED_ACTION = "requiredAction";
    public static final String RESERVE = "RESERVE";
    public static final String CANCEL_RESERVATION = "CANCEL_RESERVATION";
    public static final String USER_NAME = "userName";
    public static final String CAR_MODEL = "carModel";
    public static final String DATE_TIME = "dateTime";
    public static final String IS_RESERVED = "isReserved";
    public static final String RENTER_NAME = "renterName";
    public static final String MESSAGE = "message";

    @Value("${available-cars-app.topic-name.car-request}")
    private String carRequestTopicName;

    @Value("${available-cars-app.topic-name.s3-car}")
    private String s3CarTopicName;

    @Value("${available-cars-app.topic-name.rented-cars}")
    private String rentedCarsTopicName;

    @Value("${available-cars-app.topic-name.user-exceptions}")
    private String userExceptionsTopicName;

    @Autowired
    private Processor processor;

    @Test
    @DisplayName("Reserve car")
    void reserveCarTest() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> rentedCarsTopic = topologyTestDriver
                    .createOutputTopic(rentedCarsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest = JsonNodeFactory.instance.objectNode();
            carRequest.put(CAR_MODEL, "BMW");
            carRequest.put(DATE_TIME, "2020-03-01T09:00:00");
            carRequest.put(USER_NAME, "john");
            carRequest.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest);

            ObjectNode rentedCar = JsonNodeFactory.instance.objectNode();
            rentedCar.put(CAR_MODEL, "BMW");
            rentedCar.put(RENTER_NAME, "john");
            rentedCar.put(IS_RESERVED, true);
            rentedCar.put(DATE_TIME, "2020-03-01T09:00:00");

            Assertions.assertEquals(rentedCar, rentedCarsTopic.readValue());
        }
    }

    @Test
    @DisplayName("Cancel reservation")
    void cancelReservationTest() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> rentedCarsTopic = topologyTestDriver
                    .createOutputTopic(rentedCarsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest1 = JsonNodeFactory.instance.objectNode();
            carRequest1.put(CAR_MODEL, "BMW");
            carRequest1.put(DATE_TIME, "2020-03-01T09:00:00");
            carRequest1.put(USER_NAME, "john");
            carRequest1.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest1);

            ObjectNode carRequest2 = JsonNodeFactory.instance.objectNode();
            carRequest2.put(CAR_MODEL, "BMW");
            carRequest2.put(DATE_TIME, "2020-03-01T09:05:00");
            carRequest2.put(USER_NAME, "john");
            carRequest2.put(REQUIRED_ACTION, CANCEL_RESERVATION);
            carRequestTopic.pipeInput("BMW", carRequest2);

            ObjectNode rentedCar = JsonNodeFactory.instance.objectNode();
            rentedCar.put(CAR_MODEL, "BMW");
            rentedCar.set(RENTER_NAME, null);
            rentedCar.put(IS_RESERVED, false);
            rentedCar.put(DATE_TIME, "2020-03-01T09:05:00");

            Assertions.assertTrue(rentedCarsTopic.readValuesToList().contains(rentedCar));
        }
    }

    @Test
    @DisplayName("Attempt to reserve car already reserved by current user")
    void alreadyReservedByThisUserTest() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> userExceptionsTopic = topologyTestDriver
                    .createOutputTopic(userExceptionsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest1 = JsonNodeFactory.instance.objectNode();
            carRequest1.put(CAR_MODEL, "BMW");
            carRequest1.put(DATE_TIME, "2020-03-01T09:00:00");
            carRequest1.put(USER_NAME, "john");
            carRequest1.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest1);

            ObjectNode carRequest2 = JsonNodeFactory.instance.objectNode();
            carRequest2.put(CAR_MODEL, "BMW");
            carRequest2.put(DATE_TIME, "2020-03-01T09:05:00");
            carRequest2.put(USER_NAME, "john");
            carRequest2.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest2);

            ObjectNode userException = JsonNodeFactory.instance.objectNode();
            userException.put(MESSAGE, "User \"john\" already reserved this car \"BMW\"");
            userException.put(DATE_TIME, "2020-03-01T09:00:00");

            Assertions.assertEquals(userException, userExceptionsTopic.readValue());
        }
    }

    @Test
    @DisplayName("Attempt to reserve car already reserved by another user")
    void alreadyReservedByAnotherUserTest() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> userExceptionsTopic = topologyTestDriver
                    .createOutputTopic(userExceptionsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest1 = JsonNodeFactory.instance.objectNode();
            carRequest1.put(CAR_MODEL, "BMW");
            carRequest1.put(DATE_TIME, "2020-03-01T09:00:00");
            carRequest1.put(USER_NAME, "john");
            carRequest1.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest1);

            ObjectNode carRequest2 = JsonNodeFactory.instance.objectNode();
            carRequest2.put(CAR_MODEL, "BMW");
            carRequest2.put(DATE_TIME, "2020-03-01T09:05:00");
            carRequest2.put(USER_NAME, "bill");
            carRequest2.put(REQUIRED_ACTION, RESERVE);
            carRequestTopic.pipeInput("BMW", carRequest2);

            ObjectNode userException = JsonNodeFactory.instance.objectNode();
            userException.put(MESSAGE, "User \"bill\" tried to reserve car \"BMW\" already reserved by another user \"john\"");
            userException.put(DATE_TIME, "2020-03-01T09:00:00");

            Assertions.assertEquals(userException, userExceptionsTopic.readValue());
        }
    }

    @Test
    @DisplayName("Attempt to cancel reservation of reserved car by another user")
    void cancelReservationOfAnotherUsersCarTest() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> userExceptionsTopic = topologyTestDriver
                    .createOutputTopic(userExceptionsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest1 = JsonNodeFactory.instance.objectNode();
            carRequest1.put(CAR_MODEL, "BMW");
            carRequest1.put(DATE_TIME, "2020-03-01T09:00:00");
            carRequest1.put(USER_NAME, "john");
            carRequest1.put(REQUIRED_ACTION, "RESERVE");
            carRequestTopic.pipeInput("BMW", carRequest1);

            ObjectNode carRequest2 = JsonNodeFactory.instance.objectNode();
            carRequest2.put(CAR_MODEL, "BMW");
            carRequest2.put(DATE_TIME, "2020-03-01T09:05:00");
            carRequest2.put(USER_NAME, "bill");
            carRequest2.put(REQUIRED_ACTION, CANCEL_RESERVATION);
            carRequestTopic.pipeInput("BMW", carRequest2);

            ObjectNode userException = JsonNodeFactory.instance.objectNode();
            userException.put(MESSAGE, "User \"bill\" tried to cancel car \"BMW\" reservation already reserved by another user \"john\"");
            userException.put(DATE_TIME, "2020-03-01T09:00:00");

            Assertions.assertEquals(userException, userExceptionsTopic.readValue());
        }
    }

    @Test
    @DisplayName("Attempt to cancel reservation of not reserved car")
    void cancelReservationOfFreeCarTest() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        processor.process(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, JsonNode> s3CarTopic = topologyTestDriver
                    .createInputTopic(s3CarTopicName, new StringSerializer(), new JsonSerializer());

            TestInputTopic<String, JsonNode> carRequestTopic = topologyTestDriver
                    .createInputTopic(carRequestTopicName, new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> userExceptionsTopic = topologyTestDriver
                    .createOutputTopic(userExceptionsTopicName, new StringDeserializer(), new JsonDeserializer());

            ObjectNode s3Car = JsonNodeFactory.instance.objectNode();
            s3Car.put(CAR_MODEL, "BMW");
            s3Car.put(IS_RESERVED, false);
            s3Car.set(RENTER_NAME, null);
            s3CarTopic.pipeInput("BMW", s3Car);

            ObjectNode carRequest = JsonNodeFactory.instance.objectNode();
            carRequest.put(CAR_MODEL, "BMW");
            carRequest.put(DATE_TIME, "2020-03-01T09:05:00");
            carRequest.put(USER_NAME, "bill");
            carRequest.put(REQUIRED_ACTION, CANCEL_RESERVATION);
            carRequestTopic.pipeInput("BMW", carRequest);

            ObjectNode userException = JsonNodeFactory.instance.objectNode();
            userException.put(MESSAGE, "User \"bill\" tried to cancel reservation of not reserved car \"BMW\"");

            Assertions.assertEquals(userException.get(MESSAGE), userExceptionsTopic.readValue().get(MESSAGE));
        }
    }
}

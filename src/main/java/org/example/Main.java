package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.Scanner;

@Slf4j
@ComponentScan
@SpringBootConfiguration
public class Main implements ApplicationRunner {
    @Autowired
    UserRepository userRepository;


    public static void main(String[] args) throws PulsarClientException {
        ConfigurableApplicationContext appContext = SpringApplication.run(Main.class, args);
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        String topic = "topic";
        log.info("Build client");
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        log.info("Creating producer");
        Producer<UserDto> producer = client.newProducer(JSONSchema.of(UserDto.class))
                .topic(topic)
                .create();
        log.info("Creating consumer");
        Consumer<UserDto> consumer = client.newConsumer(JSONSchema.of(UserDto.class))
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("schema-sub")
                .subscribe();

        for (int i = 0; i < 100; i++) {
            var userProducer = new UserDto("name" + i, i);
            log.info("Sending user");
            producer.send(userProducer);
            Message<UserDto> message = consumer.receive();
            UserDto userConsumer = message.getValue();
            User user = new User(userConsumer.getName(), userConsumer.getAge());
            userRepository.createUser(user);
            log.info("Create user {}", userConsumer);
        }
    }
}
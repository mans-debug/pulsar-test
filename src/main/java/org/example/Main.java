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

        for (int i = 0; i < 100; i++) {
            var userProducer = new UserDto("name" + i, i);
            log.info("Sending user");
            producer.send(userProducer);
        }
        Thread.sleep(300_000);
    }
}
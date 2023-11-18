package org.example;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.schema.JSONSchema;

public class Main {
    public static void main(String[] args) throws PulsarClientException {
        String topic = "topic";
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        // send with json schema
        Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
                .topic(topic)
                .create();
        User userProducer = new User("Tom", 28);
        producer.send(userProducer);

        // receive with json schema
        Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class))
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("schema-sub")
                .subscribe();
        Message<User> message = consumer.receive();
        User userConsumer = message.getValue();
        assert userConsumer.age == 28 && userConsumer.name.equals("Tom");
    }
}
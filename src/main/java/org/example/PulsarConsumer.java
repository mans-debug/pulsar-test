package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class PulsarConsumer {

    private static final String USER_TOPIC = "user-topic";
    private final UserRepository userRepository;

    @PulsarListener(
            subscriptionName = "user-topic-subscription",
            topics = USER_TOPIC,
            schemaType = SchemaType.JSON
    )
    public void stringTopicListener(User user) {
        log.info("Received String message: {}", user);
        userRepository.createUser(user);
    }
}
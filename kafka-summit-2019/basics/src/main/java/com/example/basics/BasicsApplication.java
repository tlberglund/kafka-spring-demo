package com.example.basics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.IntStream;

@SpringBootApplication
public class BasicsApplication {


	public static void main(String[] args) {
		SpringApplication.run(BasicsApplication.class, args);
	}
}

interface BasicsBindings {
	String MESSAGES = "messages";
}

@Log4j2
@Component
class Producer {

	private final KafkaTemplate<String, Greeting> kafkaTemplate;

	Producer(KafkaTemplate<String, Greeting> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void produce() {

		IntStream
			.range(0, 10)
			.mapToObj(value ->
				this.kafkaTemplate
					.send(BasicsBindings.MESSAGES, UUID.randomUUID().toString(), new Greeting("hello @ " + Instant.now()))
			)
			.forEach(log::info);
	}
}

@Log4j2
@Component
class Consumer {

	@KafkaListener(topics = BasicsBindings.MESSAGES)
	public void consume(Greeting greeting) {
		log.info("new message: " + greeting.toString());
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Greeting {
	private String message;
}
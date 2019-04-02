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

import java.util.UUID;
import java.util.stream.IntStream;

@SpringBootApplication
public class BasicsApplication {

	public static void main(String[] args) {
		SpringApplication.run(BasicsApplication.class, args);
	}

}

@Component
@Log4j2
class Producer {

	private final KafkaTemplate<String, Greeting> template;

	Producer(KafkaTemplate<String, Greeting> template) {
		this.template = template;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void process() throws Exception {

		IntStream
			.range(0, 10)
			.mapToObj(value -> new Greeting("hello @ " + value))
			.map(g -> this.template.send("greetings", "g" + UUID.randomUUID().toString(), g))
			.forEach(log::info);
	}

}

@Log4j2
@Component
class Consumer {

	@KafkaListener(topics = "greetings")
	void onNewGreeting(Greeting greeting) {
		log.info("new greeting: " + greeting.toString());
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Greeting {
	private String message;
}
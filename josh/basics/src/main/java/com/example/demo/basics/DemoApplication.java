package com.example.demo.basics;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
}


@Log4j2
@Component
class Producer {

	private final KafkaTemplate kafkaTemplate;
	private final ObjectMapper objectMapper;

	Producer(KafkaTemplate kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	@SneakyThrows
	private String asJson(Greeting g) {
		return objectMapper.writeValueAsString(g);
	}

	void convertedSend() throws Exception {
		IntStream
			.range(0, 10)
			.mapToObj(value -> kafkaTemplate.send("topic", "key" + value, new Greeting("hello " + value + "!")))
			.forEach(log::info);
	}

	void helpfulSend() throws Exception {
		IntStream
			.range(0, 10)
			.mapToObj(value -> kafkaTemplate.send("topic", "key" + value, asJson(new Greeting("hello " + value + "!"))))
			.forEach(log::info);
	}


	void manualSend() throws Exception {
		IntStream
			.range(0, 10)
			.mapToObj(value -> kafkaTemplate.execute(producer -> {
				Greeting msg = new Greeting("hello " + value + "!");
				String json = asJson(msg);
				ProducerRecord producerRecord = new ProducerRecord<>("topic", "key", json);
				return producer.send(producerRecord);
			}))
			.forEach(log::info);
	}

	@EventListener(ApplicationReadyEvent.class)
	public void go() throws Exception {
//		manualSend();
//		helpfulSend();
		convertedSend();
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Greeting {
	private String message;
}

@Log4j2
@Component
class Consumer {

	@KafkaListener(topics = "topic")
	public void processIncomingValuesPleaseKThanks(ConsumerRecord<String, Greeting> incoming) {
		log.info(
			String.format("new message on key '%s' on topic '%s' with value '%s' ", incoming.key(),
				incoming.topic(), incoming.value()));

	}
}
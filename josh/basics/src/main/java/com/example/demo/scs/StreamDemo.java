package com.example.demo.scs;
/*
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;


@SpringBootApplication
@EnableBinding({Source.class, Sink.class})
public class StreamDemo {

	public static void main(String[] args) {
		SpringApplication.run(StreamDemo.class, args);
	}
}


@Log4j2
@Component
class Producer {

	private final Source source;

	Producer(Source source) {
		this.source = source;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void produce() {
		var output = source.output();
		IntStream
			.range(0, 10)
			.mapToObj(value -> {
				var msg = MessageBuilder
					.withPayload("value" + value)
					.build();
				return output.send(msg);
			})
			.forEach(log::info);
	}
}

@Log4j2
@Component
class Consumer {

	@KafkaListener(topics = "topic")
	public void processIncomingValuesPleaseKThanks(ConsumerRecord<String, String> incoming) {
		log.info(String.format("new message on key '%s' on topic '%s' with value '%s' ", incoming.key(),
			incoming.topic(), incoming.value()));

	}
} */
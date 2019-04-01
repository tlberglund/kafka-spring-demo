package com.example.scs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Log4j2
@SpringBootApplication
@EnableBinding({Sink.class, Source.class})
public class ScsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScsApplication.class, args);
	}

	@StreamListener(Sink.INPUT)
	public void process(Greeting greeting) {
		log.info("new greeting: " + greeting.toString());
	}
}

@Log4j2
@RestController
class Producer {

	private final MessageChannel output;

	Producer(Source source) {
		this.output = source.output();
	}

	@GetMapping("/go/{ctr}")
	boolean go(@PathVariable int ctr) {
		return this.send(ctr);
	}

	boolean send(int ctr) {
		Greeting payload = new Greeting("hello @ " + ctr);
		Message<Greeting> greetingMessage = MessageBuilder
			.withPayload(payload)
			.build();
		return this.output.send(greetingMessage);
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Greeting {
	private String message;
}
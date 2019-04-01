package com.example.loader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

@EnableScheduling
@EnableBinding({Source.class, Sink.class})
@SpringBootApplication
public class LoaderApplication {

	public static void main(String[] args) {
		SpringApplication.run(LoaderApplication.class, args);
	}
}

@Component
class Consumer {

	private final static Logger log = LoggerFactory.getLogger("Consumer");
	@StreamListener(Sink.INPUT)
	public void consume(String json) {
		log.info("new message has arrived: " + json);
	}
}

@Log4j2
@Component
class Producer {

	private final ObjectMapper objectMapper;
	private final RatingService ratingService;
	private final MessageChannel out;

	Producer(ObjectMapper objectMapper,
										RatingService ratingService,
										Source binding) {
		this.objectMapper = objectMapper;
		this.ratingService = ratingService;
		this.out = binding.output();
	}

	@Scheduled(fixedRate = 1_000)
	public void produce() throws Exception {
		var rating = this.ratingService.generateRandomRating(2);
		var json = toJson(rating);
		var msg = MessageBuilder.withPayload(json)
			.setHeader(KafkaHeaders.MESSAGE_KEY, Integer.toString(rating.getId()).getBytes())
			.build();
		out.send(msg);
		Producer.log.info("trying to send " + json);
	}

	@SneakyThrows
	private String toJson(Rating rating) {
		return objectMapper.writeValueAsString(rating);
	}
}


@Log4j2
@Component
class RatingService {

	private final List<Rating> ratings;

	RatingService(ObjectMapper om) throws Exception {
		var cpr = new ClassPathResource("/ratings.json");
		var json = Files.readString(cpr.getFile().toPath());
		var ref = new TypeReference<Collection<Rating>>() {
		};
		this.ratings = om.readValue(json, ref);
		Assert.notNull(this.ratings, "there are no ratings");
	}

	@SneakyThrows
	public Rating generateRandomRating(int stddev) {
		var random = ThreadLocalRandom.current();
		var numberOfTargets = ratings.size();
		var targetIndex = random.nextInt(numberOfTargets);
		var rating = this.ratings.get(targetIndex);
		var randomRating = (random.nextGaussian() * stddev) + rating.getRating();
		var randomRatingRefined = Math.max(Math.min(randomRating, 10), 0);
		return new Rating(ratings.get(targetIndex).getId(), randomRatingRefined);
	}

}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Rating {
	private Integer id;
	private double rating;
}

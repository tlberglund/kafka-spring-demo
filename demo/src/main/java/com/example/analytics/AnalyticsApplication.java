package com.example.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


interface AnalyticsBinding {

    String RAW_RATINGS = "ratings";
    String AVERAGE_RATINGS = "avg-ratings";
    String RAW_MOVIES = "raw-movies";
    String RATED_MOVIES = "rated-movies";
    String MOVIE_TABLE = "movies";

    @Input(RAW_RATINGS)
    KStream<Long, String> ratingsIn();

    @Output(AVERAGE_RATINGS)
    KStream<Long, Double> ratingsOut();

//
//    @Input(RAW_MOVIES)
//    KTable<String, Movie> moviesIn();
}

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }

}


@Component
class PageViewEventSource implements ApplicationRunner {

    //    private final MessageChannel pageViewsOut;
    private final Log log = LogFactory.getLog(getClass());

//    public PageViewEventSource(AnalyticsBinding binding) {
//        this.pageViewsOut = binding.pageViewsOut();
//    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        Runnable runnable = () -> {
//            Message<PageViewEvent> message = MessageBuilder
//                    .withPayload(pageViewEvent)
//                    .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
//                    .build();
//            try {
//                this.pageViewsOut.send(message);
//                log.info("sent " + message.toString());
//            } catch (Exception e) {
//                log.error(e);
//            }
//        };
//        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
    }
}

@Log4j2
@Component
class RatingAverager {
    @StreamListener
    @SendTo(AnalyticsBinding.AVERAGE_RATINGS)
    public KStream<Long, Double> process(@Input(AnalyticsBinding.RAW_RATINGS) KStream<Long, String> ratings) {

        ObjectMapper mapper = new ObjectMapper();
        Serde<Rating> domainEventSerde = new JsonSerde<>( Rating.class, mapper );

        

        input
                .groupBy(
                        (s, domainEvent) -> domainEvent.boardUuid,
                        Serialized.with(null, domainEventSerde))
                .aggregate(
                        String::new,
                        (s, domainEvent, board) -> board.concat(domainEvent.eventType),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("test-events-snapshots").withKeySerde(Serdes.String()).
                                withValueSerde(Serdes.String())
                );
        KGroupedStream<Long, Double> ratingsById = ratings.mapValues(Rating::getRating).groupByKey();

        KTable<Long, Long> ratingCounts = ratingsById.count();

        KTable<Long, Double> ratingSums = ratingsById.reduce((v1, v2) -> v1 + v2);

        KTable<Long, Double> ratedMovies = ratingSums.join(ratingCounts,
                (sum, count) -> sum / count.doubleValue(),
                Materialized.as("average-ratings"));

        return ratingSums.toStream();
    }
}
//
//@Component
//class MovieProcessor {
//    @StreamListener
//    @SendTo(AnalyticsBinding.MOVIE_TABLE)
//    public KTable<Long, Movie> process(@Input(AnalyticsBinding.RATED_MOVIES) KStream<Long, String> rawMovies) {
//        rawMovies.to(AnalyticsBinding.MOVIE_TABLE, Produced.with(Serdes.Long(), movieSerde));
//
//        // Movies table
//        return builder.table("movies",
//                Materialized.<Long, Movie, KeyValueStore<Bytes, byte[]>>as("movies-store")
//                        .withValueSerde(movieSerde)
//                        .withKeySerde(Serdes.Long()));
//
//    }
//}

//
//@Log4j2
//@Component
//class PageCountSink {
//
//    @StreamListener
//    public void pageCount(@Input((AnalyticsBinding.PAGE_COUNT_IN)) KTable<String, Long> counts) {
//        counts
//                .toStream()
//                .foreach((key, value) -> log.info(key + "=" + value));
//    }
//}

/*
@RestController
class CountRestController {

    private final InteractiveQueryService registry;

    CountRestController(InteractiveQueryService registry) {
        this.registry = registry;
    }

    @GetMapping("/counts")
    Map<String, Long> counts() {
        Map<String, Long> counts = new HashMap<>();
        ReadOnlyKeyValueStore<String, Long> queryableStoreType =
                this.registry.getQueryableStore(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, Long> all = queryableStoreType.all();
        while (all.hasNext()) {
            KeyValue<String, Long> value = all.next();
            counts.put(value.key, value.value);
        }
        return counts;
    }
}

*/

@Data
@AllArgsConstructor
@NoArgsConstructor
class Rating {
    private long movieId;
    private double rating;
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class Movie {
    private long movieId;
    private String title;
    private int releaseYear;
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class RatedMovie {
    private long movieId;
    private String title;
    private int releaseYear;
    private double rating;
}
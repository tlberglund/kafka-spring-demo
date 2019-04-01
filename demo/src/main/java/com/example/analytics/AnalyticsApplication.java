package com.example.analytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import io.confluent.demo.Rating;
import io.confluent.demo.Movie;


interface AnalyticsBinding {

    String RAW_RATINGS = "ratings";
    String AVERAGE_RATINGS = "avg-ratings";
    String AVERAGE_TABLE = "avg-ratings-table";
    String MOVIE_TABLE = "movies";
    String RATED_MOVIES = "rated-movies";

    @Input(RAW_RATINGS)
    KStream<Long, Rating> ratingsIn();

    @Output(AVERAGE_RATINGS)
    KStream<Long, Double> ratingsOut();

    @Input(AVERAGE_TABLE)
    KTable<Long, Double> ratingsToTable();

    @Input(MOVIE_TABLE)
    KTable<Long, Movie> moviesIn();

    @Output(RATED_MOVIES)
    KStream<Long, RatedMovie> moviesOut();
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
    public KStream<Long, Double> process(@Input(AnalyticsBinding.RAW_RATINGS) KStream<Long, Rating> ratings)
            throws Exception {

        KGroupedStream<Long, Double> ratingsById = ratings.mapValues(Rating::getRating).groupByKey();

        KTable<Long, Long> ratingCounts = ratingsById.count();

        KTable<Long, Double> ratingSums = ratingsById.reduce((v1, v2) -> v1 + v2,
                Materialized.with(Serdes.Long(), Serdes.Double()));

        KTable<Long, Double> ratedMovies = ratingSums.join(ratingCounts,
                (sum, count) -> sum / count.doubleValue(),
                Materialized.with(Serdes.Long(), Serdes.Double()));

        return ratedMovies.toStream();
    }
}

@Component
class MovieProcessor {
    @StreamListener
    @SendTo(AnalyticsBinding.RATED_MOVIES)
    public KStream<Long, RatedMovie> processAgain(@Input(AnalyticsBinding.MOVIE_TABLE) KTable<Long, Movie> movies,
                                            @Input(AnalyticsBinding.AVERAGE_TABLE) KTable<Long, Double> ratings) {

        ValueJoiner<Movie, Double, RatedMovie> joiner = (movie, rating) ->
                new RatedMovie(movie.getMovieId(),
                        movie.getTitle().toString(),
                        movie.getReleaseYear(),
                        rating);

        return movies.join(ratings, joiner).toStream();
    }
}

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

//@Data
//@AllArgsConstructor
//@NoArgsConstructor
//class Rating {
//    private long movieId;
//    private double rating;
//}


//@Data
//@AllArgsConstructor
//@NoArgsConstructor
//class Movie {
//    private long movieId;
//    private String title;
//    private int releaseYear;
//}
//
//

@Data
@AllArgsConstructor
@NoArgsConstructor
class RatedMovie {
    private long movieId;
    private String title;
    private int releaseYear;
    private double rating;
}
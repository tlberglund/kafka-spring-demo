package com.example.scs;

import io.confluent.demo.Movie;
import io.confluent.demo.Rating;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@EnableBinding(MovieRatingAnalytics.class)
@SpringBootApplication
public class ScsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScsApplication.class, args);
	}
}


interface MovieRatingAnalytics {

	String RATINGS = "ratings";
	String AVG_RATINGS = "avg-ratings";
	String AVG_TABLE = "avg-table";
	String MOVIES = "movies";
	String RATED_MOVIES = "rated-movies";

	@Input(RATINGS)
	KStream<Long, Rating> ratingsIn();

	@Output(AVG_RATINGS)
	KStream<Long, Double> ratingsAverageOut();

	@Input(AVG_TABLE)
	KTable<Long, Double> ratingsTable();

	@Input(MOVIES)
	KTable<Long, Movie> moviesIn();

	@Output(RATED_MOVIES)
	KStream<Long, RatedMovie> ratedMoviesIn();

}


@Component
class RatingsAverager {

	private final Materialized<Long, Double, KeyValueStore<Bytes, byte[]>> materialized =
		Materialized.with(Serdes.Long(), Serdes.Double());

	@StreamListener
	@SendTo(MovieRatingAnalytics.AVG_RATINGS)
	public KStream<Long, Double> averageRatings(@Input(MovieRatingAnalytics.RATINGS) KStream<Long, Rating> incoming) {

		KGroupedStream<Long, Double> ratingsById = incoming
			.mapValues(Rating::getRating)
			.groupByKey();

		KTable<Long, Long> count = ratingsById.count();
		KTable<Long, Double> sum = ratingsById.reduce(Double::sum, materialized);
		KTable<Long, Double> averages = sum.join(count, (sumValue, countValue) -> sumValue / countValue, materialized);
		return averages.toStream();
	}
}

@Component
class MovieProcessor {

	@StreamListener
	@SendTo (MovieRatingAnalytics.RATED_MOVIES)
	public KStream<Long, RatedMovie> processMovies(
		@Input(MovieRatingAnalytics.AVG_TABLE) KTable<Long, Double> ratings,
		@Input(MovieRatingAnalytics.MOVIES) KTable<Long, Movie> movies) {

		ValueJoiner<Movie, Double, RatedMovie> joiner =
			(movie, rating) -> new RatedMovie(movie.getMovieId(),
				movie.getTitle(), movie.getReleaseYear(), rating);

		KTable<Long, RatedMovie> ratedMovies = movies.join(ratings, joiner);

		return ratedMovies.toStream();
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class RatedMovie {
	private long id;
	private String title;
	private int releaseYear;
	private double rating;
}


// rating: { id: name , rating: 1-10}
// average them (stream out)
// turn average into table
// bring movies in from reference data
// join movies to ratings
//
























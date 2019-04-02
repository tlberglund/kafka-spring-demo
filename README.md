# Spring and Kafka Demo 

This is the demo for [Tim Berglund](http://twitter.com/tlberglund) and [Josh Long](http://twitter.com/Starbuxman)'s talk for the Kafka Summit 2019 edition in NYC. 

The code is in major parts the work of Tim Berglund, Soby Chacko, Josh Long, and Viktor Gamov, among others. 

Please feel free to use the code as you like

## Notes on Running the Demos 

You can use kafkacat to tail a topic in Kafka: 

```
kafkacat -b localhost:9092 -C -t rated-movies
```

Go to the `loader` folder and run the following programs, in the following order: 

```
gradlew loadAvroMovies
gradlew streamWithAvroRatingStreamer
```

## Wnat to Play The Home Game?

You can generate new projects on [the Spring Initializr](http://start.spring.io). Choose `Kafka`, `Kafka Streams`, `Cloud Stream`, `Webflux`, `Lombok` and Java >= 11. You would do well to use the [Confluent platform to bootstrap development](https://www.confluent.io/download/).

Add the following to your Maven dependencies:


```
<dependency>
	<groupId>io.confluent</groupId>
	<artifactId>kafka-schema-registry-client</artifactId>
	<version>5.1.0</version>
</dependency>
<dependency>
	<groupId>io.confluent</groupId>
	<artifactId>kafka-avro-serializer</artifactId>
	<version>5.1.0</version>
</dependency>
<dependency>
	<groupId>org.apache.avro</groupId>
	<artifactId>avro</artifactId>
	<version>1.8.2</version>
</dependency>
<dependency>
	<groupId>io.confluent</groupId>
	<artifactId>kafka-streams-avro-serde</artifactId>
	<version>5.1.0</version>
	<exclusions>
		<exclusion>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-log4j12</artifactId>
		</exclusion>
	</exclusions>
</dependency>
```

Add the following to your Maven build plugin:

```
<plugin>
	<groupId>org.apache.avro</groupId>
	<artifactId>avro-maven-plugin</artifactId>
	<version>1.8.2</version>
	<executions>
		<execution>
			<phase>generate-sources</phase>
			<goals>
				<goal>schema</goal>
			</goals>
			<configuration>
				<sourceDirectory>src/main/resources/avro</sourceDirectory>
				<outputDirectory>${project.build.directory}/generated-sources</outputDirectory>
				<stringType>String</stringType>
			</configuration>
		</execution>
	</executions>
</plugin>
```

Add the following to your Maven repositories:

```
<repository>
	<id>confluent</id>
	<url>https://packages.confluent.io/maven/</url>
</repository>
```


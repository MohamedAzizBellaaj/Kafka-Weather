# Kafka-Weather

Reads the weather data using Kafka. Part of our Big Data lab project.



the command to run the consumer in the HADOOP 
``` 
spark-submit  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  --class weather.kafka.StreamWeatherKafka --master local  weather-stream-kafka-spark-1-jar-with-dependencies.jar localhost:9092 Weather-Stream mySparkConsumerGroup   >> out
```


# Kafka-Weather Streaming Instructions

Follow these steps to set up and run the Kafka-Weather streaming application.

1. **Create “Weather Stream” topic:**
    ```
    kafka-topics.sh --create --topic Weather-Stream --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
    ```

2. **Compile WeatherProducer:**
    ```
    javac -cp "$KAFKA_HOME/libs/*":. WeatherProducer.java
    ```

3. **Run WeatherProducer:**
    ```
    java -cp "$KAFKA_HOME/libs/*":. WeatherProducer Weather-Stream
    ```

4. **Run SimpleConsumer:**
    ```
    java -cp "$KAFKA_HOME/libs/*":. SimpleConsumer Weather-Stream
    ```

5. **Compile the project:**
    ```
    mvn clean compile assembly:single
    ```

6. **Copy the jar file to the Hadoop master node:**
    ```
    docker cp target/weather-stream-kafka-spark-1-jar-with-dependencies.jar hadoop-master:/root
    ```
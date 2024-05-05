# Kafka-Weather

Reads the weather data using Kafka. Part of our Big Data lab project.



the command to run the consumer in the HADOOP 
```
spark-submit  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  --class weather.kafka.StreamWeatherKafka --master local  weather-stream-kafka-spark-1-jar-with-dependencies.jar localhost:9092 Weather-Stream mySparkConsumerGroup   >> out
```
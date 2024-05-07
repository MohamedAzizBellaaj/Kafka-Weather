package weather.kafka;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.ForeachFunction;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.ReplaceOptions;

class WeatherData {
    private String capital;
    private Double avgTempC;
    private String date;
    private String country;

    public String getCountry() {
        return this.country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCapital() {
        return capital;
    }

    public void setCapital(String capital) {
        this.capital = capital;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Double getAvgTempC() {
        return avgTempC;
    }

    public void setAvgTempC(Double avgTempC) {
        this.avgTempC = avgTempC;
    }
}

public class StreamWeatherKafka {
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println(
                    "Usage: SparkKafkaWordCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession.builder().appName("SparkKafkaWordCount").getOrCreate();

        Dataset<Row> kafkaSource = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", topics)
                .option("kafka.group.id", groupId).load();

        Dataset<String> lines = kafkaSource.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        Dataset<WeatherData> df = lines.map((MapFunction<String, WeatherData>) x -> {
            String[] parts = x.split(",");
            WeatherData data = new WeatherData();
            data.setCapital(parts[1]);
            data.setAvgTempC(Double.parseDouble(parts[4]));
            data.setDate(parts[2]);
            data.setCountry(parts[0]);
            return data;
        }, Encoders.bean(WeatherData.class));

        df.createOrReplaceTempView("my_table");

        Dataset<Row> maxTemp = spark
                .sql("SELECT MAX(capital) AS capital, MAX(avgTempC) as max_temp, MAX(date) as latest_date FROM my_table GROUP BY capital");

        String mongoUri = "mongodb+srv://admin:1234@cluster0.iunwbji.mongodb.net/";
        String mongoDatabase = "bigdata";
        String mongoCollection = "weather_stream";
        StreamingQuery query = maxTemp
                .writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    MongoClient mongoClient = MongoClients.create(mongoUri);
                    MongoCollection<Document> collection = mongoClient.getDatabase(mongoDatabase)
                            .getCollection(mongoCollection);

                    List<Row> rowList = batchDF.collectAsList();
                    for (Row row : rowList) {
                        String capital = row.getAs("capital");
                        Double max_temp = row.getAs("max_temp");
                        String latest_date = row.getAs("latest_date");
                        String country = row.getAs("country");

                        Document document = new Document();
                        document.append("capital", capital);
                        document.append("max_temp", max_temp);
                        document.append("latest_date", latest_date);
                        document.append("country", country);

                        Bson filter = Filters.eq("capital", capital);
                        ReplaceOptions options = new ReplaceOptions().upsert(true);
                        collection.replaceOne(filter, document, options);
                    }
                    mongoClient.close();
                })
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();
        query.awaitTermination();
    }
}

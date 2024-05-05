package weather.kafka;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.function.ForeachFunction;

class WeatherData {
    private String capital;
    private Double avgTempC;

    public String getCapital() {
        return capital;
    }

    public void setCapital(String capital) {
        this.capital = capital;
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
            return data;
        }, Encoders.bean(WeatherData.class));

        df.createOrReplaceTempView("my_table");

        Dataset<Row> maxTemp = spark
                .sql("SELECT capital, MAX(avgTempC) as max_temp FROM my_table GROUP BY capital");

        // try {
        // FileInputStream serviceAccount = new FileInputStream("bigdata.json");

        // FirebaseOptions options = new FirebaseOptions.Builder()
        // .setCredentials(GoogleCredentials.fromStream(serviceAccount))
        // .setDatabaseUrl("https://bigdata-e23f0-default-rtdb.firebaseio.com")
        // .build();

        // FirebaseApp.initializeApp(options);
        // // Get a reference to the database

        // FirebaseDatabase database = FirebaseDatabase.getInstance();
        // DatabaseReference ref = database.getReference("data");
        // maxTemp.foreach((ForeachFunction<Row>) row -> {
        // Map<String, Object> data = new HashMap<>();
        // data.put("capital", row.getString(0));
        // data.put("max_temp", row.getDouble(1));
        // ref.push().setValueAsync(data);
        // });
        // System.out.println("Data pushed to Firebase");

        // } catch (Exception e) {
        // e.printStackTrace();
        // return;
        // }
        String mongoUri = "mongodb+srv://admin:1234@cluster0.iunwbji.mongodb.net/";
        String mongoDatabase = "bigdata";
        String mongoCollection = "weather";
        StreamingQuery query = maxTemp
                .writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("mongo")
                            .option("uri", mongoUri)
                            .option("database", mongoDatabase)
                            .option("collection", mongoCollection)
                            .mode("append")
                            .save();
                })
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        // format("console")
        // .trigger(Trigger.ProcessingTime("1 second")).start();

        query.awaitTermination();
    }
}

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WeatherProducer {

   public static void main(String[] args) throws Exception {
      if (args.length == 0) {
         System.out.println("Enter the topic name");
         return;
      }

      String topicName = args[0].toString();

      List<String> dataAfter2023 = readCSVDataAfter2023();

      Timer timer = new Timer();
      for (int i = 0; i < dataAfter2023.size(); i++) {
         final int index = i;
         timer.schedule(new TimerTask() {
            @Override
            public void run() {
               sendRecord(topicName, dataAfter2023.get(index));
            }
         }, i * 500);
      }
   }

   private static List<String> readCSVDataAfter2023() throws Exception {
      List<String> data = new ArrayList<>();
      BufferedReader br = new BufferedReader(new FileReader("data_after_2023.csv"));
      String line;
      while ((line = br.readLine()) != null) {
         data.add(line);
      }
      br.close();
      return data;
   }

   // Method to send a record to Kafka
   private static void sendRecord(String topicName, String record) {
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
         producer.send(new ProducerRecord<>(topicName, record));
         System.out.println("Message sent successfully: " + record);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}

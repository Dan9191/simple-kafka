package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        //Logger
        final Logger logger = LoggerFactory.getLogger(Consumer.class);
        //create variables for strings
        final String bootstrapServers = "127.0.0.1:9092";
        final String consumerGroupID = "java-group-consumer";
        //create and populate properties object
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //create consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        //subscribe to topics
        consumer.subscribe(Arrays.asList("sample-topic"));
        //poll and consume records

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                System.out.println("Received new record : \n " +
                      "Key:" + record.key() + ", " +
                      "Value: " + record.value() + ", " +
                      "Topic: " + record.topic() + ", " +
                      "Partition: " + record.partition() + ", " +
                      "Offset: " + record.offset() + "\n");
            }
        }
    }
}

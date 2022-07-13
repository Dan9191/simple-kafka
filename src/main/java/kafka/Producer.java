package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        //create properties object for producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i < 100; i++) {
            //create the ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("sample-topic", "key_" + i, "value_" + i);

            //send data -async
            //producer.send(record);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("\nReceived record metadata. \n "
                                + "Topic: " + metadata.topic() + ", Partition: " + metadata.partition() + " "
                                + "Offset: " + metadata.offset() + " @ Timestamp: " + metadata.timestamp() + "\n");
                    } else {
                        logger.error("Error occurred", exception);
                    }

                }
            });
        }

        //flush and close producer
        producer.flush();
        producer.close();

    }
}

package tech.datarchy.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    final private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";

        // Create producer properties :
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 100; i++) {
            // Create a record
            final String topic = "first_topic";
            final String value = "value " + i;
            final String key = "key " + (i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // Send record
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successful or an exception is throw
                    if (e == null) {
                        // the record was successfully send
                        logger.info("(" + key + ", " + value  + ") ==> " + recordMetadata.partition() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }

                }
            });

            Thread.sleep(100);
        }

        // Flush
        producer.flush();

        // Close and flush :
        producer.close();
    }
}

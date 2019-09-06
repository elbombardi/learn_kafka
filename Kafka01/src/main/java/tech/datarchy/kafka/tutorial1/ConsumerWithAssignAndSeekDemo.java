package tech.datarchy.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithAssignAndSeekDemo {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithAssignAndSeekDemo.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partition));

        // Seek
        consumer.seek(partition, 15);

        // Poll
        int messageCounter = 0;
        while(messageCounter < 5) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                 messageCounter++;
                logger.info("Key : " + record.key() + ", Value " + record.value()
                        + ", Partition: " + record.partition() + ", Offset : " + record.offset());
            }

        }


    }
}

package org.bigdatatechcir.learn_kafka.part3_kafka_consumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
public class OffsetCommitSyncSingle {
    public static final String brokerList = "192.168.241.128:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo4";
    private static AtomicBoolean running = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        int i = 0;
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                    long offset = record.offset();
                    TopicPartition partition =
                            new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections
                            .singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                }
            }

//            TopicPartition tp1 = new TopicPartition(topic, 0);
//            TopicPartition tp2 = new TopicPartition(topic, 1);
//            TopicPartition tp3 = new TopicPartition(topic, 2);
//            TopicPartition tp4 = new TopicPartition(topic, 3);
//            System.out.println(consumer.committed(tp1) + " : " + consumer.position(tp1));
//            System.out.println(consumer.committed(tp2) + " : " + consumer.position(tp2));
//            System.out.println(consumer.committed(tp3) + " : " + consumer.position(tp3));
//            System.out.println(consumer.committed(tp4) + " : " + consumer.position(tp4));
        } finally {
            consumer.close();
        }
    }
}

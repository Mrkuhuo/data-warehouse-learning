package org.bigdatatechcir.learn_kafka.part3_kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerAnalysis_3 {
    public static final String brokerList = "192.168.241.128:9092";
    public static final String topic1 = "topic-demo";
    public static final String topic2 = "topic-demo";

    public static final String topic3 = "part2_kafka_producer";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CompanyDeserailizer.class.getName());

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topicList = new ArrayList<>();
        consumer.subscribe(topicList);

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100000000));
                for (String topic : topicList){
                    for (ConsumerRecord<String, String> record : records){
                        if (record.topic().equals(topic)){
                            System.out.println("topic = " + record.topic()
                                    + ", partition = " + record.partition()
                                    + ", offset = " + record.offset());
                            System.out.println("key = " + record.key()
                                    + ", value = " + record.value());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}

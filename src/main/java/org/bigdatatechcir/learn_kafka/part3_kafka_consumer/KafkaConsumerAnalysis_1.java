package org.bigdatatechcir.learn_kafka.part3_kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class KafkaConsumerAnalysis_1 {
    public static final String brokerList = "192.168.241.128:9092";
    public static final String topic1 = "topic-demo";
    public static final String topic2 = "topic-demo";

    public static final String topic3 = "part2_kafka_producer";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        //         "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        //        "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        //         StringDeserializer.class.getName());
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

        //consumer.subscribe(Arrays.asList(topic1));
        //consumer.subscribe(Arrays.asList(topic2));
        //consumer.subscribe(Pattern.compile("topic-.*"));

        List<TopicPartition> partitionList = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic1);
        if (partitionInfos != null){
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitionList.add(new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition()));
            }
        }
        // consumer.assign(partitionList);
        consumer.unsubscribe();
        consumer.subscribe(new ArrayList<String>());
        consumer.assign(new ArrayList<TopicPartition>());

        //consumer.assign(Arrays.asList(new TopicPartition(topic1, 0)));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100000000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic()
                            + ", partition = " + record.partition()
                            + ", offset = " + record.offset());
                    System.out.println("key = " + record.key()
                            + ", value = " + record.value());
                    //do something to process record.
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}

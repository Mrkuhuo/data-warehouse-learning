package org.bigdatatechcir.learn_kafka.part1_kafka_overwrite;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemo {

    private static final String BOOTSTRAP_SERVERS = "192.168.241.128:9092";
    private static final String TOPIC_NAME = "part1_kafka_overwrite";

    public static void main(String[] args) {
        // 创建 topic
        createTopicIfNotExists();

        // 配置 Kafka 生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建 Kafka 生产者
        Producer<String, String> producer = new KafkaProducer<>(props);

        try  {

            for (int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                // 创建 ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                // 发送消息
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Message sent successfully: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                                metadata.topic(), metadata.partition(), metadata.offset(), key, value);
                    } else {
                        System.err.println("Failed to send message: " + exception.getMessage());
                    }
                }).get();
            }
        }catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            // 关闭生产者
            producer.close();
        }
    }

    private static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 检查 topic 是否存在
            if (!adminClient.listTopics().names().get().contains(TOPIC_NAME)) {
                // 创建 topic
                NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Topic created: " + TOPIC_NAME);
            } else {
                System.out.println("Topic already exists: " + TOPIC_NAME);
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Failed to create topic: " + e.getMessage());
        }
    }
} 
package org.bigdatatechcir.learn_kafka.part2_kafka_producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerSyncDemo {

    private static final String BOOTSTRAP_SERVERS = "192.168.241.128:9092";
    private static final String TOPIC_NAME = "part2_kafka_producer1";

    public static void main(String[] args) {
        // 创建 topic
        createTopicIfNotExists();

        // 配置 Kafka 生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 拦截器配置
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() + "," + ProducerInterceptorPrefixPlus.class.getName());
        // 同步发送配置
        props.put(ProducerConfig.ACKS_CONFIG, "all");  // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3);   // 重试次数
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // 重试间隔

        // 创建 Kafka 生产者
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            // 同步发送消息
            for (int i = 0; i < 10; i++) {
                String key = "sync-key-" + i;
                String value = "sync-value-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                try {
                    // 同步发送并等待结果
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata metadata = future.get();

                    System.out.printf("Sync message sent successfully: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                            metadata.topic(), metadata.partition(), metadata.offset(), key, value);

                    // 模拟业务处理时间
                    Thread.sleep(1000);
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Failed to send message: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            // 关闭生产者
            producer.close();
        }
    }

    private static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        AdminClient adminClient = AdminClient.create(props);
        try {
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
        } finally {
            adminClient.close();
        }
    }
}
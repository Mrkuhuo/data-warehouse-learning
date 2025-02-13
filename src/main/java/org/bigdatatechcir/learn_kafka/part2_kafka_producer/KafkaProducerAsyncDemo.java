package org.bigdatatechcir.learn_kafka.part2_kafka_producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaProducerAsyncDemo {

    private static final String BOOTSTRAP_SERVERS = "192.168.241.128:9092";
    private static final String TOPIC_NAME = "part2_kafka_producer";

    public static void main(String[] args) {
        // 创建 topic
        createTopicIfNotExists();

        // 配置 Kafka 生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 异步发送配置
        props.put(ProducerConfig.ACKS_CONFIG, "1");     // 只需主副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3);    // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);  // 等待时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 缓冲区大小

        // 创建 Kafka 生产者
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            // 异步发送消息
            for (int i = 0; i < 10; i++) {
                String key = "async-key-" + i;
                String value = "async-value-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                
                // 异步发送，使用回调函数处理结果
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            // 发送成功
                            System.out.printf("Async message sent successfully: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                                    metadata.topic(), metadata.partition(), metadata.offset(), key, value);
                        } else {
                            // 发送失败
                            System.err.println("Failed to send message: " + exception.getMessage());
                        }
                    }
                });

                // 模拟业务处理时间
                try {
                    Thread.sleep(100);  // 异步发送使用较短的等待时间
                } catch (InterruptedException e) {
                    System.err.println("Sleep interrupted: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            try {
                // 关闭生产者（带超时等待）
                // 1. flush()：等待所有消息发送完成
                producer.flush();
                System.out.println("Producer flushed successfully");

                // 2. close()：关闭生产者
                // 使用 Duration 指定超时时间
                producer.close(java.time.Duration.ofSeconds(5));  // 等待5秒后强制关闭
                System.out.println("Producer closed successfully");
            } catch (Exception e) {
                System.err.println("Error while closing producer: " + e.getMessage());
            }
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
            try {
                // 关闭 AdminClient（带超时等待）
                adminClient.close(java.time.Duration.ofSeconds(5));
                System.out.println("AdminClient closed successfully");
            } catch (Exception e) {
                System.err.println("Error while closing admin client: " + e.getMessage());
            }
        }
    }
}

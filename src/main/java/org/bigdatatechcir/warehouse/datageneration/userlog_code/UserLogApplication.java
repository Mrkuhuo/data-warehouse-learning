package org.bigdatatechcir.warehouse.datageneration.userlog_code;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.bigdatatechcir.warehouse.datageneration.userlog_code.generator.UserLogGenerator;
import org.bigdatatechcir.warehouse.datageneration.userlog_code.model.UserLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class UserLogApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(UserLogApplication.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${kafka.topic}")
    private String topic;
    
    @Value("${generator.interval}")
    private long interval;
    
    public static void main(String[] args) {
        // 配置ObjectMapper，避免转义字符
        objectMapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, false);
        
        SpringApplication.run(UserLogApplication.class, args);
    }
    
    @Override
    public void run(String... args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("max.block.ms", "3000"); // 设置连接超时时间为3秒
        
        boolean useKafka = true;
        KafkaProducer<String, String> producer = null;
        
        try {
            producer = new KafkaProducer<>(props);
            // 测试Kafka连接
            producer.partitionsFor(topic);
            logger.info("Successfully connected to Kafka at {}", bootstrapServers);
        } catch (Exception e) {
            useKafka = false;
            logger.error("Failed to connect to Kafka: {}. Will print logs locally.", e.getMessage());
        }
        
        try {
            while (true) {
                UserLog log = UserLogGenerator.generateLog();
                String jsonLog = processNestedJson(log);
                
                if (useKafka && producer != null) {
                    try {
                        producer.send(new ProducerRecord<>(topic, jsonLog), (metadata, exception) -> {
                            if (exception != null) {
                                logger.error("Error sending message to Kafka", exception);
                                System.out.println("Generated log (failed to send to Kafka): " + jsonLog);
                            } else {
                                logger.info("Message sent to partition {} with offset {}", 
                                    metadata.partition(), metadata.offset());
                            }
                        }).get(); // 使用get()来确保消息发送成功
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("Failed to send message to Kafka", e);
                        System.out.println("Generated log (failed to send to Kafka): " + jsonLog);
                    }
                } else {
                    // 本地打印日志
                    System.out.println("Generated log (local print mode): " + jsonLog);
                }
                
                Thread.sleep(interval);
            }
        } catch (Exception e) {
            logger.error("Error in log generation", e);
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
    
    /**
     * 处理嵌套的JSON字符串，避免转义
     */
    private String processNestedJson(UserLog log) throws Exception {
        // 先将对象转换为JSON节点
        JsonNode rootNode = objectMapper.valueToTree(log);
        
        // 处理actions字段
        if (rootNode.has("actions") && !rootNode.get("actions").isNull()) {
            String actionsStr = rootNode.get("actions").asText();
            if (actionsStr != null && !actionsStr.isEmpty() && !actionsStr.equals("[]")) {
                // 解析actions字符串为JSON数组
                JsonNode actionsNode = objectMapper.readTree(actionsStr);
                ((ObjectNode) rootNode).set("actions", actionsNode);
            }
        }
        
        // 处理displays字段
        if (rootNode.has("displays") && !rootNode.get("displays").isNull()) {
            String displaysStr = rootNode.get("displays").asText();
            if (displaysStr != null && !displaysStr.isEmpty() && !displaysStr.equals("[]")) {
                // 解析displays字符串为JSON数组
                JsonNode displaysNode = objectMapper.readTree(displaysStr);
                ((ObjectNode) rootNode).set("displays", displaysNode);
            }
        }
        
        // 将处理后的JSON节点转换回字符串
        return objectMapper.writeValueAsString(rootNode);
    }
} 
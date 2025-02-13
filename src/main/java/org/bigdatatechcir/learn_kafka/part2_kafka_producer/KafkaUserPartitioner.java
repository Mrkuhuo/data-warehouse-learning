package org.bigdatatechcir.learn_kafka.part2_kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaUserPartitioner {
    public static final String brokerList = "192.168.241.128:9092";
    public static final String topic = "part2_kafka_producer";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CompanySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                DemoPartitioner.class.getName());

        KafkaProducer<String, Company> producer =
                new KafkaProducer<>(properties);
        Company company = Company.builder().name("hiddenkafka")
                .address("China").build();
        ProducerRecord<String, Company> record =
                new ProducerRecord<>(topic, company);
        producer.send(record).get();
    }
}

package org.bigdatatechcir.learn_kafka.part3_kafka_consumer;

import org.apache.kafka.common.errors.SerializationException;
import org.bigdatatechcir.learn_kafka.part2_kafka_producer.Company;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanyDeserailizer {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return Company.builder().name(name).address(address).build();
    }

    public void close() {
    }

}

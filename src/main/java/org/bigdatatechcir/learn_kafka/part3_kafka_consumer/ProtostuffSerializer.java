package org.bigdatatechcir.learn_kafka.part3_kafka_consumer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Serializer;
import org.bigdatatechcir.learn_kafka.part2_kafka_producer.Company;


import java.util.Map;

public class ProtostuffSerializer implements Serializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        Schema schema = (Schema) RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer =
                LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(data, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }

    public void close() {
    }
}


package org.bigdatatechcir.warehouse.flink.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
/*
 * 1. mvn clean install -DskipTests
 * 2. 上传jar包到flink_home/lib
 * 3. 重启flink集群
 * 4. 创建临时方法  CREATE TEMPORARY FUNCTION json_displays_array_parser AS 'org.bigdatatechcir.warehouse.flink.udf.JsonDisplaysArrayParser';
 * 5. 查询 select json_array_parser(`actions`).`action_id` as action_id, json_array_parser(`actions`).`item` as item,json_array_parser(`actions`).`item_type` as item_type,json_array_parser(`actions`).`ts` as ts from ods.ods_log_inc;
 */

public class JsonDisplaysArrayParser extends ScalarFunction {
    private static final ObjectMapper mapper = new ObjectMapper();
    @DataTypeHint("ROW<display_type STRING, item STRING, item_type STRING, order INT, pos_id INT>")
    public Row eval(String jsonStr) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return new Row(5); // 返回一个空的Row，所有字段为null
        }

        try {
            JsonNode rootNode = mapper.readTree(jsonStr);
            if (!rootNode.isArray() || rootNode.size() != 1) {
                throw new IllegalArgumentException("Expected a single-element JSON array.");
            }
            JsonNode actionNode = rootNode.get(0);

            String displayType = actionNode.has("display_type") ? actionNode.get("display_type").asText() : "";
            String item = actionNode.has("item") ? actionNode.get("item").asText() : "";
            String itemType = actionNode.has("item_type") ? actionNode.get("item_type").asText() : "";
            Integer order = actionNode.has("order") ? actionNode.get("order").asInt() : null;
            Integer posId = actionNode.has("pos_id") ? actionNode.get("pos_id").asInt() : null;

            Row result = new Row(5);
            result.setField(0, displayType);
            result.setField(1, item);
            result.setField(2, itemType);
            result.setField(3, order);
            result.setField(4, posId);

            return result;
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            return null; // 或者返回一个默认的Row
        }
    }

    @DataTypeHint("ROW<action_id STRING, item STRING, item_type STRING, ts BIGINT>")
    public static class ReturnType {
    }
}

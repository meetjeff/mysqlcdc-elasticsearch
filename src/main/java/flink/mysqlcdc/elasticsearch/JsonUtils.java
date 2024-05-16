package flink.mysqlcdc.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Iterator;
import java.util.Map.Entry;

class JsonUtils {
    static String getFirstFieldValue(JsonNode jsonNode) {
        Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();
        if (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            return convertJsonNodeToValue(entry.getValue()).toString();
        }
        return null;
    }

    static Object convertJsonNodeToValue(JsonNode node) {
        if (node.isInt()) {
            return node.intValue();
        } else if (node.isDouble()) {
            return node.doubleValue();
        } else if (node.isBoolean()) {
            return node.booleanValue();
        } else if (node.isTextual()) {
            return node.textValue();
        } else if (node.isNull()) {
            return null;
        } else {
            return node.toString();
        }
    }
}
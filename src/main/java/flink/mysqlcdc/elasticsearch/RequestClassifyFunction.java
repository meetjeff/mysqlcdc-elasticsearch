package flink.mysqlcdc.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import javax.naming.Context;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;

class RequestClassifyFunction extends ProcessFunction<String, Object> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final String restartUrl;
    private final int connectTimeout;
    private final int socketTimeout;

    public RequestClassifyFunction(String restartUrl, int connectTimeout, int socketTimeout) {
        this.restartUrl = restartUrl;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void processElement(String element, Context ctx, Collector<Object> out) throws JsonProcessingException {
        JsonNode rootNode = mapper.readTree(element);

        JsonNode tableChangesNode = mapper
            .readTree(rootNode.path("historyRecord").asText())
            .path("tableChanges");
        for (JsonNode change : tableChangesNode) {
            JsonNode typeNode = change.path("type");
            if (!typeNode.isMissingNode() && "CREATE".equals(typeNode.asText())) {
                RestartService restartService = new RestartService(restartUrl, connectTimeout, socketTimeout);
                restartService.sendRequest();
                return;
            }
        }
        
        String opType = rootNode.path("op").asText();
        JsonNode sourceNode = rootNode.path("source");
        String dbName = sourceNode.path("db").asText();
        String tableName = sourceNode.path("table").asText();
        String indexName = dbName + "_" + tableName;
        switch (opType) {
            case "c":
            case "r":
            case "u": {
                JsonNode dataNode = rootNode.get("after");
                String id = JsonUtils.getFirstFieldValue(dataNode);
                Map<String, Object> jsonMap = new HashMap<>();
                dataNode.fields().forEachRemaining(field -> {
                    jsonMap.put(field.getKey(), JsonUtils.convertJsonNodeToValue(field.getValue()));
                });
                out.collect(new IndexRequest()
                    .index(indexName)
                    .id(id)
                    .source(jsonMap)
                );
                return;
            }
            case "d": {
                String id = JsonUtils.getFirstFieldValue(rootNode.get("before")); 
                out.collect(new DeleteRequest(indexName, id));
                return;
            }
        }
    }
}
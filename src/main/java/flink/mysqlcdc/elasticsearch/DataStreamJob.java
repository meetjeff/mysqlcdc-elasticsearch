package flink.mysqlcdc.elasticsearch;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        String mySqlHostname = params.get("mySqlHostname");
        int mySqlPort = Integer.parseInt(params.get("mySqlPort", "3306"));
        String databaseList = params.get("databaseList");
        String tableList = params.get("tableList", ".*");
        String mySqlusername = params.get("mySqlusername");
        String mySqlpassword = params.get("mySqlpassword");
        String serverId = params.get("serverId", "5401-5404");
        String serverTimeZone = params.get("serverTimeZone", "UTC");

        String esIp = params.get("esIp");
        int esPort = Integer.parseInt(params.get("esPort", "9200"));
        String esHostType = params.get("esHostType", "http");
        String esUsername = params.get("esUsername");
        String esPassword = params.get("esPassword");

        int checkpointing = Integer.parseInt(params.get("checkpointing", "3000"));
        String sourceName = params.get("sourceName", "MySQL Source");
        int sourceParallelism = Integer.parseInt(params.get("sourceParallelism", "1"));
        int sinkParallelism = Integer.parseInt(params.get("sinkParallelism", "1"));
        String jobName = params.get("jobName", "MySQL CDC to ES");

        String restartUrl = params.get("restartUrl");
        int connectTimeout = Integer.parseInt(params.get("connectTimeout", "6000"));
        int socketTimeout = Integer.parseInt(params.get("socketTimeout", "1000"));


        env.enableCheckpointing(checkpointing);

        MySqlSourceBuilder<String> mySqlSourceBuilder = MySqlSource.<String>builder()
            .hostname(mySqlHostname)
            .port(mySqlPort)
            .scanNewlyAddedTableEnabled(true) 
            .databaseList(databaseList) 
            .tableList(tableList) 
            .username(mySqlusername)
            .password(mySqlpassword)
            .serverId(serverId)
            .serverTimeZone(serverTimeZone)
            .deserializer(new JsonDebeziumDeserializationSchema())
            .includeSchemaChanges(true);

        MySqlSource<String> mySqlSource = mySqlSourceBuilder.build();

        ElasticsearchSink<Object> esSink = new Elasticsearch7SinkBuilder<Object>()
            .setBulkFlushMaxActions(1)
            .setHosts(new HttpHost(esIp, esPort, esHostType))
            .setConnectionUsername(esUsername)
            .setConnectionPassword(esPassword)
            .setEmitter(
                (element, context, indexer) -> {
                    if (element instanceof IndexRequest) {
                        indexer.add((IndexRequest) element);
                    } else if (element instanceof DeleteRequest) {
                        indexer.add((DeleteRequest) element);
                    } 
                }
            )
            .build();

        DataStream<String> sourceStream = env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), sourceName)
            .setParallelism(sourceParallelism); 

        DataStream<Object> processedStream = sourceStream
            .process(new RequestClassifyFunction(restartUrl, connectTimeout, socketTimeout));
        
        processedStream
            .sinkTo(esSink)
            .setParallelism(sinkParallelism); 

        env.execute(jobName);
    }
}
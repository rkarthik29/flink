package com.cloudera.example.flink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import com.cloudera.example.flink.udf.MyKeySelector;

/**
 * Hello world!
 *
 */
public class FlinkExample2 {
    public static void main(String[] args) {
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            final OutputTag<String> speedTag = new OutputTag<String>("truck-speed-event") {

                /**
                 * 
                 */
                private static final long serialVersionUID = -4530747042581467360L;
            };
            // set up the execution environment

            // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081, "/Users/knarayanan/Downloads/flink-connector-kafka_2.11-1.9.1.jar",
            // "/Users/knarayanan/Downloads/flink-streaming-java_2.11-1.9.1.jar", "/Users/knarayanan/Downloads/flink-connector-kafka-base_2.11-1.9.1.jar",
            // "/Users/knarayanan/.m2/repository/org/apache/kafka/kafka-clients/2.2.0/kafka-clients-2.2.0.jar", "/Users/knarayanan/eclipse-workspace/flink/target/flink-0.0.1-SNAPSHOT.jar",
            // "/Users/knarayanan/Downloads/flink-table-planner-blink_2.11-1.9.1.jar", "/Users/knarayanan/Downloads/flink-table-planner_2.11-1.9.1.jar");

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            for (String catalog : tEnv.listCatalogs())
                System.out.println(catalog);
            for (String database : tEnv.listDatabases())
                System.out.println(database);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "olympus-11.field.hortonworks.com:6667");
            // only required for Kafka 0.8
            properties.setProperty("zookeeper.connect", "olympus-1.field.hortonworks.com:2181");
            properties.setProperty("security.protocol", "SASL_PLAINTEXT");
            properties.setProperty("sasl.kerberos.service.name", "kafka");
            properties.setProperty("group.id", "flink-test-local");

            DataStreamSink<String> event = env.addSource(new FlinkKafkaConsumer<String>("violations_karthik", new SimpleStringSchema(), properties)).print();
            final KeySelector<String, Integer> keySelector = new MyKeySelector();
            // make parameters available in the web interface
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
            for (String table : tEnv.listTables())
                System.out.println(table);
            env.execute("Streaming WordCount");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}

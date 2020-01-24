package com.cloudera.example.flink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.cloudera.example.flink.udf.MyJoinFunction;
import com.cloudera.example.flink.udf.MyKeySelector;

/**
 * Hello world!
 *
 */
public class FlinkExample_Tuple {
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
            final KeySelector<String, Integer> keySelector = new MyKeySelector();
            // make parameters available in the web interface
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "olympus-11.field.hortonworks.com:6667");
            // only required for Kafka 0.8
            properties.setProperty("zookeeper.connect", "olympus-1.field.hortonworks.com:2181");
            properties.setProperty("security.protocol", "SASL_PLAINTEXT");
            properties.setProperty("sasl.kerberos.service.name", "kafka");
            properties.setProperty("group.id", "flink-test");
            // Create a geoEvent and speedEvent data split , using the sideoutput option.
            // once i have two streams , i can join them to get a joinedstream with both geoEvents and Speeds
            SingleOutputStreamOperator<String> event = env.addSource(new FlinkKafkaConsumer<String>("gateway-west-raw-sensors", new SimpleStringSchema(), properties))
                    .process(new ProcessFunction<String, String>() {

                        /**
                         * 
                         */
                        private static final long serialVersionUID = 3608344144056999350L;

                        @Override
                        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                            // TODO Auto-generated method stub
                            String type = value.split("\\|")[2];
                            if ("truck_geo_event".equals(type)) {
                                out.collect(value);
                            } else {
                                // emit data to side output
                                ctx.output(speedTag, value);
                            }

                        }

                    });

            // create a keyed stream, by extracting the driver id and route id as a key from the events.
            KeyedStream<String, Integer> geoEvent = event.keyBy(keySelector);
            KeyedStream<String, Integer> speedEvent = event.getSideOutput(speedTag).keyBy(keySelector);
            // join the two events to get a composite event. I am doing a tumbling window join based on ingesttime
            // implemented a JoinFunction to the actual join.
            // For now i am just printing to console.
            DataStream<Tuple9<String, String, String, String, String, String, String, String, Integer>> violationData = geoEvent.join(speedEvent).where(keySelector, TypeInformation.of(Integer.class))
                    .equalTo(keySelector, TypeInformation.of(Integer.class)).window(TumblingEventTimeWindows.of(Time.seconds(1))).apply(new MyJoinFunction());

            // FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
            // "violations_karthik", new SimpleStringSchema(), properties);
            // violationData.addSink(producer);
            tEnv.registerDataStream("violations", violationData, "eventime,timestamp,id,name,routeid,route,violation,truckid,speed");
            for (String table : tEnv.listTables())
                System.out.println(table);
            env.execute("Kafka Streamin Join");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}

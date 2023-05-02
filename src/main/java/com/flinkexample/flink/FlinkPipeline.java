package com.flinkexample.flink;

import static com.flinkexample.flink.sourceandsink.Producers.createStringProducerForTopic2;
import static com.flinkexample.flink.sourceandsink.Sources.createStringConsumerForTopic2;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flinkexample.flink.operator.WordsCapitalizer;

public class FlinkPipeline {

    public static void capitalize() throws Exception {
    	
    	PropertiesConfiguration config = new PropertiesConfiguration();
    	config.load("application.properties");
    	
//    	config.load(
//    	FlinkPipeline.class.getResourceAsStream ("./application.properties")
//    	);
    	
        String inputTopic = "flink_input";
        String outputTopic = "flink_output";
        String consumerGroup = "flinkexample";
        String address = "localhost:49092";

//        String inputTopic = config.getString("inputTopic");
//        String outputTopic = config.getString("outputTopic");
//        String consumerGroup = config.getString("consumerGroup");
//        String address = config.getString("address");
//        
//        
//        System.out.println("inputTopic -"+inputTopic);
//        System.out.println("outputTopic -"+outputTopic);
//        System.out.println("consumerGroup -"+consumerGroup);
//        System.out.println("address -"+address);

        
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        
        KafkaSource<String> flinkKafkaConsumer2 = createStringConsumerForTopic2(inputTopic, address, consumerGroup);
        
        DataStream<String> stringInputStream =environment.fromSource(flinkKafkaConsumer2, WatermarkStrategy.noWatermarks(), "kafkasource");
        
       

         KafkaSink<String> flinkKafkaProducer2= createStringProducerForTopic2(outputTopic, address);

        stringInputStream.map(new WordsCapitalizer()).sinkTo(flinkKafkaProducer2);
    
        environment.execute();
    }



    public static void main(String[] args) throws Exception {
    	capitalize();
    }

}

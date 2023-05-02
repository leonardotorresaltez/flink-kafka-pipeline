package com.flinkexample.flink.sourceandsink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producers {


    
    public static KafkaSink<String> createStringProducerForTopic2(String topic, String kafkaAddress) {
   	 return KafkaSink
   		     .<String>builder()
   		     .setBootstrapServers(kafkaAddress)
   		     
   		     .setRecordSerializer(KafkaRecordSerializationSchema
   		    		 						.builder()
   		    		 						.setTopic(topic)
   		    		 						.setKafkaValueSerializer(StringSerializer.class)
   		     								.build())
   		     .build(); 
    }


}

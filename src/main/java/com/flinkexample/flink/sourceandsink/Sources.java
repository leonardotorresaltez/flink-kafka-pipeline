package com.flinkexample.flink.sourceandsink;

import java.util.Arrays;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Sources {

    
    
    public static KafkaSource<String> createStringConsumerForTopic2(String topic, String kafkaAddress, String kafkaGroup) {
    	 return KafkaSource
    		     .<String>builder()
    		     .setBootstrapServers(kafkaAddress)
    		     .setGroupId(kafkaGroup)
    		     .setTopics(Arrays.asList(topic))
    		     .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
    		     .setStartingOffsets(OffsetsInitializer.earliest())
    		     .build();    	
    }

}

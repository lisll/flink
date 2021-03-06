package com.dingli.flink.stream;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.dingli.flink.stream.sink.MySQLSink;

public class FlinkStreamToMysql {

	public static void main(String[] args) throws Exception{
		
		//1.create env
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	    env.enableCheckpointing(1000);
	    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
	    
	    // kafka params
		Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "172.16.41.100:9092,172.16.41.178:9092");   
	    properties.setProperty("group.id", "flumeid_http");
	    properties.setProperty("enable.auto.commit", "false");
	    properties.setProperty("sasl.kerberos.service.name", "kafka");
	    properties.setProperty("security.protocol", "SASL_PLAINTEXT");	
	    
	    // create a Kafka streaming source consumer for Kafka
	    FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("http514", new SimpleStringSchema(), properties);
	    //myConsumer.setStartFromLatest();       // 最新记录开始

	    //2.business logic
		DataStreamSource<String> dataStreamSource = env.addSource(myConsumer);
		
		DataStream<Tuple2<String, Long>> resultStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> map(String value) throws Exception {
				String[] line = value.split("\\|");
				String msisdn = line[87]; // u64msisdn
				String ultraffic = line[38]; // u32ultraffic
				String dltraffic = line[39]; // u32dltraffic
				Long totalTraffic = Long.valueOf(ultraffic)  +  Long.valueOf(dltraffic);
				return new Tuple2<String, Long>(msisdn, totalTraffic);	      
			}
		}).keyBy(0).timeWindow(Time.minutes(5), Time.minutes(1)).sum(1);	
		
		resultStream.addSink(new MySQLSink());
		
		env.execute("Window Stream save mysql");
	}
}

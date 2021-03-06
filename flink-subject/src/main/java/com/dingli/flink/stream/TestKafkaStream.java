/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dingli.flink.stream;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

/**
 * A simple example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, parse them into a POJO type {@link KafkaEvent}, group by some key, and finally
 * perform a rolling addition on each key for which the results are written back to another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition
 * watermarks directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that
 * the String messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage:
 * 	--input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class TestKafkaStream {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// parse input arguments
		//final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		//1.create env
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	    env.enableCheckpointing(1000);
	    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);		

	    // kafka params
		Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "172.16.41.100:9092,172.16.41.178:9092");

	    //properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    //properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");	    
	    properties.setProperty("group.id", "flumeid_http");
	    properties.setProperty("enable.auto.commit", "false");
	    //properties.setProperty("auto.offset.reset", "largest"); //earliest
	    //properties.setProperty("session.timeout.ms", "60000");
	    //properties.setProperty("max.poll.interval.ms", "10000");
	    //properties.setProperty("auto.commit.interval.ms", "1000");
	    properties.setProperty("sasl.kerberos.service.name", "kafka");
	    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
	    													
	    // create a Kafka streaming source consumer for Kafka
	    FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("http514", new SimpleStringSchema(), properties);
	    //myConsumer.setStartFromLatest();       // 最新记录开始

	    //2.business logic
		DataStreamSource<String> dataStreamSource = env.addSource(myConsumer);

		/**
		 //2.
		    // business logic
		    val resultStream = originalStream.map(x =>{
		      //lteu1_http_zc:..|..|..|..|..|..|..|..
		      var line = x.split("\\|")
		      var citycode= line(91) 	//u16citycode
		      var msisdn = line(87) 	//u64msisdn
		      var apptype = line(28) 	//u16apptype
		      var apptypewhole = line(27) //u16apptypewhole
		      var ultraffic = line(38) //u32ultraffic
		      var dltraffic = line(39) //u32dltraffic
		      var begintime = line(92) //u32begintime
		      var xdr_msg = citycode+ "|" + msisdn + "|" + apptype + "|" + apptypewhole + "|" + ultraffic + "|" + dltraffic + "|" + begintime
		      xdr_msg
		    })
		 */
		DataStream<String> text = dataStreamSource.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				String[] line = value.split("\\|");
				String citycode = line[91]; // u16citycode
				String msisdn = line[87]; // u64msisdn
				String apptype = line[28]; // u16apptype
				String apptypewhole = line[27]; // u16apptypewhole
				String ultraffic = line[38]; // u32ultraffic
				String dltraffic = line[39]; // u32dltraffic
				String begintime = line[92]; // u32begintime
				String xdr_msg = citycode + "|" + msisdn + "|"
						+ apptype + "|" + apptypewhole + "|"
						+ ultraffic + "|" + dltraffic + "|" + begintime;
	
				return xdr_msg;
			}
		});
		
		//save
		text.writeAsText("hdfs://nameservice1/tmp/test/flink_out2", WriteMode.OVERWRITE);//

		//execute
		env.execute("Kafka Example");
	}

}

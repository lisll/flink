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

import java.time.ZoneId;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
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
public class FlinkKafkaStream {

	/**
	 * /opt/cloudera/flink-1.7.1/bin/flink run -m yarn-cluster -yn 2 -yjm 1024 -ytm 1024 FlinkKafkaStream.jar
	 */
	public static void main(String[] args) throws Exception {
		// parse input arguments
		//final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		//StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "172.16.41.100:9092,172.16.41.178:9092");

	    //properties.setProperty("zookeeper.connect", "172.16.41.169:2181,172.16.41.166:2181,172.16.41.100:2181");
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
	    													

	    FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("http514", new SimpleStringSchema(), properties);
	    myConsumer.setStartFromLatest();       // 最新记录开始

		DataStreamSource<String> dataStreamSource = env.addSource(myConsumer);

		//DataStream<String> stream = env.addSource(myConsumer).print();
		//dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台
		// 方式1：将数据导入Hadoop的文件夹
		//recordData.writeAsText("hdfs://hadoop:9000/flink/");
		// 方式2：将数据导入Hadoop的文件夹
		BucketingSink<String> hadoopSink = new BucketingSink<String>("hdfs://nameservice1/tmp/test/flink_out/");
		// 使用东八区时间格式"yyyyMMddHH"命名存储区
		hadoopSink.setBucketer(new DateTimeBucketer<String>("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));
		// 下述两种条件满足其一时，创建新的块文件
		// 条件1.设置块大小为100MB
		hadoopSink.setBatchSize(1024 * 1024 * 100);
		// 条件2.设置时间间隔20min
		hadoopSink.setBatchRolloverInterval(20 * 60 * 1000);
		// 设置块文件前缀
		hadoopSink.setPendingPrefix("");
		// 设置块文件后缀
		hadoopSink.setPendingSuffix("");
		// 设置运行中的文件前缀
		hadoopSink.setInProgressPrefix(".");
		// 添加Hadoop-Sink,处理相应逻辑

		//dataStreamSource.setParallelism(8).writeAsText("hdfs://nameservice1/tmp/test/flink_out", WriteMode.OVERWRITE);//
		 //存到hdfs
		dataStreamSource.addSink(hadoopSink);
		
		env.execute("Kafka Example");
	}

}

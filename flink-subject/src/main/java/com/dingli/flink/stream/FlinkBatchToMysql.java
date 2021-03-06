package com.dingli.flink.stream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dingli.flink.stream.sink.MySQLSinkLocal;


public class FlinkBatchToMysql {

	public static void main(String[] args) throws Exception{
		
		//1.create env
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<String> text1 = env.readTextFile("F:\\Flink_learn\\src_data\\lteu1_http_201812251259_25_npssig132_8150200127858432.DAT.gz");
				
		SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = text1.map(new RichMapFunction<String, Tuple2<String, Long>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Long> map(String value) throws Exception {
				String[] line = value.split("\\|");
				String msisdn = line[87]; // u64msisdn
				String ultraffic = line[38]; // u32ultraffic
				String dltraffic = line[39]; // u32dltraffic
				Long totalTraffic = Long.valueOf(ultraffic)  +  Long.valueOf(dltraffic);
				return new Tuple2<String, Long>(msisdn, totalTraffic);	      
			}
		});
		
		resultStream.addSink(new MySQLSinkLocal());
		
		env.execute("Window Stream save mysql");
	}
}

package com.dingli.flink.learn.chapter05;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		DataStream<TemperatureEvent> inputEventStream = env.addSource(
				new FlinkKafkaConsumer<TemperatureEvent>("test", new EventDeserializationSchema(), properties));

		Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent> begin("first")
				.subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(
							TemperatureEvent value,
							org.apache.flink.cep.pattern.conditions.IterativeCondition.Context<TemperatureEvent> arg1)
							throws Exception {
						if (value.getTemperature() >= 26.0) {
							return true;
						}
						return false;
					}
				}).within(Time.seconds(10));

		DataStream<Alert> patternStream = CEP.pattern(inputEventStream, warningPattern)
				.select(new PatternSelectFunction<TemperatureEvent, Alert>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Alert select(Map<String, List<TemperatureEvent>> event)
							throws Exception {
						return new Alert("Temperature Rise Detected:" + event.get("first")
								+ " on machine name:" + event.get("first"));
					}

				});

		patternStream.print();
		env.execute("CEP on Temperature Sensor");
	}
}

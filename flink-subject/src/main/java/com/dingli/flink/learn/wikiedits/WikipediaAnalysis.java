package com.dingli.flink.learn.wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


public class WikipediaAnalysis {
    @SuppressWarnings("serial")
	public static void main(String[] args) throws Exception{
        //1.获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.为环境信息添加WikipediaEditsSource源
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        //3.根据事件中的用户名为key来区分数据流
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                        return wikipediaEditEvent.getUser();
                    }
                });

        
        //(这边聚合函数使用了Aggregation函数，替换了原先的fold函数(提示为deprecated))
        DataStream<Tuple2<String, Integer>> result = keyedEdits
                //4.设置窗口时间为5s
                .timeWindow(Time.seconds(5))
                //5.聚合当前窗口中相同用户名的事件，最终返回一个tuple2<user，累加的ByteDiff>
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return new Tuple2<>("",0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(WikipediaEditEvent value, Tuple2<String, Integer> accumulator) {
                        return new Tuple2<>(value.getUser(), value.getByteDiff()+accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2<>(a.f0+b.f0, a.f1+b.f1);
                    }
                });

        //6.把tuple2映射为string
        result.map(new MapFunction<Tuple2<String,Integer>, String>() {

            @Override
            public String map(Tuple2<String, Integer> stringLongTuple2) throws Exception {
                return stringLongTuple2.toString();
            }
            
        }).print();
        
        //7.sink数据到kafka，topic为wiki-result
        //.addSink(new FlinkKafkaConsumer<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        //8.执行操作
        env.execute();

    }
}
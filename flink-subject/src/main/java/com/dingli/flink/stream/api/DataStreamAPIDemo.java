package com.dingli.flink.stream.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class DataStreamAPIDemo {

	@SuppressWarnings({ "null", "serial" })
	public static void main(String[] args) throws Exception {
		
		
		DataStream<Integer> dataStream = null;
		
		//1.Map
		//消费一个元素并产出一个元素
		//参数 MapFunction
		//返回 DataStream
		dataStream.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return 2 * value;
			}
		});
		//2.FlatMap
		//消费一个元素并产生零到多个元素
		//参数 FlatMapFunction
		//返回 DataStream
		DataStream<String> dataStream2 = null;
		dataStream2.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
		        for(String word: value.split(" ")){
		            out.collect(word);
		        }
			}
		});
		
		//3.Filter
		//根据FliterFunction返回的布尔值来判断是否保留元素，true为保留，false则丢弃
		//参数 FilterFunction
		//返回 DataStream
		dataStream.filter(new FilterFunction<Integer>() {
		    @Override
		    public boolean filter(Integer value) throws Exception {
		        return value != 0;
		    }
		});	
		
		//4.Filter
		//根据指定的Key将元素发送到不同的分区，相同的Key会被分到一个分区（这里分区指的就是下游算子多个并行的节点的其中一个）。keyBy()是通过哈希来分区的。
		//只能使用KeyedState（Flink做备份和容错的状态）
		//参数 String，tuple的索引，覆盖了hashCode方法的POJO，不能使数组
		//返回KeyedStream
		dataStream.keyBy("someKey");// Key by field "someKey"
		dataStream.keyBy(0);// Key by the first element of a Tuple
		
		//5.WindowAll
		//将元素按照某种特性聚集在一起（如时间：滑动窗口，翻转窗口，会话窗口，又如出现次数：计数窗口）
		//参数 WindowAssigner
		//返回 AllWindowedStream
		dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
		
		//6.Union
		//将两个或多个datastream合并，创造一个新的流包含这些datastream的所有元素
		//参数DataStream（一个或多个）
		//返回UnionStream
		//dataStream.union(otherStream1, otherStream2, ...);
		
		//7.Join
		//将两个DataStream按照key和window join在一起
		//参数：1. KeySelector1 2. KeySelector2 3. DataStream 4. WindowAssigner 5. JoinFunction/FlatJoinFunction
		//Transformation：1. 调用join方法后生成JoinedStream，JoinedStream保存了两个input  2. 调用where方法生成一个内部类Where对象，注入KeySelector1 3. 调用equalTo生成内部类EqualTo对象，注入KeySelector2 4. 调用window升成内部静态类WithWindow，并且注入WindowAssigner（在该对象中还可以注入Trigger和Evictor 5. 最后调用apply方法将（Flat)JoinFunction注入并且用一个(Flat)JoinCoGroupFunction封装起来，而在这一步会将所有注入的对象用在coGroup上。详情见下一个Window CoGroup的解析
		//返回DataStream
		/*
		dataStream.join(otherStream)
	    .where(<key selector>).equalTo(<key selector>)
	    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
	    .apply (new JoinFunction () {...});	
	    */
		

		//8.Window CoGroup
		//根据Key和window将两个DataStream的元素聚集在两个集合中，根据CoGroupFunction来处理这两个集合，并产出结果
		//参数 1. DataStream 2. KeySelector1 3. KeySelector2 4. WindowAssigner 5. CoGroupFunction
		//Transformation：生成一个TaggedUnion类型和unionKeySelector，里面分别包含了两个流的元素类型和两个流的KeySelector。将两个流通过map分别输出为类型是TaggedUnion的两个流（map详情见StreamMap），再Union在一起（详情见Union），再使用合并过后的流和unionKeySelector生成一个KeyedStream（详情见KeyBy），最后使用KeyedStream的window方法并传入WindowAssigner生成WindowedStream，并apply CoGroupFunction来处理（详情见WindowedStream Apply方法）。总体来说，Flink对这个方法做了很多内部的转换，最后生成了两个StreamMapTransformation，一个PartitionTransformation和一个包含了WindowOperator的OneInputTransformation
		//返回DataStream	
		/*
		dataStream.coGroup(otherStream)
	    .where(0).equalTo(1)
	    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
	    .apply (new CoGroupFunction () {...});
	    */		
	}
}

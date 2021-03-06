package com.dingli.flink.learn.chapter03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
/**
 * Iterative Pi example, makes use of iteration data set to compute Pi.
 * @author TDeshpande
 *
 */
public class IterativePiExample {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create initial IterativeDataSet
		IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

		DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer i) throws Exception {
				double x = Math.random();
				double y = Math.random();
				System.out.println("rs1:" + x + "_y:" + y + "_i:" + i);
				return i + ((x * x + y * y < 1) ? 1 : 0);
			}
		});

		// Iteratively transform the IterativeDataSet
		DataSet<Integer> count = initial.closeWith(iteration);

		System.out.println("size:" + count.count());
		count.map(new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer count) throws Exception {
				System.out.println("count:" + count);
				System.out.println("rs:" + count / (double) 10000 * 4);
				return count / (double) 10000 * 4;
			}
		}).print();

		
	}
}

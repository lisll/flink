package com.dingli.flink.chapter06

import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.preprocessing.PolynomialFeatures

object MLRJob {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

 
    val trainingDataset = MLUtils.readLibSVM(env, "F:\\Flink_learn\\src_data\\data\\iris-train.txt")
    val testingDataset = MLUtils.readLibSVM(env, "F:\\Flink_learn\\src_data\\data\\iris-test.txt").map { lv => lv.vector }
    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(5)
      .setConvergenceThreshold(0.001)

    mlr.fit(trainingDataset)

    // The fitted model can now be used to make predictions
    val predictions = mlr.predict(testingDataset)

    predictions.print()

  }
}

package com.dingli.flink.chapter06.demo

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.math.{ SparseVector, DenseVector }
import org.apache.flink.ml.common.LabeledVector

object MyMRLApp {

  def main(args: Array[String]): Unit = {
    
    val pathToTrainingFile: String = "F:\\Flink_learn\\src_data\\data\\iris-train.txt"
    val pathToTestingFile: String = "F:\\Flink_learn\\src_data\\data\\iris-test.txt"
    
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Create multiple linear regression learner
    val mlr = MultipleLinearRegression()
      .setIterations(10)
      .setStepsize(0.5)
      .setConvergenceThreshold(0.001)

    // Obtain training and testing data set
    //val trainingDS: DataSet[LabeledVector] = // input data
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)
    //val testingDS: DataSet[Vector] = // output data
    val testingDS: DataSet[Vector] = env.readLibSVM(pathToTestingFile).map(_.vector)
      
    // Fit the linear model to the provided data
    mlr.fit(trainingDS)

    // Calculate the predictions for the test data
    val predictions = mlr.predict(testingDS)
    predictions.writeAsText("mlr-out")

    env.execute("Flink MLR App")
  }
}
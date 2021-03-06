package com.dinglicom.flink.stream.batch

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.extensions._
import java.lang.Iterable
 
object DataSetExample {
 
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    //System.err.println("=========[groupBy]==========")
    //groupBy(benv)
    
    //使用多个Case Class Fields
    //System.err.println("=========[groupTest2]==========")
    //groupTest2(benv)
    
    //System.err.println("=========[ReduceGroup]==========")
    //ReduceGroup(benv)
    
    //System.err.println("=========[ReduceGroup2]==========")
    //ReduceGroup2(benv)
    
    //System.err.println("=========[sortGroup]==========")
    //sortGroup(benv)
    
    //System.err.println("=========[sortGroup2]==========")
    //sortGroup2(benv)
    
    //System.err.println("=========[minBy]==========")
    //minBy(benv)
    
    //System.err.println("=========[maxBy]==========")
    //maxBy(benv)
    
    //System.err.println("=========[distinct]==========")
    //distinct(benv)
    //distinct4(benv)
    //distinct5(benv)
    
    //System.err.println("=========[join]==========")
    //join(benv)
    //join2(benv)
    //join3(benv)
    //join4(benv)
    //join5(benv)
    
    //System.err.println("=========[leftOuterJoin]==========")
    //leftOuterJoin(benv)
    //leftOuterJoin2(benv)
    
    //System.err.println("=========[rightOuterJoin]==========")
    //rightOuterJoin(benv)
    //rightOuterJoin2(benv)
    
    //System.err.println("=========[fullOuterJoin]==========")
    //fullOuterJoin(benv)
    //fullOuterJoin2(benv)
    
    //System.err.println("=========[cross]==========")
    //cross(benv)
    //cross2(benv)
    //cross3(benv)
    
    //System.err.println("=========[crossWith]==========")
    //crossWithTiny(benv)
    //crossWithHuge(benv)
    
    //System.err.println("=========[Union]==========")
    //Union(benv)
    
    //System.err.println("=========[first]==========")
    //first(benv)
    
    //System.err.println("=========[getParallelism]==========")
    //getParallelism(benv)
    
    //System.err.println("=========[writeAsTextCSV]==========")
    //writeAsTextCSV(benv)
    
    //System.err.println("=========[extensionsAPI]==========")
    //extensionsAPI(benv)
    
    //System.err.println("=========[MapFunction001scala]==========")
    //MapFunction001scala(benv)
    
    //System.err.println("=========[mapPartition]==========")
    //mapPartition(benv)
    
    //System.err.println("=========[FlatMapFunction001scala]==========")
    //FlatMapFunction001scala(benv)
    
    //System.err.println("=========[FilterFunction001scala]==========")
    //FilterFunction001scala(benv)
    
    System.err.println("=========[ReduceFunction001scala]==========")
    ReduceFunction001scala(benv)
    
    //System.err.println("=========[GroupReduceFunction001scala]==========")
    //GroupReduceFunction001scala(benv) 
    
    //System.err.println("=========[JoinFunction001scala]==========")
    //JoinFunction001scala(benv)
    
    //System.err.println("=========[CoGroupFunction001scala]==========")
    //CoGroupFunction001scala(benv)    
  }
 
  /**
    * 模板
    *
    * @param benv
    */
  def testTmp(benv: ExecutionEnvironment): Unit = {
  }
 
 
  def groupBy(benv: ExecutionEnvironment): Unit = {
 
    //1.定义 class
    case class WC(val word: String, val salary: Int)
    //2.定义DataSet[WC]
    val words: DataSet[WC] = benv.fromElements(
      WC("LISI", 600), WC("LISI", 400), WC("WANGWU", 300), WC("ZHAOLIU", 700))
 
    //3.使用自定义的reduce方法,使用key-expressions
    val wordCounts1 = words.groupBy("word").reduce {
      (w1, w2) => new WC(w1.word, w1.salary + w2.salary)
    }
 
    //4.使用自定义的reduce方法,使用key-selector
    val wordCounts2 = words.groupBy {
      _.word
    } reduce {
      (w1, w2) => new WC(w1.word, w1.salary + w2.salary)
    }
 
    //5.显示结果
    println(wordCounts2.collect)
    System.err.println(wordCounts1.collect)
  }
 
 
  /**
    * 使用多个Case Class Fields
    *
    * @param benv
    */
  def groupTest2(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Student(val name: String, addr: String, salary: Double)
 
    //2.定义DataSet[Student]
    val tuples: DataSet[Student] = benv.fromElements(
      Student("lisi", "shandong", 2400.00), Student("zhangsan", "henan", 2600.00),
      Student("lisi", "shandong", 2700.00), Student("lisi", "guangdong", 2800.00))
 
    //3.使用自定义的reduce方法,使用多个Case Class Fields name
    val reducedTuples1 = tuples.groupBy("name", "addr").reduce {
      (s1, s2) => Student(s1.name, s1.addr, s1.salary + s2.salary)
    }
 
    //4.使用自定义的reduce方法,使用多个Case Class Fields index
    val reducedTuples2 = tuples.groupBy(0, 1).reduce {
      (s1, s2) => Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    }
 
    //5.使用自定义的reduce方法,name和index混用
    val reducedTuples3 = tuples.groupBy(0, 1).reduce {
      (s1, s2) => Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    }
 
 
    //6.显示结果
    println(reducedTuples1.collect)
    println(reducedTuples2.collect)
    println(reducedTuples3.collect)
  }
 
  /**
    * 此函数和reduce函数类似，不过它每次处理一个grop而非一个元素。
    *
    * ReduceGroup示例一，操作tuple
    *
    * @param benv
    *
    * res14: Seq[(Int, String)] = Buffer((22,lisi), (20,zhangsan), (22,zhangsan))
    */
  def ReduceGroup(benv: ExecutionEnvironment): Unit = {
    //1.定义 DataSet[(Int, String)]
    val input: DataSet[(Int, String)] = benv.fromElements(
      (20, "zhangsan"), (22, "zhangsan"),
      (22, "lisi"), (20, "zhangsan"))
 
    //2.先用string分组，然后对分组进行reduceGroup
    val output = input.groupBy(1).reduceGroup {
      //将相同的元素用set去重
      (in, out: Collector[(Int, String)]) => {
        in.toSet foreach (out.collect)
      }
    }
    //3.显示结果
    println(output.collect)
  }
  /**
    * 操作case class
    *
    * @param benv
    *
    * res16: Seq[Student] = Buffer(Student(20,zhangsan), Student(22,zhangsan), Student(22,lisi))
    */
  def ReduceGroup2(benv: ExecutionEnvironment): Unit = {
    //1.定义case class
    case class Student(age: Int, name: String)
 
    //2.创建DataSet[Student]
    val input: DataSet[Student] = benv.fromElements(
      Student(20, "zhangsan"),
      Student(22, "zhangsan"),
      Student(22, "lisi"),
      Student(20, "zhangsan"))
    //3.以age进行分组，然后对分组进行reduceGroup
    val output = input.groupBy(_.age).reduceGroup {
      //将相同的元素用set去重
      (in, out: Collector[Student]) =>
        in.toSet foreach (out.collect)
    }
    //4.显示结果
    println(output.collect)
  }
  
  /**
    *
    * @param benv
    * res25: Seq[(Int, String)] = Buffer((18,zhangsan), (20,zhangsan), (22,zhangsan), (22,lisi))
    */
  def sortGroup(benv: ExecutionEnvironment): Unit = {
 
    //1.创建 DataSet[(Int, String)]
    val input: DataSet[(Int, String)] = benv.fromElements(
      (20, "zhangsan"),
      (22, "zhangsan"),
      (22, "lisi"),
      (22, "lisi"),
      (22, "lisi"),
      (18, "zhangsan"),
      (18, "zhangsan"))
 
    //2.用int分组，用int对分组进行排序
    //val sortdata = input.groupBy(0).sortGroup(0, Order.ASCENDING)
    val sortdata = input.groupBy(1).sortGroup(0, Order.ASCENDING)
    
    //3.对排序好的分组进行reduceGroup
    val outputdata = sortdata.reduceGroup {
      //将相同的元素用set去重
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach (out.collect)
    }
    //4.显示结果
    println(outputdata.collect)
    
    //for (x <- outputdata.collect)
    //  println(x)
  }
  
  def sortGroup2(benv: ExecutionEnvironment): Unit = {
    case class Student(name: String, age: Int)
    //1.创建 DataSet[(Int, String)]
    val input: DataSet[Student] = benv.fromElements(
      Student("zhangsan", 20),
      Student("zhangsan", 22),
      Student("lisi", 22),
      Student("lisi", 22),
      Student("lisi", 22),
      Student("zhangsan", 18),
      Student("zhangsan", 18))
 
    //2.用string分组，用string对分组进行排序
    val sortdata = input.groupBy(0).sortGroup(1, Order.DESCENDING)
    
    //3.对排序好的分组进行reduceGroup
    val outputdata = sortdata.reduceGroup {
      //将相同的元素用set去重
      (in, out: Collector[Student]) =>
        in.toSet foreach (out.collect)
    }
    //4.显示结果
    println(outputdata.collect)
  }
  
  /**
    * 在分组后的数据中，获取每组最小的元素。
    *
    * @param benv
    */
  def minBy(benv: ExecutionEnvironment): Unit = {
 
    //1.定义case class
    case class Student(age: Int, name: String, height: Double)
 
    //2.创建DataSet[Student]
    val input: DataSet[Student] = benv.fromElements(
      Student(16, "zhangasn", 194.5),
      Student(17, "zhangasn", 184.5),
      Student(18, "zhangasn", 174.5),
      Student(16, "lisi", 194.5),
      Student(17, "lisi", 184.5),
      Student(18, "lisi", 174.5))
 
    //3.以name进行分组，获取age最小的元素
    val output0: DataSet[Student] = input.groupBy(_.name).minBy(0)
    println(output0.collect)
 
    //4.以name进行分组，获取height和age最小的元素
    val output1: DataSet[Student] = input.groupBy(_.name).minBy(2, 0)
    println(output1.collect)
 
  }
 
 
  /**
    * 在分组后的数据中，获取每组最大的元素
    *
    * @param benv
    * res75: Seq[Student] = Buffer(Student(18,lisi,174.5), Student(18,zhangasn,174.5))
    */
  def maxBy(benv: ExecutionEnvironment): Unit = {
 
    //1.定义case class
    case class Student(age: Int, name: String, height: Double)
 
    //2.创建DataSet[Student]
    val input: DataSet[Student] = benv.fromElements(
      Student(16, "zhangasn", 194.5),
      Student(17, "zhangasn", 184.5),
      Student(18, "zhangasn", 174.5),
      Student(16, "lisi", 194.5),
      Student(17, "lisi", 184.5),
      Student(18, "lisi", 174.5))
 
    //3.以name进行分组，获取age最大的元素
    val output0: DataSet[Student] = input.groupBy(_.name).maxBy(0)
    println(output0.collect)
 
    //4.以name进行分组，获取height和age最大的元素
    val output1: DataSet[Student] = input.groupBy(_.name).maxBy(2, 0)
    println(output1.collect)
 
  }
 
 
  /**
    * 对DataSet中的元素进行去重
    *
    * @param benv
    */
  def distinct(benv: ExecutionEnvironment): Unit = {
 
    /**
      * distinct示例一，单一项目的去重
      */
    //1.创建一个 DataSet其元素为String类型
    val input: DataSet[String] = benv.fromElements("lisi", "zhangsan", "lisi", "wangwu")
 
    //2.元素去重
    val result = input.distinct()
 
    //3.显示结果
    println(result.collect)
    println(result.count())
 
    /**
      * distinct示例二，多项目的去重，不指定比较项目，默认是全部比较
      */
 
    //1.创建DataSet[(Int, String, Double)]
    val input2: DataSet[(Int, String, Double)] = benv.fromElements(
      (2, "zhagnsan", 1654.5), (3, "lisi", 2347.8), (2, "zhagnsan", 1654.5),
      (4, "wangwu", 1478.9), (5, "zhaoliu", 987.3), (2, "zhagnsan", 1654.0))
 
    //2.元素去重
    val output2 = input2.distinct()
 
    //3.显示结果
    println(output2.collect)
 
    /**
      * distinct示例三，多项目的去重，指定比较项目
      */
    //1.创建DataSet[(Int, String, Double)]
    val input3: DataSet[(Int, String, Double)] = benv.fromElements(
      (2, "zhagnsan", 1654.5), (3, "lisi", 2347.8), (2, "zhagnsan", 1658.5),
      (4, "wangwu", 1478.9), (5, "zhaoliu", 987.3), (2, "zhagnsan", 1657.0))
 
    //2.元素去重:指定比较第0和第1号元素
    val output3 = input3.distinct(0, 1)
 
    //3.显示结果
    println(output3.collect)
  }
 
 
  /**
    * case class的去重，指定比较项目
    *
    * @param benv
    */
  def distinct4(benv: ExecutionEnvironment): Unit = {
    //1.创建case class Student
    case class Student(name: String, age: Int)
 
    //2.创建DataSet[Student]
    val input: DataSet[Student] = benv.fromElements(
      Student("zhangsan", 24), Student("zhangsan", 24), Student("zhangsan", 25),
      Student("lisi", 24), Student("wangwu", 24), Student("lisi", 25))
 
    //3.去掉age重复的元素
    val age_r = input.distinct("age")
    println(age_r.collect)
 
    //4.去掉name重复的元素
    val name_r = input.distinct("name")
    println(name_r.collect)
 
    //5.去掉name和age重复的元素
    val all_r = input.distinct("age", "name")
    println(all_r.collect)
 
    //6.去掉name和age重复的元素
    val all = input.distinct()
    println(all.collect)
 
    //7.去掉name和age重复的元素
    val all0 = input.distinct("_")
    println(all0.collect)
  }
 
  /**
    * 根据表达式进行去重
    *
    * @param benv
    * res55: Seq[Int] = Buffer(3, 4, -5, 6, 7)
    */
  def distinct5(benv: ExecutionEnvironment): Unit = {
    //1.创建DataSet[Int]
    val input: DataSet[Int] = benv.fromElements(3, -3, 4, -4, 6, -5, 7)
 
    //2.根据表达式，本例中是根据元素的绝对值进行元素去重
    val output = input.distinct { x => Math.abs(x) }
 
    //3.显示结果
    println(output.collect)
  }
 
 
  /**
    * 将两个DataSet进行join操作
    *
    * @param benv
    *
    * res56: Seq[((Int, String), (Double, Int))] = Buffer(
    * ((4,wangwu),(1850.98,4)),
    * ((5,zhaoliu),(1950.98,5)),
    * ((3,lisi),(3850.98,3)))
    */
  def join(benv: ExecutionEnvironment): Unit = {
    //1.创建一个 DataSet其元素为[(Int,String)]类型
    val input1: DataSet[(Int, String)] = benv.fromElements(
      (2, "zhagnsan"), (3, "lisi"), (4, "wangwu"), (5, "zhaoliu"))
 
    //2.创建一个 DataSet其元素为[(Double, Int)]类型
    val input2: DataSet[(Double, Int)] = benv.fromElements(
      (1850.98, 4), (1950.98, 5), (2350.98, 6), (3850.98, 3))
 
    //3.两个DataSet进行join操作，条件是input1(0)==input2(1)
    val result = input1.join(input2).where(0).equalTo(1)
 
    //4.显示结果
    println(result.collect)
  }
 
  /**
    *
    * @param benv
    */
  def join2(benv: ExecutionEnvironment): Unit = {
    //1.定义case class
    case class Rating(name: String, category: String, points: Int)
 
    //2.定义DataSet[Rating]
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    //3.创建DataSet[(String, Double)]
    val weights: DataSet[(String, Double)] = benv.fromElements(
      ("youny1", 4.3), ("youny2", 7.2),
      ("youny3", 9.0), ("youny4", 1.5))
 
    //4.使用方法进行join
    val weightedRatings = ratings.join(weights).where("category").equalTo(0) {
      (rating, weight) => (rating.name, rating.points + weight._2)
    }
 
    //5.显示结果
    println(weightedRatings.collect)
  }
 
 
  /**
    *
    * @param benv
    */
  def join3(benv: ExecutionEnvironment): Unit = {
    case class Rating(name: String, category: String, points: Int)
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    val weights: DataSet[(String, Double)] = benv.fromElements(
      ("youny1", 4.3), ("youny2", 7.2),
      ("youny3", 9.0), ("youny4", 1.5))
 
    val weightedRatings = ratings.join(weights).where("category").equalTo(0) {
      (rating, weight, out: Collector[(String, Double)]) =>
        if (weight._2 > 0.1) out.collect(rating.name, rating.points * weight._2)
    }
 
    println(weightedRatings.collect)
  }
 
 
  /**
    *
    * @param benv
    * 执行join操作时暗示数据大小
    * 在执行join操作时暗示数据大小，可以帮助flink优化它的执行策略，提高执行效率。
    */
  def join4(benv: ExecutionEnvironment): Unit = {
    //1.定义DataSet[(Int, String)]
    val input1: DataSet[(Int, String)] =
      benv.fromElements((3, "zhangsan"), (2, "lisi"), (4, "wangwu"), (6, "zhaoliu"))
 
    //2.定义 DataSet[(Int, String)]
    val input2: DataSet[(Int, String)] =
      benv.fromElements((4000, "zhangsan"), (70000, "lisi"), (4600, "wangwu"), (53000, "zhaoliu"))
 
    // 3.暗示第二个输入很小
    val result1 = input1.joinWithTiny(input2).where(1).equalTo(1)
    println(result1.collect)
 
    // 4.暗示第二个输入很大
    val result2 = input1.joinWithHuge(input2).where(1).equalTo(1)
    println(result2.collect)
  }
 
 
  /**
    * 执行join操作时暗示数据大小
    *
    * @param benv
    *   flink有很多种执行join的策略，你可以指定一个执行策略，以便提高执行效率。
    *
    *
    *   暗示有如下选项：
    *
    *1.JoinHint.OPTIMIZER_CHOOSES:
    *   没有明确暗示，让系统自行选择。
    *2.JoinHint.BROADCAST_HASH_FIRST
    *   把第一个输入转化成一个哈希表，并广播出去。适用于第一个输入数据较小的情况。
    *3.JoinHint.BROADCAST_HASH_SECOND:
    *   把第二个输入转化成一个哈希表，并广播出去。适用于第二个输入数据较小的情况。
    *4.JoinHint.REPARTITION_HASH_FIRST:（defalut）
    *   1.如果输入没有分区，系统将把输入重分区。
    *   2.系统将把第一个输入转化成一个哈希表广播出去。
    *   3.两个输入依然比较大。
    *   4.适用于第一个输入小于第二个输入的情况。
    *5.JoinHint.REPARTITION_HASH_SECOND:
    *   1.如果输入没有分区，系统将把输入重分区。
    *   2.系统将把第二个输入转化成一个哈希表广播出去。
    *   3.两个输入依然比较大。
    *   4.适用于第二个输入小于第一个输入的情况。
    *6.JoinHint.REPARTITION_SORT_MERGE:
    *   1.如果输入没有分区，系统将把输入重分区。
    *   2.如果输入没有排序，系统将吧输入重排序。
    *   3.系统将合并两个排序好的输入。
    *   4.适用于一个或两个分区已经排序好的情况。
    */
  def join5(benv: ExecutionEnvironment): Unit = {
    //1.定义两个 DataSet
    val input1: DataSet[(Int, String)] =
      benv.fromElements((3, "zhangsan"), (2, "lisi"), (4, "wangwu"), (6, "zhaoliu"))
    val input2: DataSet[(Int, String)] =
      benv.fromElements((4000, "zhangsan"), (70000, "lisi"), (4600, "wangwu"), (53000, "zhaoliu"))
 
    //2.暗示input2很小
    val result1 = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST).where(1).equalTo(1)
 
    //3.显示结果
    println(result1.collect)
  }
 
  /**
    *
    * @param benv
    * res26: Seq[(String, Int)] = Buffer((moon,3), (dog,5), (cat,1), (sun,4), (water,-1))
    */
  def leftOuterJoin(benv: ExecutionEnvironment): Unit = {
    //1.定义case class
    case class Rating(name: String, category: String, points: Int)
 
    //2.定义 DataSet[Rating]
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5), Rating("tiger", "youny4", 5))
 
    //3.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"), ("water", "nice"))
 
    //4.两个dataset进行左外连接，指定方法
    val result1 = movies.leftOuterJoin(ratings).where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    *
    * 左外连接支持以下项目：
    *JoinHint.OPTIMIZER_CHOOSES
    *JoinHint.BROADCAST_HASH_SECOND
    *JoinHint.REPARTITION_HASH_SECOND
    *JoinHint.REPARTITION_SORT_MERGE
    *
    * res26: Seq[(String, Int)] = Buffer((cat,1), (dog,5), (moon,3), (sun,4), (water,-1))
    */
  def leftOuterJoin2(benv: ExecutionEnvironment): Unit = {
    //1.定义case class
    case class Rating(name: String, category: String, points: Int)
 
    //2.定义 DataSet[Rating]
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5), Rating("tiger", "youny4", 5))
 
    //3.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"), ("water", "nice"))
 
    //4.两个dataset进行左外连接，指定连接暗示，并指定连接方法
    val result1 = movies.leftOuterJoin(ratings, JoinHint.REPARTITION_SORT_MERGE)
      .where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    */
  def rightOuterJoin(benv: ExecutionEnvironment): Unit = {
    //1.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"))
 
    //2.定义 DataSet[Rating]
    case class Rating(name: String, category: String, points: Int)
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    //3.两个dataset进行左外连接，指定连接方法
    val result1 = movies.rightOuterJoin(ratings).where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    *
    * res34: Seq[(String, Int)] = Buffer((moon,3), (sun,4), (cat,1), (dog,5))
    */
  def rightOuterJoin2(benv: ExecutionEnvironment): Unit = {
    //1.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"))
 
    //2.定义 DataSet[Rating]
    case class Rating(name: String, category: String, points: Int)
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    //3.两个dataset进行左外连接，暗示连接方式，指定连接方法
    val result1 = movies.rightOuterJoin(ratings, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    */
  def fullOuterJoin(benv: ExecutionEnvironment): Unit = {
    //1.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"))
 
    //2.定义 DataSet[Rating]
    case class Rating(name: String, category: String, points: Int)
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    //3.两个dataset进行全外连接，指定连接方法
    val result1 = movies.fullOuterJoin(ratings).where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
  /**
    *
    * @param benv
    * res41: Seq[(String, Int)] = Buffer((cat,1), (dog,5), (moon,3), (sun,4))
    */
  def fullOuterJoin2(benv: ExecutionEnvironment): Unit = {
    //1.定义DataSet[(String, String)]
    val movies: DataSet[(String, String)] = benv.fromElements(
      ("moon", "ok"), ("dog", "good"),
      ("cat", "notbad"), ("sun", "nice"))
 
    //2.定义 DataSet[Rating]
    case class Rating(name: String, category: String, points: Int)
    val ratings: DataSet[Rating] = benv.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
 
    //3.两个dataset进行全外连接，指定连接方法
    val result1 = movies.fullOuterJoin(ratings, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo("name") {
      (m, r) => (m._1, if (r == null) -1 else r.points)
    }
 
    //5.显示结果
    println(result1.collect)
  }
 
  /**
    *
    * @param benv
    * 交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    *
    * res71: Seq[((Int, Int, Int), (Int, Int, Int))] = Buffer(
    * ((1,4,7),(10,40,70)), ((2,5,8),(10,40,70)), ((3,6,9),(10,40,70)),
    * ((1,4,7),(20,50,80)), ((2,5,8),(20,50,80)), ((3,6,9),(20,50,80)),
    * ((1,4,7),(30,60,90)), ((2,5,8),(30,60,90)), ((3,6,9),(30,60,90)))
    */
  def cross(benv: ExecutionEnvironment): Unit = {
    //1.定义两个DataSet
    val coords1 = benv.fromElements((1, 4, 7), (2, 5, 8), (3, 6, 9))
    val coords2 = benv.fromElements((10, 40, 70), (20, 50, 80), (30, 60, 90))
 
    //2.交叉两个DataSet[Coord]
    val result1 = coords1.cross(coords2)
 
    //3.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    * res69: Seq[(Coord, Coord)] = Buffer(
    * (Coord(1,4,7),Coord(10,40,70)), (Coord(2,5,8),Coord(10,40,70)), (Coord(3,6,9),Coord(10,40,70)),
    * (Coord(1,4,7),Coord(20,50,80)), (Coord(2,5,8),Coord(20,50,80)), (Coord(3,6,9),Coord(20,50,80)),
    * (Coord(1,4,7),Coord(30,60,90)), (Coord(2,5,8),Coord(30,60,90)), (Coord(3,6,9),Coord(30,60,90)))
    */
  def cross2(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)
 
    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))
 
    //3.交叉两个DataSet[Coord]
    val result1 = coords1.cross(coords2)
 
    //4.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    *
    * res65: Seq[(Int, Int, Int)] = Buffer(
    * (1,1,22), (2,1,24), (3,1,26),
    * (1,2,24), (2,2,26), (3,2,28),
    * (1,3,26), (2,3,28), (3,3,30))
    */
  def cross3(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)
 
    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
 
    //3.交叉两个DataSet[Coord]，使用自定义方法
    val r = coords1.cross(coords2) {
      (c1, c2) => {
        val dist = (c1.x + c2.x) + (c1.y + c2.y)
        (c1.id, c2.id, dist)
      }
    }
    //4.显示结果
    println(r.collect)
  }
 
 
  /**
    * 暗示第二个输入较小的交叉。
    * 拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    *
    * @param benv
    * res67: Seq[(Coord, Coord)] = Buffer(
    * (Coord(1,4,7),Coord(10,40,70)), (Coord(1,4,7),Coord(20,50,80)), (Coord(1,4,7),Coord(30,60,90)),
    * (Coord(2,5,8),Coord(10,40,70)), (Coord(2,5,8),Coord(20,50,80)), (Coord(2,5,8),Coord(30,60,90)),
    * (Coord(3,6,9),Coord(10,40,70)), (Coord(3,6,9),Coord(20,50,80)), (Coord(3,6,9),Coord(30,60,90)))
    */
  def crossWithTiny(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)
 
    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))
 
    //3.交叉两个DataSet[Coord]，暗示第二个输入较小
    val result1 = coords1.crossWithTiny(coords2)
 
    //4.显示结果
    println(result1.collect)
  }
 
  /**
    *
    * @param benv
    * 暗示第二个输入较大的交叉。
    * 拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
    * *
    *
    * res68: Seq[(Coord, Coord)] = Buffer(
    * (Coord(1,4,7),Coord(10,40,70)), (Coord(2,5,8),Coord(10,40,70)), (Coord(3,6,9),Coord(10,40,70)),
    * (Coord(1,4,7),Coord(20,50,80)), (Coord(2,5,8),Coord(20,50,80)), (Coord(3,6,9),Coord(20,50,80)),
    * (Coord(1,4,7),Coord(30,60,90)), (Coord(2,5,8),Coord(30,60,90)), (Coord(3,6,9),Coord(30,60,90)))
    */
  def crossWithHuge(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)
 
    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))
 
    //3.交叉两个DataSet[Coord]，暗示第二个输入较大
    val result1 = coords1.crossWithHuge(coords2)
 
    //4.显示结果
    println(result1.collect)
  }
 
 
  /**
    *
    * @param benv
    *
    * 合并多个DataSet。
    */
  def Union(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Student(val name: String, addr: String, salary: Double)
 
    //2.定义三个DataSet[Student]
    val tuples1 = benv.fromElements(
      Student("lisi-1", "shandong", 2400.00), Student("zhangsan-1", "henan", 2600.00))
 
    val tuples2 = benv.fromElements(
      Student("lisi-2", "shandong", 2400.00), Student("zhangsan-2", "henan", 2600.00))
 
    val tuples3 = benv.fromElements(
      Student("lisi-3", "shandong", 2400.00), Student("zhangsan-3", "henan", 2600.00))
 
    //3.将三个DataSet合并起来
    val unioned = tuples1.union(tuples2).union(tuples3)
 
    //4.显示结果
    println(unioned.collect)
  }
 
 
  /**
    *
    * @param benv
    *
    * Scala-Flink> out1.collect
    * res126: Seq[Student] = Buffer(
    * Student(lisi,shandong,2400.0), Student(zhangsan,hainan,2600.0))
    * *
    * Scala-Flink> out2.collect
    * res127: Seq[Student] = Buffer(
    * Student(lisi,shandong,2400.0), Student(wangwu,shandong,2400.0), Student(xiaoba,henan,2600.0),
    * Student(xiaoqi,guangdong,2400.0), Student(zhangsan,hainan,2600.0), Student(zhaoliu,hainan,2600.0))
    * *
    * Scala-Flink> out3.collect
    * res128: Seq[Student] = Buffer(
    * Student(lisi,shandong,2400.0), Student(wangwu,shandong,2400.0), Student(xiaoba,henan,2600.0),
    * Student(xiaoqi,guangdong,2400.0), Student(zhangsan,hainan,2600.0), Student(zhaoliu,hainan,2600.0))
    */
  def first(benv: ExecutionEnvironment): Unit = {
    //1.定义 case class
    case class Student(val name: String, addr: String, salary: Double)
 
    //2.定义DataSet[Student]
    val in: DataSet[Student] = benv.fromElements(
      Student("lisi", "shandong", 2400.00), Student("zhangsan", "hainan", 2600.00),
      Student("wangwu", "shandong", 2400.00), Student("zhaoliu", "hainan", 2600.00),
      Student("xiaoqi", "guangdong", 2400.00), Student("xiaoba", "henan", 2600.00))
 
    //3.取前2个元素
    val out1 = in.first(2)
    println(out1.collect)
 
    //3.取前2个元素 ???
    val out2 = in.groupBy(0).first(2)
    println(out2.collect)
 
    //3.取前3个元素 ???
    val out3 = in.groupBy(0).sortGroup(1, Order.ASCENDING).first(3)
    println(out3.collect)
  }
 
 
  /**
    *
    * @param benv
    * 获取DataSet的并行度。
    */
  def getParallelism(benv: ExecutionEnvironment): Unit = {
    //1.创建一个 DataSet其元素为String类型
    val input0: DataSet[String] = benv.fromElements("A", "B", "C")
 
    //2.设置DataSet的并行度。
    input0.setParallelism(2)
 
    //3.获取DataSet的并行度。
    println(input0.getParallelism)
    
  }
 
  /**
    *
    * @param benv
    */
  def writeAsTextCSV(benv: ExecutionEnvironment): Unit = {
    //1.创建 DataSet[Student]
    case class Student(age: Int, name: String, height: Double)
    val input: DataSet[Student] = benv.fromElements(
      Student(16, "zhangasn", 194.5),
      Student(17, "zhangasn", 184.5),
      Student(18, "zhangasn", 174.5),
      Student(16, "lisi", 194.5),
      Student(17, "lisi", 184.5),
      Student(18, "lisi", 174.5))
 
    //2.将DataSet写出到存储系统
    //input.writeAsText("hdfs:///output/flink/dataset/testdata/students.txt")
    //input.writeAsCsv("hdfs:///output/flink/dataset/testdata/students.csv", "#", "|")     
    input.writeAsText("F:\\Flink_learn\\out_data\\students.txt")
    input.writeAsCsv("F:\\Flink_learn\\out_data\\students2.csv", "\r\n", "|")  

    //3.执行程序
    benv.execute()
  }
 
 
  /**
    * Aggregate
    * CoGroup
    * combineGroup
    *
    * @param benv
    * TODO
    */
  def Aggregate(benv: ExecutionEnvironment): Unit = {
 
  }
 
 
  /**
    * 获取DataSet的每个分片中元素的个数。
    *
    * @param benv
    */
  def countElementsPerPartition(benv: ExecutionEnvironment): Unit = {
    //1.创建一个 DataSet其元素为String类型
    val input: DataSet[String] = benv.fromElements("A", "B", "C", "D", "E", "F")
 
    //2.设置分片前
    val p0 = input.getParallelism
    //    val c0=input.countElementsPerPartition
    //    c0.collect
 
    //2.设置分片后
    //设置并行度为3，实际上是将数据分片为3
    input.setParallelism(3)
    val p1 = input.getParallelism
    //    val c1=input.countElementsPerPartition
    //    c1.collect
 
 
  }
 
 
  /**
    * import org.apache.flink.api.scala.extensions._
    *
    * @param benv
    *
    *
    * 全函数
    * 偏函数
    */
  def extensionsAPI(benv: ExecutionEnvironment): Unit = {
    val input: DataSet[String] = benv.fromElements("A", "B", "C", "D", "E", "F")
 
 
    //1.引入增强依赖
    import org.apache.flink.api.scala.extensions._
 
    //2.创建DataSet[Point]
    case class Point(x: Double, y: Double)
    val ds = benv.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
 
 
    /**
      * 全函数
      */
    //3.使用mapWith进行元素转化
    val r = ds.mapWith {
      case Point(x, y) => Point(x * 2, y + 1)
    }
    /**
      * 偏函数
      */
    val r2 = ds.mapWith {
      case Point(x, _) => x * 2
    }
 
 
    //3.使用filterWith进行元素过滤
    val rfilterWith = ds.filterWith {
      case Point(x, y) => x > 1 && y < 5
    }
 
 
    val rfilterWith2 = ds.filterWith {
      case Point(x, _) => x > 1
    }
 
    //3.使用reduceWith进行元素的merger
    /**
      * res159: Seq[Point] = Buffer(Point(9.0,12.0))
      */
    val rreduceWith = ds.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + x2, y1 + y2)
    }
 
 
    /**
      * 可以使用偏函数进行flatMap操作。
      *
      * 结果：res1: Seq[(String, Double)] = Buffer((x,1.0), (y,2.0), (x,3.0), (y,4.0), (x,5.0), (y,6.0))
      */
    //3.使用reduceWith进行元素的merger
    val rflatMapWith = ds.flatMapWith {
      case Point(x, y) => Seq("x" -> x, "y" -> y)
    }
 
    //4.显示结果
    println(r.collect)
    println(r2.collect)
    println(rfilterWith.collect)
    println(rfilterWith2.collect)
    println(rreduceWith.collect)
    println(rflatMapWith.collect)
 
  }
 
 
  /**
    * 定制function
    *
    * 以element为粒度，对element进行1：1的转化
    *
    * text2.print();
    * FLINK VS SPARK--##bigdata##
    * BUFFER VS  SHUFFER--##bigdata##
    * *
    * text3.print();
    * (FLINK VS SPARK,14)
    * (BUFFER VS  SHUFFER,18)
    * *
    * text4.print();
    * Wc(FLINK VS SPARK,14)
    * Wc(BUFFER VS  SHUFFER,18)
    */
  def MapFunction001scala(benv: ExecutionEnvironment): Unit = {
    val text = benv.fromElements("flink vs spark", "buffer vs  shuffer")
 
    // 2.以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    val text2 = text.map(new MapFunction[String, String] {
      override def map(s: String): String = s.toUpperCase() + "--##bigdata##"
    })
    text2.print()
 
    // 3.以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
    val text3 = text.map(new MapFunction[String, (String, Int)] {
      override def map(s: String): (String, Int) = (s.toUpperCase(), s.length)
    })
    text3.print()
 
    // 4.以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
    //4.1定义class
    case class Wc(line: String, lenght: Int)
    //4.2转化成class类型
    val text4 = text.map(new MapFunction[String, Wc] {
      override def map(s: String): Wc = Wc(s.toUpperCase(), s.length)
    })
    text4.print()
  }
 
 
  /**
    *
    * 以partition为粒度，对element进行1：1的转化。有时候会比map效率高。
    *
    * @param env
    *
    *
    * text2.print();
    * 2
    * *
    * text3.print();
    * FLINK VS SPARK--##bigdata##
    * BUFFER VS  SHUFFER--##bigdata##
    * *
    * text4.print();
    * Wc(FLINK VS SPARK,14)
    * Wc(BUFFER VS  SHUFFER,18)
    */
  def mapPartition(benv: ExecutionEnvironment): Unit = {
    val text = benv.fromElements("flink vs spark", "buffer vs  shuffer")
 
    //2.以partition为粒度，进行map操作，计算element个数
    val text2 = text.mapPartition(new MapPartitionFunction[String, Long]() {
      override def mapPartition(iterable: Iterable[String], collector: Collector[Long]): Unit = {
        var c = 0
        val itor = iterable.iterator()
        while (itor.hasNext) {
          itor.next()
          c = c + 1
        }
        collector.collect(c)
      }
    })
    text2.print()
    //3.以partition为粒度，进行map操作，转化element内容
    val text3 = text.mapPartition(partitionMapper = new MapPartitionFunction[String, String]() {
      override def mapPartition(iterable: Iterable[String], collector: Collector[String]): Unit = {
        val itor = iterable.iterator()
        while (itor.hasNext) {
          val line = itor.next().toUpperCase + "--##bigdata##"
          collector.collect(line)
        }
      }
    })
    text3.print()
 
    //4.以partition为粒度，进行map操作，转化为大写并,并计算line的长度。
    //4.1定义class
    case class Wc(line: String, lenght: Int)
    //4.2转化成class类型
    val text4 = text.mapPartition(new MapPartitionFunction[String, Wc] {
      override def mapPartition(iterable: Iterable[String], collector: Collector[Wc]): Unit = {
        val itor = iterable.iterator()
        while (itor.hasNext) {
          var s = itor.next()
          collector.collect(Wc(s.toUpperCase(), s.length))
        }
      }
    })
    text4.print()
  }
 
  /**
    *
    * 以element为粒度，对element进行1：n的转化。
    *
    * @param env
    *
    * text2.print()
    * FLINK VS SPARK--##bigdata##
    * BUFFER VS  SHUFFER--##bigdata##
    * *
    * text3.collect().foreach(_.foreach(println(_)))
    * FLINK
    * VS
    * SPARK
    * BUFFER
    * VS
    * SHUFFLE
    */
  def FlatMapFunction001scala(env: ExecutionEnvironment): Unit = {
    val text = env.fromElements("flink vs spark", "buffer vs  shuffer")
 
    // 2.以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    val text2 = text.flatMap(new FlatMapFunction[String, String]() {
      override def flatMap(s: String, collector: Collector[String]): Unit = {
        collector.collect(s.toUpperCase() + "--##bigdata##")
      }
    })
    text2.print()
 
    //3.对每句话进行单词切分,一个element可以转化为多个element，这里是一个line可以转化为多个Word
    //map的只能对element进行1：1转化，而flatMap可以对element进行1：n转化
    val text3 = text.flatMap {
      new FlatMapFunction[String, Array[String]] {
        override def flatMap(s: String, collector: Collector[Array[String]]): Unit = {
          val arr: Array[String] = s.toUpperCase().split("\\s+")
          collector.collect(arr)
        }
      }
    }
    //显示结果的简单写法
    text3.collect().foreach(_.foreach(println(_)))
    
    //实际上是先获取Array[String],再从中获取到String
    text3.collect().foreach(arr => {
      arr.foreach(token => {
        println(token)
      })
    })
  }
 
 
  /**
    *
    * 以element为粒度，对element进行合并操作。最后只能形成一个结果。
    * @param env
    *
    *            text2.print()
28
text3.print()
5040
text4.print()
157
text5.print()
intermediateResult=1 ,next=2
intermediateResult=3 ,next=3
intermediateResult=6 ,next=4
intermediateResult=10 ,next=5
intermediateResult=15 ,next=6
intermediateResult=21 ,next=7
    */
  def FilterFunction001scala(env: ExecutionEnvironment): Unit = {
    val text = env.fromElements(2, 4, 7, 8, 9, 6)
 
    //2.对DataSet的元素进行过滤，筛选出偶数元素
    val text2 = text.filter(new FilterFunction[Int] {
      override def filter(t: Int): Boolean = {
        t % 2 == 0
      }
    })
 
 
    text2.print()
    println("=======================")
    //3.对DataSet的元素进行过滤，筛选出大于5的元素
    val text3 = text.filter(new FilterFunction[Int] {
      override def filter(t: Int): Boolean = {
        t > 5
      }
    })
    text3.print()
    println("=======================")
    
    val test4 = text.filter(x => x > 5)
    test4.print()
  }
 
 
 
 
  /**
    *
    * @param env
    */
  def ReduceFunction001scala(env: ExecutionEnvironment): Unit = {
    val text = env.fromElements(1, 2, 3, 4, 5, 6, 7)
 
    //2.对DataSet的元素进行合并，这里是计算累加和
    val text2 = text.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        intermediateResult + next
      }
    })
    text2.print()
    println("=======================")
    //3.对DataSet的元素进行合并，这里是计算累乘积
    val text3 = text.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        intermediateResult * next
      }
    })
    text3.print()
    println("=======================")
    //4.对DataSet的元素进行合并，逻辑可以写的很复杂
    val text4 = text.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        if (intermediateResult % 2 == 0) {
          intermediateResult + next
        } else {
          intermediateResult * next
        }
      }
    })
    text4.print()
    println("=======================")
    //5.对DataSet的元素进行合并，可以看出intermediateResult是临时合并结果，next是下一个元素
    val text5 = text.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        println("intermediateResult=" + intermediateResult + " ,next=" + next)
        intermediateResult + next
      }
    })
    text5.collect()
  }
 

  /**
    *
    * @param env
    *
    *
    * text3.print()
      28
      text3.print()
      (12,16)
      data2.print
      (lisi,2003)
      (zhangsan,4000)
    *
    *
    */
  def GroupReduceFunction001scala(env: ExecutionEnvironment): Unit = {
    val text = env.fromElements(1, 2, 3, 4, 5, 6, 7)
 
    //2.对DataSet的元素进行分组合并，这里是计算累加和
    val text2 = text.reduceGroup(new GroupReduceFunction[Int, Int] {
      override def reduce(iterable: Iterable[Int], collector: Collector[Int]): Unit = {
        var sum = 0
        val itor = iterable.iterator()
        while (itor.hasNext) {
          sum += itor.next()
        }
        collector.collect(sum)
      }
    })
    text2.print()
 
    //3.对DataSet的元素进行分组合并，这里是分别计算偶数和奇数的累加和
    val text3 = text.reduceGroup(new GroupReduceFunction[Int, (Int, Int)] {
      override def reduce(iterable: Iterable[Int], collector: Collector[(Int, Int)]): Unit = {
        var sum0 = 0
        var sum1 = 0
        val itor = iterable.iterator()
        while (itor.hasNext) {
          val v = itor.next
          if (v % 2 == 0) {
            //偶数累加和
            sum0 += v
          } else {
            //奇数累加和
            sum1 += v
          }
        }
        collector.collect(sum0, sum1)
      }
    })
    text3.print()
 
    //4.对DataSet的元素进行分组合并，这里是对分组后的数据进行合并操作，统计每个人的工资总和（每个分组会合并出一个结果）
    val data = env.fromElements(
      ("zhangsan", 1000), ("lisi", 1001), ("zhangsan", 3000), ("lisi", 1002))
    //4.1根据name进行分组，
    val data2 = data.groupBy(0).reduceGroup(new GroupReduceFunction[(String, Int), (String, Int)]{
      override def reduce(iterable: Iterable[(String, Int)], collector: Collector[(String, Int)]):
      Unit = {
        var salary = 0
        var name = ""
        val itor = iterable.iterator()
        //4.2统计每个人的工资总和
        while (itor.hasNext) {
          val t = itor.next()
          name = t._1
          salary += t._2
        }
        collector.collect(name, salary)
      }
    })
    data2.print
  }
 

  /**
    *join将两个DataSet按照一定的关联度进行类似SQL中的Join操作。
    * @param env
    *
    * @return
    *                    text2.print()
    ((A001,wangwu,wangwu@qq.com),(P003,wangwu))
    ((A001,zhangsan,zhangsan@qq.com),(P001,zhangsan))
    ((A001,lisi,lisi@qq.com),(P002,lisi))
    ((A001,lisi,lisi@qq.com),(P004,lisi))
    *
    */
  def JoinFunction001scala(env: ExecutionEnvironment): Unit = {
    val authors = env.fromElements(
      Tuple3("A001", "zhangsan", "zhangsan@qq.com"),
      Tuple3("A001", "lisi", "lisi@qq.com"),
      Tuple3("A001", "wangwu", "wangwu@qq.com"))
    val posts = env.fromElements(
      Tuple2("P001", "zhangsan"),
      Tuple2("P002", "lisi"),
      Tuple2("P003", "wangwu"),
      Tuple2("P004", "lisi"))
    // 2.scala中没有with方法来使用JoinFunction
    val text2 = authors.join(posts).where(1).equalTo(1)
 
    //3.显示结果
    text2.print()
  }
 
 
  /**
    *
    * 将2个DataSet中的元素，按照key进行分组，一起分组2个DataSet。而groupBy值能分组一个DataSet
    *
    * @param env
    *
    * text2.print()
([Lscala.Tuple3;@6c2c1385,[Lscala.Tuple2;@5f354bcf)
([Lscala.Tuple3;@3daf7722,[Lscala.Tuple2;@78641d23)
([Lscala.Tuple3;@74589991,[Lscala.Tuple2;@146dfe6)
    *
    */
  def CoGroupFunction001scala(env: ExecutionEnvironment): Unit = {
 
    val authors = env.fromElements(
      Tuple3("A001", "zhangsan", "zhangsan@qq.com"),
      Tuple3("A001", "lisi", "lisi@qq.com"),
      Tuple3("A001", "wangwu", "wangwu@qq.com"))
    val posts = env.fromElements(
      Tuple2("P001", "zhangsan"),
      Tuple2("P002", "lisi"),
      Tuple2("P003", "wangwu"),
      Tuple2("P004", "lisi"))
    // 2.scala中coGroup没有with方法来使用CoGroupFunction
    val text2 = authors.coGroup(posts).where(1).equalTo(1)
 
    //3.显示结果
    text2.print()
  }
 
 
}
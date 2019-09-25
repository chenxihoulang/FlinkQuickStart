package org.myorg.quickstart

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    var inputFile = "file:///Users/chaihongwei/flink/demo/word.txt"
    val ds: DataSet[String] = env.readTextFile(inputFile)
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    //经过groupby进行分组，sum进行聚合
    val aggDs: AggregateDataSet[(String, Int)] =
      ds.flatMap(_.split(" "))
        .map((_, 1))
        .groupBy(0)
        .sum(1)
    // 打印
    aggDs.print()

  }
}

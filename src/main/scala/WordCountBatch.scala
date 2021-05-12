

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.util.Collector

object WordCountBatch {
  @throws[Exception]
  def main(args: Array[String]): Unit = { // 获取批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val dataSet = env.readTextFile("./src/main/resources/words")
    val result = dataSet.flatMap(new WordCountBatch.MyFlatMapper).groupBy(0).sum(1) // 按照第一个位置的word分组
    // 将第二个位置的word求和
    result.print()
    env.execute("wordCountbatch")
  }

  class MyFlatMapper extends FlatMapFunction[String, Tuple2[String, Integer]] {
    @throws[Exception]
    override def flatMap(s: String, collector: Collector[Tuple2[String, Integer]]): Unit = {
      val arr = s.split(" ")
      for (a <- arr) {
        collector.collect(new Tuple2[String, Integer](a, 1))
      }
    }
  }
}

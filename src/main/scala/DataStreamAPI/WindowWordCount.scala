package DataStreamAPI

object WindowWordCount {

    import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


    def main(args: Array[String]): Unit = {
      import org.apache.flink.streaming.api.windowing.time.Time
      // 不同的获取执行环境的方法
      // val env = StreamExecutionEnvironment.getExecutionEnvironment()
      // val env = StreamExecutionEnvironment.createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
      // val env = StreamExecutionEnvironment.createLocalEnvironment()
      // val text: DataStream[String] = env.readTextFile("./FlinkBasic/src/main/resources/words")
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // 按照window窗口时间进行个数统计
      val text = env.socketTextStream("localhost", 9999)
      val counts = text.flatMap { _.toLowerCase.split("\\W+")filter{ _.nonEmpty } }
        .map { (_, 1) }
        .keyBy(_._1)
        .timeWindow(Time.seconds(10))
        .reduce((x, y) =>{
        (x._1, x._2 + y._2)
      })
      print("counts结果：")
      counts.print()

      // keyby和reduce综合验证,数值一直累加
      val result: DataStream[(String, Int)] = text.flatMap { _.toLowerCase.split("\\W+")filter{ _.nonEmpty } }
        .map { (_, 1) }
        .keyBy(_._1).reduce((x, y) =>{
        (x._1, x._2 + y._2)
      })
      result.print()


      env.execute("Window Stream WordCount")
    }
}

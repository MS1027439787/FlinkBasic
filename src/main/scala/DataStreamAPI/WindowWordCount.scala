package DataStreamAPI

object WindowWordCount {

    import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


    def main(args: Array[String]): Unit = {
      import org.apache.flink.streaming.api.windowing.time.Time
      //不同的获取执行环境的方法
      //  val env = StreamExecutionEnvironment.getExecutionEnvironment()
      //val env = StreamExecutionEnvironment.createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
      //val env = StreamExecutionEnvironment.createLocalEnvironment()
      //val text: DataStream[String] = env.readTextFile("./FlinkBasic/src/main/resources/words")
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val text = env.socketTextStream("192.168.100.254", 9999)
      val counts = text.flatMap { _.toLowerCase.split("\\W+")filter{ _.nonEmpty } }
        .map { (_, 1) }
        .keyBy(_._1)
        .timeWindow(Time.seconds(20))
        .sum(1)
      counts.print()

      env.execute("Window Stream WordCount")
    }
}

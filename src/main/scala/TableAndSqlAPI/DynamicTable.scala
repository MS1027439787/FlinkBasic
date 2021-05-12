package TableAndSqlAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.types.Row

/**
 * @author masai
 * @date 2021/5/12
 */
object DynamicTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, fsSettings)
    val stream:DataStream[String] = env.socketTextStream("localhost", 9999)

    val map:DataStream[(String, Int)] = stream.flatMap { _.toLowerCase.split("\\W+")filter{ _.nonEmpty } }
      .map { (_, 1) }
    val table: Table = tableEnv.fromDataStream(map, $("name"), $("num"))

    val search = tableEnv.sqlQuery("""
        |SELECT name, num
        |from
    """.stripMargin
      + table)

//    val appendRow: DataStream[Row] = tableEnv.toAppendStream[Row](search)
//    appendRow.print()


    val revenue = tableEnv.sqlQuery("""
                                      |SELECT count(1)
                                      |from
    """.stripMargin
      + table)

    // 如果这里改成tableEnv.toAppendStream[Row](revenue) 程序会报错
    val retractRow: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](revenue)

    retractRow.print()
    env.execute("table test")
  }


}

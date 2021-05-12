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
    val sourceTable: Table = tableEnv.fromDataStream(map, $("name"), $("num"))

    /**
     * 1、结果表和原表没任何区别，也就是该结果表的数据只有insert因此可以用Append Mode
     */

    val search = tableEnv.sqlQuery("""
        |SELECT name, num
        |from
    """.stripMargin
      + sourceTable)

//    val appendRow: DataStream[Row] = tableEnv.toAppendStream[Row](search)
//    appendRow.print()


    /**
     * 2、分组聚合，也就是当有新数据进来，会对以前的结果进行更新操作（先删除再插入）
     */
    val groupbyTable = tableEnv.sqlQuery("""
                                      |SELECT name, count(1)
                                      |from
    """.stripMargin
      + sourceTable + " group by name")

    // 如果这里改成tableEnv.toAppendStream[Row](revenue) 程序会报错
    val retractRow: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](groupbyTable)

    //retractRow.print()

    /**
     * 3、增加时间滚动窗口的分组聚合，每次只计算
     */
//    val tmptable = tableEnv.sqlQuery("""
//                                       |SELECT name, num, CURRENT_TIMESTAMP as cTime
//                                       |from
//    """.stripMargin
//      + sourceTable)
//    val groupbyTime = tableEnv.sqlQuery("""
//                                           |SELECT name, count(1), tumble_end(cTime, interval '1' HOURS) as endT
//                                           |from
//    """.stripMargin
//      + tmptable + " group by name, Tumble(cTime, interval '1' HOURS)")
//    val retractRow2: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](groupbyTime)
//    retractRow2.print()
    env.execute("table test")


  }


}

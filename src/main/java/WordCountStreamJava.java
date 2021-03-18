/**
 * @author masai
 * @date 2021/3/18
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);   // 默认是执行机器的核数

        DataStreamSource<String> input = env.socketTextStream("127.0.0.1", 8888);
        input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String word : s) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
            // 流式处理是没有 group by 的
        }).keyBy(0).sum(1).print();

        // 流式处理必须启动执行
        env.execute("word count test");
    }

}

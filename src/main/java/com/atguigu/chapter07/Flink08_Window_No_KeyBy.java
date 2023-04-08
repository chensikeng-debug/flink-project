package com.atguigu.chapter07;

import com.atguigu.utils.MyUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * [root@hadoop162 ~]# nc -lk 9999
 * a a a
 * a b c
 * 2> [(a,1), (a,1), (a,1)]
 * 1> [(a,1), (b,1), (c,1)]
 * <p>
 * 按照之前的想法，两个并行度可能是在两个机器节点上，是不可能在一个窗口内的，为啥看着像是在一个窗口内呢？
 * 因为：在keyby之前使用process,process算子的并行度一定是1.从webui中也可以看到它的并行度
 * The parallelism of non parallel operator must be 1.
 * 所以数据都进入到一个窗口内的。
 * 所以不建议在keyby之前使用
 *
 * @author Administrator
 * @date 2022/11/30 16:31
 */
public class Flink08_Window_No_KeyBy {
    @SneakyThrows
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);  // localhost:20000可以看到webui
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.socketTextStream("hadoop162", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }

                    }
                })
                // window一般在keyby之后
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        List<Tuple2<String, Long>> tuple2s = MyUtil.toList(elements);
                        out.collect(String.valueOf(tuple2s));
                    }
                })
                .print();

        env.execute();
    }
}

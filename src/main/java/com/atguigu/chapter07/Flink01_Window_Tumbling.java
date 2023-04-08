package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * 只计算窗口内的数据，窗口结束后统计数据
 * window一般在keyby之后
 * <p>
 * [root@hadoop162 ~]# nc -lk 9999
 * a a a
 * a b c
 * <p>
 * 2> (a,3)
 * 2> (a,1)
 * 1> (b,1)
 * 1> (c,1)
 *
 * @author Administrator
 * @date 2022/11/29 10:30
 */
public class Flink01_Window_Tumbling {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .keyBy(t -> t.f0)
                // window一般在keyby之后
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute();
    }
}


/*
 * process可以看到窗口开始结束时间,每一个key都有一个窗口,相同的key进入一个窗口
 * 上下文对象可以获取窗口的开始结束时间
 *
 * [root@hadoop162 ~]# nc -lk 9999
 *  a
 *  a a b
 *
 *
 *  2> key=a, window[= Tue Nov 29 14:38:50 CST 2022, Tue Nov 29 14:38:55 CST 2022), [a]
 *  1> key=b, window[= Tue Nov 29 14:39:00 CST 2022, Tue Nov 29 14:39:05 CST 2022), [b]
 *  2> key=a, window[= Tue Nov 29 14:39:00 CST 2022, Tue Nov 29 14:39:05 CST 2022), [a, a]
 *
 *
 * @author chensikeng
 * @create 2022/11/29 14:26
 **/
class Flink01_Window_Tumbling2 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .keyBy(t -> t.f0)
                // window一般在keyby之后
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements, // 这个窗口内所有的元素
                                        Collector<String> out) throws Exception {
                        ArrayList<String> words = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            words.add(element.f0);
                        }
                        TimeWindow window = context.window(); // 上下文对象可以获取窗口的开始结束时间
                        Date start = new Date(window.getStart());
                        Date end = new Date(window.getEnd());
                        out.collect("key=" + key + ", window[= " + start + ", " + end + "), " + words);

                    }
                })
                .print();

        env.execute();
    }
}

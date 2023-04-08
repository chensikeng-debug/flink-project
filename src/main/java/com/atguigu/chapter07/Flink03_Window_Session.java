package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * 超过gap，就结束上一个窗口，然后打印+
 *
 * [root@hadoop162 ~]# nc -lk 9999
 * a a
 * a
 * a
 * a
 * a
 * v a
 *
 * 2> key=a, window[= Tue Nov 29 16:07:45 CST 2022, Tue Nov 29 16:07:59 CST 2022), [a, a, a, a, a, a, a]
 * 1> key=v, window[= Tue Nov 29 16:07:54 CST 2022, Tue Nov 29 16:07:59 CST 2022), [v]
 *
 * @author Administrator
 * @date 2022/11/29 14:46
 */
public class Flink03_Window_Session {
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
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements, // 窗口时间内的数据
                                        Collector<String> out) throws Exception {
                        ArrayList<String> list = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            list.add(element.f0);
                        }
                        TimeWindow window = context.window();
                        Date start = new Date(window.getStart());
                        Date end = new Date(window.getEnd());
                        out.collect("key=" + key + ", window[= " + start + ", " + end + "), " + list);
                    }
                })

                .print();

        env.execute();


    }
}


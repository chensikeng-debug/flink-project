package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * 一个数据可能属于多个窗口
 * 例：窗口5秒，步长2秒，每两秒钟会得到一个最近5秒钟的数据。步长, 用来控制滑动窗口启动的频率.
 * [root@hadoop162 ~]# nc -lk 9999
 * a
 * dd d d e f
 *
 * 2> key=a, window[= Tue Nov 29 15:07:26 CST 2022, Tue Nov 29 15:07:31 CST 2022), [a]
 * 2> key=a, window[= Tue Nov 29 15:07:28 CST 2022, Tue Nov 29 15:07:33 CST 2022), [a]
 * 2> key=a, window[= Tue Nov 29 15:07:30 CST 2022, Tue Nov 29 15:07:35 CST 2022), [a]
 *
 * 1> key=e, window[= Tue Nov 29 15:09:32 CST 2022, Tue Nov 29 15:09:37 CST 2022), [e]
 * 2> key=dd, window[= Tue Nov 29 15:09:32 CST 2022, Tue Nov 29 15:09:37 CST 2022), [dd]
 * 1> key=f, window[= Tue Nov 29 15:09:32 CST 2022, Tue Nov 29 15:09:37 CST 2022), [f]
 * 2> key=d, window[= Tue Nov 29 15:09:32 CST 2022, Tue Nov 29 15:09:37 CST 2022), [d, d]
 * 1> key=f, window[= Tue Nov 29 15:09:34 CST 2022, Tue Nov 29 15:09:39 CST 2022), [f]
 * 2> key=d, window[= Tue Nov 29 15:09:34 CST 2022, Tue Nov 29 15:09:39 CST 2022), [d, d]
 * 1> key=e, window[= Tue Nov 29 15:09:34 CST 2022, Tue Nov 29 15:09:39 CST 2022), [e]
 * 2> key=dd, window[= Tue Nov 29 15:09:34 CST 2022, Tue Nov 29 15:09:39 CST 2022), [dd]
 * 2> key=d, window[= Tue Nov 29 15:09:36 CST 2022, Tue Nov 29 15:09:41 CST 2022), [d, d]
 * 1> key=f, window[= Tue Nov 29 15:09:36 CST 2022, Tue Nov 29 15:09:41 CST 2022), [f]
 * 2> key=dd, window[= Tue Nov 29 15:09:36 CST 2022, Tue Nov 29 15:09:41 CST 2022), [dd]
 * 1> key=e, window[= Tue Nov 29 15:09:36 CST 2022, Tue Nov 29 15:09:41 CST 2022), [e]
 *
 * @author Administrator
 * @date 2022/11/29 14:46
 */
public class Flink02_Window_Sliding {
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
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
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


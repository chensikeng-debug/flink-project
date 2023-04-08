package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * 元素满足数量就关窗
 * <p>
 * 数据各关各的窗，关闭时没有固定的时间的
 * <p>
 *
 * 各种窗口的区别
 *  数量的窗口：各关各的
 *  时间点窗口：一起关
 *  session的窗口：分开的，谁的gap到了，就关谁的
 * <p>
 * [root@hadoop162 ~]# nc -lk 9999
 * a a a
 * a a
 * b
 * b
 * a
 * b
 * <p>
 * 2> key=a, [a, a, a]
 * 2> key=a, [a, a, a]
 * 1> key=b, [b, b, b]
 *
 * 滚动
 *
 * @author Administrator
 * @date 2022/11/29 14:46
 */
public class Flink04_Window_CountWindow {
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
                .countWindow(3)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        ArrayList<String> list = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            list.add(element.f0);
                        }
                        out.collect("key=" + key + ", " + list);

                    }
                })
                .print();

        env.execute();
    }
}

/**
 *
 * 滑动
 * 进入（来够）两个元素，就开始关闭前面的窗口：
 * 从最后一个元素开始向前数最多5个元素
 *
 * [root@hadoop162 ~]# nc -lk 9999
 * a
 * a
 * b c
 * b
 * a
 * a
 * a
 * a
 *
 * 2> key=a, [a, a]
 * 1> key=b, [b, b]
 * 2> key=a, [a, a, a, a]
 * 2> key=a, [a, a, a, a, a]
 *
 * @author chensikeng
 * @create 2022/11/29 17:09
 **/
class Flink04_Window_CountWindow2 {
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
                .countWindow(5, 2)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        ArrayList<String> list = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            list.add(element.f0);
                        }
                        out.collect("key=" + key + ", " + list);

                    }
                })
                .print();

        env.execute();
    }
}
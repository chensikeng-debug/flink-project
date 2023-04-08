package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * Reduce是增量聚合函数，来数据了就计算，计算效率高
 * 要求输入输出类型一致
 * <p>
 * reduce是第二个元素进来的时候会调用reduce方法聚合
 * <p>
 * [root@hadoop162 ~]# nc -lk 9999
 * a
 * a
 * <p>
 * a
 * a
 * a
 * a
 * <p>
 * a
 * aa a a
 * <p>
 * Flink01_Window_Reduce.reduce
 * 2> (a,2)
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * 2> (a,4)
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * 2> (a,3)
 * 2> (aa,1)
 *
 * @author Administrator
 * @date 2022/11/29 10:30
 */
public class Flink05_Window_Reduce {
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
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,// 这个窗口内上一次聚合的结果
                                                       Tuple2<String, Long> value2 // 当前新进来的需要聚合的元素
                    ) throws Exception {
                        System.out.println("Flink01_Window_Reduce.reduce");
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);

                    }
                })
                .print();


        env.execute();
    }
}

/**
 * reduce(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function)
 * <p>
 * 二次处理reduce已经处理好的结果
 * // 相对于reduce(ReduceFunction<T> reduceFunction)
 * // 1.可以返回和输入不同的类型
 * // 2.可以获取key
 * // 2.可以获取窗口的开始结束时间
 * <p>
 * ReduceFunction<T> reduceFunction是来一条算一条
 * ProcessWindowFunction<T, R, K, W> function是窗口关闭的时候算一下
 * <p>
 * [root@hadoop162 ~]# nc -lk 9999
 * a
 * a
 * a
 * a
 * a
 * a
 * a
 * a
 * a
 * <p>
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * 2> key=a, window=TimeWindow{start=1669718100000, end=1669718105000}, result(a,3)
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * Flink01_Window_Reduce.reduce
 * 2> key=a, window=TimeWindow{start=1669718105000, end=1669718110000}, result(a,6)
 *
 * @author chensikeng
 * @create 2022/11/29 18:26
 **/
class Flink05_Window_Reduce2 {
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
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,// 这个窗口内上一次聚合的结果
                                                               Tuple2<String, Long> value2 // 当前新进来的需要聚合的元素
                            ) throws Exception {
                                System.out.println("Flink01_Window_Reduce.reduce");
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);

                            }

                        }, // 处理的是reduceFuncation处理的结果，被ProcessWindowFuncation截胡的了的
                        // 相对于没有这个
                        // 1.可以返回和输入不同的类型，数据类型
                        // 2.可以获取窗口的开始结束时间
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<Tuple2<String, Long>> elements, // 这里永远只有一个值，例如（a, 4），是上面已经reduce已经处理好的结果，传入到这里来
                                                Collector<String> out) throws Exception {
                                Tuple2<String, Long> result = elements.iterator().next();
                                out.collect("key=" + key + ", window=" + ctx.window() + ", result" + result);
                            }
                        })
                .print();


        env.execute();
        // 校验身份证


    }
}
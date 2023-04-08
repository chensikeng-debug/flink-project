package com.atguigu.chapter07;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Aggregate是增量聚合函数，来数据了就计算，计算效率高
 * <p>
 * 例如：求平均值
 * 输入的是(a 3) (a 2)
 * 结果要求平均值 2.5
 * <p>
 * [root@hadoop162 ~]# nc -lk 9999
 * a 2
 * a 3
 * <p>
 * Flink06_Window_Aggregate.createAccumulator
 * Flink06_Window_Aggregate.add
 * Flink06_Window_Aggregate.add
 * Flink06_Window_Aggregate.getResult
 * 2> 2.5
 *
 * @author Administrator
 * @date 2022/11/30 10:32
 */
public class Flink06_Window_Aggregate {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("hadoop162", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] data = value.split(" ");
                        out.collect(Tuple2.of(data[0], Long.valueOf(data[1])));

                    }
                })
                .keyBy(t -> t.f0)
                // window一般在keyby之后
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 此例子中：输入的是(a 3) (a 2) 累加器是（5 2） 结果要求平均值 (a 2.5)
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Integer>, Double>() {
                    // 初始化一个累加器
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        System.out.println("Flink06_Window_Aggregate.createAccumulator");
                        return Tuple2.of(0L, 0);
                    }

                    // 把传入的值，使用累加器进行累加
                    @Override
                    public Tuple2<Long, Integer> add(Tuple2<String, Long> value,
                                                     Tuple2<Long, Integer> acc) { // acc累加器
                        System.out.println("Flink06_Window_Aggregate.add");
                        return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
                    }

                    // 返回最终的结果
                    @Override
                    public Double getResult(Tuple2<Long, Integer> acc) {
                        System.out.println("Flink06_Window_Aggregate.getResult");
                        return acc.f0 * 1.0 / acc.f1;
                    }

                    // 合并两个累加器：只有会话窗口会有，其它窗口不会用
                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> acc1, Tuple2<Long, Integer> acc2) {
                        System.out.println("Flink06_Window_Aggregate.merge");
                        return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                    }
                })
                .print();


        env.execute();
    }
}

/**
 * 想要获取key的相关信息，可以参考如下的案例
 * aggregate(AggregateFunction<T> aggregateFunction, ProcessWindowFunction<T, R, K, W> function)
 * [root@hadoop162 ~]# nc -lk 9999
 * a 3
 * a 2
 * <p>
 * Flink06_Window_Aggregate.createAccumulator
 * Flink06_Window_Aggregate.add
 * Flink06_Window_Aggregate.add
 * Flink06_Window_Aggregate.getResult
 * 2> key=a, window=TimeWindow{start=1669796325000, end=1669796330000}, result2.5
 *
 * @author chensikeng
 * @create 2022/11/30 16:11
 **/

class Flink06_Window_Aggregate2 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("hadoop162", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] data = value.split(" ");
                        out.collect(Tuple2.of(data[0], Long.valueOf(data[1])));

                    }
                })
                .keyBy(t -> t.f0)
                // window一般在keyby之后
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 此例子中：输入的是(a 3) (a 2) 累加器是（5 2） 结果要求平均值 (a 2.5)
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Integer>, Double>() {
                               // 初始化一个累加器
                               @Override
                               public Tuple2<Long, Integer> createAccumulator() {
                                   System.out.println("Flink06_Window_Aggregate.createAccumulator");
                                   return Tuple2.of(0L, 0);
                               }

                               // 把传入的值，使用累加器进行累加
                               @Override
                               public Tuple2<Long, Integer> add(Tuple2<String, Long> value,
                                                                Tuple2<Long, Integer> acc) { // acc累加器
                                   System.out.println("Flink06_Window_Aggregate.add");
                                   return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
                               }

                               // 返回最终的结果
                               @Override
                               public Double getResult(Tuple2<Long, Integer> acc) {
                                   System.out.println("Flink06_Window_Aggregate.getResult");
                                   return acc.f0 * 1.0 / acc.f1;
                               }

                               // 合并两个累加器：只有会话窗口会有，其它窗口不会用
                               @Override
                               public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> acc1, Tuple2<Long, Integer> acc2) {
                                   System.out.println("Flink06_Window_Aggregate.merge");
                                   return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                               }
                           },
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<Double> elements, Collector<String> out) throws Exception {
                                Double avg = elements.iterator().next();
                                out.collect("key=" + key + ", window=" + ctx.window() + ", result" + avg);
                            }
                        })
                .print();


        env.execute();
    }
}

/**
 * 尽量使用Reduce和Aggregate,少用process，它要窗口的缓存数据到内存，对内存的消耗比较大，效率也低一些
 *
 * @author chensikeng
 * @create 2022/11/30 16:21
 **/
package com.atguigu.chapter06;

import com.atguigu.pojo.UserBehavior;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Administrator
 * @date 2022/11/14 19:28
 */
public class Flink01_Project_PV2 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        env.setParallelism(1);
        env.readTextFile("input/userBehavior/UserBehavior.csv")
                .map(data -> {
                    String[] dataLine = data.split(",");
                    return new UserBehavior(
                            Long.parseLong(dataLine[0]),
                            Long.parseLong(dataLine[1]),
                            Integer.parseInt(dataLine[2]),
                            dataLine[3],
                            Long.parseLong(dataLine[4]));
                })
                .keyBy(ub -> ub.getBehavior())
                /*.filter(bhv -> "pv".equalsIgnoreCase(bhv.getBehavior()))*/
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    Long sum = 0L;

                    @Override
                    public void processElement(UserBehavior userBehavior, Context context, Collector<Long> collector) throws Exception {
                        if ("pv".equals(userBehavior.getBehavior())) {
                            sum++;
                            collector.collect(sum);
                        }
                    }
                })
                .print(); // 434349

        env.execute();
    }
}

class Flink01_Project_PV_2 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.readTextFile("input/userBehavior/UserBehavior.csv")
                .map(data -> {
                    String[] dataLine = data.split(",");
                    return new UserBehavior(
                            Long.parseLong(dataLine[0]),
                            Long.parseLong(dataLine[1]),
                            Integer.parseInt(dataLine[2]),
                            dataLine[3],
                            Long.parseLong(dataLine[4]));
                })
                .keyBy(ub -> ub.getBehavior())
                .process(new KeyedProcessFunction<String, UserBehavior, Tuple2<String, Long>>() {
                    Long sum = 0L;

                    @Override
                    public void processElement(UserBehavior userBehavior, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if ("pv".equals(userBehavior.getBehavior())) {
                            sum++;
                            collector.collect(Tuple2.of(userBehavior.getBehavior(), sum));
                        }
                    }
                })
                .print(); // (pv,434349)

        env.execute();
    }
}

class Flink01_Project_PV_3 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.readTextFile("input/userBehavior/UserBehavior.csv")
                .map(data -> {
                    String[] dataLine = data.split(",");
                    return new UserBehavior(
                            Long.parseLong(dataLine[0]),
                            Long.parseLong(dataLine[1]),
                            Integer.parseInt(dataLine[2]),
                            dataLine[3],
                            Long.parseLong(dataLine[4]));
                })
                .process(new ProcessFunction<UserBehavior, Tuple2<String, Long>>() {
                    Long sum = 0L;

                    @Override
                    public void processElement(UserBehavior userBehavior, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if ("pv".equals(userBehavior.getBehavior())) {
                            sum++;
                            collector.collect(Tuple2.of(userBehavior.getBehavior(), sum));
                        }
                    }
                })
                .print(); // (pv,434349)

        env.execute();
    }
}
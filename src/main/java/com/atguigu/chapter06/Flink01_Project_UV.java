package com.atguigu.chapter06;

import com.atguigu.pojo.UserBehavior;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * 先算pv再算uv
 */
public class Flink01_Project_UV {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.readTextFile("input/userBehavior/UserBehavior.csv")
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (data, out) -> {
                    String[] dataLine = data.split(",");
                    UserBehavior ub = new UserBehavior(
                            Long.parseLong(dataLine[0]),
                            Long.parseLong(dataLine[1]),
                            Integer.parseInt(dataLine[2]),
                            dataLine[3],
                            Long.parseLong(dataLine[4]));
                    if ("pv".equals(ub.getBehavior())) {
                        out.collect(Tuple2.of("uv", ub.getUserId()));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(ub -> ub.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                    Set userIds = new HashSet<Long>();

                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<Long> collector) throws Exception {
                        if (userIds.add(stringLongTuple2.f1)) { // add
                            collector.collect((long) userIds.size());
                        }
                    }
                })
                .print("uv");

        env.execute();

    }
}

class Flink01_Project_UV_2 {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.readTextFile("input/userBehavior/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String data, Collector<UserBehavior> out) throws Exception {
                        String[] dataLine = data.split(",");
                        UserBehavior ub = new UserBehavior(
                                Long.parseLong(dataLine[0]),
                                Long.parseLong(dataLine[1]),
                                Integer.parseInt(dataLine[2]),
                                dataLine[3],
                                Long.parseLong(dataLine[4]));
                        Set userIds = new HashSet<Long>();
                        if ("pv".equals(ub.getBehavior())) {
                            out.collect(ub);
                        }
                    }
                })


                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    Set userIds = new HashSet<Long>();

                    @Override
                    public void processElement(UserBehavior userBehavior, Context context, Collector<Long> out) throws
                            Exception {
                        if (userIds.add(userBehavior.getUserId())) {
                            out.collect((long) userIds.size());
                        }
                    }
                })
                .print("uv");

        env.execute();

    }
}
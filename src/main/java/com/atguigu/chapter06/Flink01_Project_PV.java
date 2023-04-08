package com.atguigu.chapter06;

import com.atguigu.pojo.UserBehavior;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 统计用户行为为pv的数据量
 */
public class Flink01_Project_PV {
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
                .filter(bhv -> "pv".equalsIgnoreCase(bhv.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of(userBehavior.getBehavior(), 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print()
        ;

        env.execute();


    }
}

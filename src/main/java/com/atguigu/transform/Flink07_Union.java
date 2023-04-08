package com.atguigu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/3/3 10:28
 *
 * union之前两个或多个流的数据类型必须是一样
 * union可以操作多个流
 */
public class Flink07_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> s2 = env.fromElements(4, 6, 7, 8, 9);
        DataStream<Integer> s12 = s1.union(s2);
        s12.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return value * value + "";
            }
        }).print();

        env.execute();
    }
}

package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author Administrator
 * @date 2022/3/3 10:20
 * <p>
 * 1.两个流中存储的数据类型可以不同
 * 2.只是机械的合并在一起, 内部仍然是分离的2个流
 * 3.只能2个流进行connect, 不能有第3个参与
 */
public class Flink06_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> s2 = env.fromElements("a", "b", "c", "d", "e");
        ConnectedStreams<Integer, String> s12 = s1.connect(s2);
        // CoMapFunction<Integer, String, String> 参数：第一个流数据类型，第二个流数据类型，第三个流数据类型
        s12.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + ">";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + ">";
            }
        }).print();

        env.execute();
    }
}

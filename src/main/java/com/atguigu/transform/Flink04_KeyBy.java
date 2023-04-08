package com.atguigu.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/3/2 10:57
 */
public class Flink04_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 奇数分一组, 偶数分一组
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(new KeySelector<Integer, String>() { //第一个参数是数据的类型，第二个数据是key的类型
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();
        env.execute();

    }
}

class KeyBy_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 奇数分一组, 偶数分一组
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数" )
                .print();
        env.execute();

    }
}


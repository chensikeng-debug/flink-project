package com.atguigu.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/3/3 10:44
 *
 * 把流中的元素随机打乱. 对同一个组数据, 每次执行得到的结果都不同.
 */
public class Flink05_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .shuffle()
                .print();
        env.execute();
    }
}

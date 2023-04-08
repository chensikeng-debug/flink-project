package com.atguigu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/3/1 16:50
 */
public class Flink03_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 ==0;
                    }
                }).print();

        env.execute();
    }
}

class Filter_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5)
//                .filter((Integer value) -> {
//                    return value % 2 ==0;
//                }).print();
                .filter(value -> value % 2 ==0)
                .print();

        env.execute();


    }
}

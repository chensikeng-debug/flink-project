package com.atguigu.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Administrator
 * @date 2022/3/1 15:26
 * /*
 * 2> 16
 * 2> 64
 * 1> 9
 * 3> 25
 * 7> 1
 * 8> 4
 * 7> 1
 * 3> 125
 * 1> 27
 * 8> 8
 */
public class Flink02_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value * value);
                        out.collect(value * value * value);
                    }
                })
                .print();

        env.execute();
    }
}

/**
 * @author Administrator
 * @date 2022/3/1 16:06
 * 在使用Lambda表达式表达式的时候, 由于泛型擦除的存在, 在
 * 运行的时候无法获取泛型的具体类型, 全部当做Object来处理, 及其低效,
 * 所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.
 */
class FlatMap_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5)
                .flatMap((Integer value, Collector<Integer> out) -> {
                    out.collect(value * value);
                    out.collect(value * value * value);
                }).returns(Types.INT).print();

        env.execute();

    }
}



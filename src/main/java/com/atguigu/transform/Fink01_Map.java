package com.atguigu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/3/1 14:39
 */
public class Fink01_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1,2,3,4,5)
            .map(new MapFunction<Integer, String>() {
                   @Override
                   public String map(Integer value) throws Exception {
                       return value + ">";
                   }
               }).print();

        env.execute();
    }
}

class Map_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 3, 4, 5)
                .map(ele -> ele * ele)
                .print();

        env.execute();
    }
}

class Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(5);

        /*env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyRichMapFunction()).setParallelism(2)
                .print();*/

        env
                .fromElements(1,2,3,4,5)
                .map(new MyRichMapFunction()).setParallelism(2)
                .print();

        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ... 执行一次");
        }

        @Override
        public Integer map(Integer value) throws Exception {
            System.out.println("map ... 一个元素执行一次");
            return value * value;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close ... 执行一次");
        }
    }
}

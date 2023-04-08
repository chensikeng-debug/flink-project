package com.atguigu.mode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 执行模式
 * <p>
 * 流 STREAMING
 * 批 BATCH
 * 自动 AUTOMATIC
 * <p>
 * 流批一体，一套api就能搞定
 */
public class Flink_Execution_mode {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> lineStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        lineStream.print();
        //扁平化为一行数据
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String inputWord, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = inputWord.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        }).keyBy(10).sum(1);
        ;
        streamOperator.print();
        env.execute();
    }

}

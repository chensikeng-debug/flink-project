package com.atguigu.wordcount.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author Administrator
 * @date 2022/11/4 10:54
 * 步骤：
 * 1.创建执行环境
 * 2.读取文件一行一行的数据作为数据集
 * 3.分组聚合
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        /**
         * chen si keng
         * are you ok
         * how old are you
         * how are you
         **/
        dataSource.print();
        //扁平化为一行数据
        FlatMapOperator<String, Tuple2<String, Long>> kvTuple = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String inputWord, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = inputWord.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        /**
         * (are,1)
         * (you,1)
         * (ok,1)
         * (chen,1)
         * (si,1)
         **/
        kvTuple.print();
        AggregateOperator<Tuple2<String, Long>> kvGroupSum = kvTuple.groupBy(0).sum(1);
        /**
         *(chen,1)
         * (you,3)
         * (ok,1)
         * (are,3)
         **/
        kvGroupSum.print();

    }
}

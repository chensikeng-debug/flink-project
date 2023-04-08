package com.atguigu.wordcount.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @create 2022/11/3 19:59
 * 入门案例 wordcount
 * 步骤
 * 1.创建执行环境
 * 2.从文件读取数据，执行读取，读出来的就是一行一行的数据集
 * 3.把数据集进行处理，按照空额分来，转换成(word, 1)二元组进行统计
 **/
class WordCount2 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.独处文件一行一行的数据集, String是一行数据，Tuple是想要返回的数据(word, 1)
        DataSource<String> inputDataSet = env.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> kvTuple = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String wordLine, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = wordLine.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        /**
         * (how,1)
         * (old,1)
         * (are,1)
         * (you,1)
         * (are,1)
         * (you,1)
         **/
        kvTuple.print();
        AggregateOperator<Tuple2<String, Long>> kGroupVsum = kvTuple.groupBy(0).sum(1);
        kGroupVsum.print();
    }
}

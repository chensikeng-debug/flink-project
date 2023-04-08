package com.atguigu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author Administrator
 * @date 2022/1/1 18:21
 */
public class wordcount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.从文件读取数据，执行读取，读出来的就是一行一行的数据集
        DataSet<String> inputDataSet = env.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        //3.把数据集进行处理，按照空格分开，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Long>> resultDataSet = inputDataSet.flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);
        resultDataSet.print();
    }

    // FlatMapFunction,输入的是一行String，想要返回的是一个二元组Tuple2
    // flatMap没有返回值，所有用out是用来收集你想返回的数据的
    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
//                out.collect(new Tuple2<>(word ,1));
                out.collect(Tuple2.of(word, 1L)); //Tuple2.of调用的也是new Tuple2
            }
        }
    }
}


package com.atguigu.wordcount.practice;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Administrator
 * @date 2022/11/4 11:55
 */
public class WordCount3 {
    public static void main(String[] args) throws Exception {
       /* ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> kvTuple = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        kvTuple.print();

        AggregateOperator<Tuple2<String, Long>> aggreSum = kvTuple.groupBy(0).sum(1);
        aggreSum.print();*/
       String delimiter = "|@|".replaceAll("\\|", "\\\\|");
       String line = "hive|@|160|@|12.34|@|1900-01-01|@|21:00:00|@|2021-12-15 19:00:00";
        String[] delimiters = line.split(delimiter, -1);
        System.out.println(delimiters.length);

//        SimpleDateFormat format = new SimpleDateFormat();
//        Date date = format.parse("1900-01-01");
//        System.out.println(date.toString().getBytes());

        System.out.println("1900-01-01".getBytes().length);
        System.out.println("21:00:00".getBytes().length);
        System.out.println("2021-12-15 19:00:00".getBytes().length);

        //37 byte = 0.000037MB = 1850M

    }
}

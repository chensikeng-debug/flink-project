package com.atguigu.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/1/5 11:21
 */
public class Stream_wordcount {
    public static void main(String[] args) throws Exception {
        // 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可以设置
        env.setParallelism(8);
        // 2.读取文件
        DataStream<String> inputStreamData = env.readTextFile("C:\\Users\\Administrator\\Desktop\\实时数仓\\flink-project\\src\\main\\resources\\words.txt");
        // 3.对数据进行处理，用空格分开，转换成(word, 1)，按照key分组，再聚合汇总
        DataStream<Tuple2<String, Long>> resultStreamData = inputStreamData.flatMap(new wordcount.MyFlatMapFunction())
                // 4.分组
                .keyBy(t -> t.f0)
                // 5.求和
                .sum(1);

        // 6.打印
        resultStreamData.print();

        // 7.执行
        env.execute();
    }
}

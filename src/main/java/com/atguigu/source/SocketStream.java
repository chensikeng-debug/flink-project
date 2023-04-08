package com.atguigu.source;

import com.atguigu.wordcount.wordcount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/1/5 12:03
 */
public class SocketStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStreamData = env.socketTextStream("hadoop162", 9999);
        DataStream<Tuple2<String, Long>> resultStreamData = inputStreamData.flatMap(new wordcount.MyFlatMapFunction())
                .keyBy(t -> t.f0)  // 4.分组
                .sum(1); // 5.求和

        resultStreamData.print(); // 6.打印

        // 7.执行
        env.execute();
    }
}



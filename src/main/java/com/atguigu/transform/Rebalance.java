package com.atguigu.transform;

import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 * @date 2022/11/8 18:00
 */
// rebalance均匀的分布到每个分区
public class Rebalance {
    @SneakyThrows
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.fromElements(1,2,3,4,5,6,7)
                .rebalance()
                .print().setParallelism(2);

        env.execute();
    }
}

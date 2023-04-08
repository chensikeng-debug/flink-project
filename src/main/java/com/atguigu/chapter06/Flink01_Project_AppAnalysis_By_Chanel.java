package com.atguigu.chapter06;

import com.atguigu.pojo.MarketingUserBehavior;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * APP市场推广统计 - 分渠道
 */
public class Flink01_Project_AppAnalysis_By_Chanel {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }


}

class AppMarketingDataSource implements SourceFunction<MarketingUserBehavior> {


    @Override
    public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
        while (true) {
            Random random = new Random();
            Long userId = (long) random.nextInt(10000000) + 1;
            String[] behaviors = {"download", "update", "install", "uninstall"};
            String[] channels = {"xiaomi", "apple", "oppo", "vivo", "huawei"};
            String behavior = behaviors[random.nextInt(behaviors.length)];
            String channel = channels[random.nextInt(channels.length)];
            Long timestamp = System.currentTimeMillis();
            MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(userId, behavior, channel, timestamp);
            sourceContext.collect(marketingUserBehavior);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }

}

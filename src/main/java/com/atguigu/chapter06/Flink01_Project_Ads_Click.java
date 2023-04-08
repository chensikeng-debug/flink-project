package com.atguigu.chapter06;

import com.atguigu.pojo.AdsClickLog;
import com.atguigu.pojo.MarketingUserBehavior;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 各省份页面广告点击量实时统计
 */
public class Flink01_Project_Ads_Click {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String line) throws Exception {
                        String[] datas = line.split(",");
                        return new AdsClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .map(adCk -> Tuple2.of(adCk.getProvince() + "_" + adCk.getAdId(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tp -> tp.f0)
                .sum(1)
                .print("省份-广告");

        env.execute();
    }


}
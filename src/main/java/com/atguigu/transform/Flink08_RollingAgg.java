package com.atguigu.transform;

import com.atguigu.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author A dministrator
 * @date 2022/3/3 11:07
 * <p>
 * 简单滚动聚合算子
 * sum, max, min, maxBy, minBy
 * select sum(vc) from t group by id
 *
 * 1.在flink中，聚合必须是在keyBy之后
 * 2.聚合之后，非keyBy和聚合字段，取的是碰到的第一个
 * 3.maxBy, MinBy默认随着最大和最小的来的
 * 4.maxBy和minBy可以指定当出现相同值的时候,其他字段是否取第一个. true表示取第一个, false表示取与最大值(最小值)同一行的.
 *
 * <p>
 *
 */
public class Flink08_RollingAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_1", 1607527998000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_2", 1607527997000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(value -> value.getId());
//                .keyBy(WaterSensor::getId); // 方法引用

        kbStream
                .sum("vc")
                //.max("vc")
                //.min("vc")
//                .maxBy("vc", true)
                .print();
/**
 * WaterSensor(id=sensor_1, ts=1607527992000, vc=20)      20+50=70 70+30=100 100+50=150  10+30=40 40+30=70， 非keyBy和聚合字段取的第一个，如ts
 * WaterSensor(id=sensor_1, ts=1607527992000, vc=70)
 * WaterSensor(id=sensor_1, ts=1607527992000, vc=100)
 * WaterSensor(id=sensor_1, ts=1607527992000, vc=150)
 * WaterSensor(id=sensor_2, ts=1607527993000, vc=10)
 * WaterSensor(id=sensor_2, ts=1607527993000, vc=40)
 * WaterSensor(id=sensor_2, ts=1607527993000, vc=70)
 **/
        env.execute();
    }
}

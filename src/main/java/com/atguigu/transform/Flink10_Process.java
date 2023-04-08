package com.atguigu.transform;

import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Process可以获取到流的更多信息，不仅仅是数据本身
 * context上下文对象 可以获取到一些流的信息
 * <p>
 * Process算是最灵活的算子了，当用其它转换算子无法完成需求时，可以考虑用process算子
 *
 * WaterSensor(id=sensor_1, ts=1607527992000, vc=20)
 * WaterSensor(id=sensor_1, ts=1607527994000, vc=70)
 * WaterSensor(id=sensor_1, ts=1607527996000, vc=120)
 * WaterSensor(id=sensor_2, ts=1607527993000, vc=130)
 * WaterSensor(id=sensor_2, ts=1607527995000, vc=160)
 */
public class Flink10_Process {
    @SneakyThrows
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        env.fromCollection(waterSensors)
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    Integer sum = 0; //这里的sum相加只能是在当前的并行度中生效 状态

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                        sum += waterSensor.getVc();
                        waterSensor.setVc(sum);
                        collector.collect(waterSensor);
                    }
                }).print();

        env.execute();
    }
}

class Process2 {
    @SneakyThrows
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(Tuple2.of("key是：" + context.getCurrentKey(), waterSensor.getVc()));
                    }
                })
                .print();
        env.execute();
    }
}
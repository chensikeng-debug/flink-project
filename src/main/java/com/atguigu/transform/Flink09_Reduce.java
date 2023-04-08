package com.atguigu.transform;

import com.atguigu.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值
 * ，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
 *  reduce算子输入输出的类型一致，用在keyby之后
 **/
/**
     WaterSensor(id=sensor_1, ts=1607527992000, vc=20)
     reducer function ...
     WaterSensor(id=sensor_1, ts=1607527992000, vc=70)
     reducer function ...
     WaterSensor(id=sensor_1, ts=1607527992000, vc=120)
     WaterSensor(id=sensor_2, ts=1607527993000, vc=10)
     reducer function ...
     WaterSensor(id=sensor_2, ts=1607527993000, vc=40)
 */
public class Flink09_Reduce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("reducer function ...");
                        return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                        //相加之后和keyby再sum(vc)的效果一样的，若想要非分组聚合字段ts也随着同一行变，可以把这里改成value2.getTs()
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

/**
 * Reduce写法简化，小技巧 reduce匿名函数改成Lambda写法，
 * 只需要把reduce()方法的参数改成@Override其中的参数就可以了，
 * 表示输入参数，之后的箭头标识返回值
 */
class Reduce_Lambda {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        // 一个分组数据流的聚合操作，合并当前元素和上次聚合结果，返回一个新的值。聚合后结果的类型, 必须和原来流中元素的类型保持一致!
        env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId)
                /*.reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                    }
                })*/
                .reduce((value1, value2) -> new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc()))
                //相加之后和keyby再sum(vc)的效果一样的，若想要非分组聚合字段ts也随着同一行变，可以把这里改成value2.getTs()

                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author Administrator
 * @date 2022/11/9 20:37
 * <p>
 * 前提：启动zk和kafka集群
 * 注意：指定hostname:port的方式，需要在本机的hosts加ip映射关系
 * <p>
 * 推荐使用二，三方式写入，数据更均匀，避免数据倾斜
 */
public class Flink_KafkaSink {
    @SneakyThrows
    public static void main(String[] args) {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", "hadoop162:9092");
        env
                .fromCollection(waterSensors)
//                .map(elem -> JSON.toJSONString(elem));

                /**
                 * 案例一：
                 * 简单的生产数据
                 * 如果并行度为1，那么数据会只写入到一个分区中，案例一就是这种情况，数据只写入到一个一个分区中
                 *
                 * 1.当并行度小鱼分区数的时候，会导致kafka的个别分区没有数据
                 * 2.当向kafka写数据的时候，让你的并行度和kafka分区数保持一致
                 * 3.也可以采用轮训的方式写入，或者让kafka根据key自动计算分区
                 **/
                /*.map(JSON::toJSONString)
                .addSink(new FlinkKafkaProducer<String>(
                        "hadoop162:9092",
                        "topic_sensor",
                        new SimpleStringSchema()));*/
                // 案例二：指定key的方式,根据key的哈希值，将value均匀的打散到kafka的分区中，
                /*.addSink(new FlinkKafkaProducer<WaterSensor>(
                        "default",
                        new KafkaSerializationSchema<WaterSensor>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, @Nullable Long aLong) {
                                return new ProducerRecord<byte[], byte[]>(
                                        "topic_sensor",
                                        waterSensor.getId().getBytes(),
                                        JSON.toJSONString(waterSensor).getBytes());
                            }
                        },
                        producerConfig,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                ));*/
                // 案例三：不使用key,flink11会自动将数据轮询
                // （根据时间的轮询，一段时间写入到这个分区，一段时间写入到另一个分区）写入到kafka的分区中
                .addSink(new FlinkKafkaProducer<WaterSensor>(
                        "default",
                        new KafkaSerializationSchema<WaterSensor>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, @Nullable Long aLong) {
                                return new ProducerRecord<byte[], byte[]>(
                                        "topic_sensor",
                                        JSON.toJSONString(waterSensor).getBytes());
                            }
                        },
                        producerConfig,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE

                ));
        env.execute();

    }
}

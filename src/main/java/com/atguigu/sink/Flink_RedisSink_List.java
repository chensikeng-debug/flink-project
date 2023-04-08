package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @author Administrator
 * @date 2022/11/9 21:51
 *
 * 案例一的类型是string,所以本案例跑不成功，需要再redis-cli下执行FLASHALL先清理一下，再操作本案例
 *
 * 前提：启动redis服务端 ~ » redis-server /etc/redis.conf
 *
 * 进入到客户端 ~ » redis-cli --raw
 *
 * 127.0.0.1:6379> keys *
 * 传感器
 * sensor_1
 * sensor_3
 * sensor_2
 *
 * LANGE sensor_1 0 -1查看key的元素列表
 *
 * 127.0.0.1:6379> LRANGE sensor_1 0 -1
 * {"id":"sensor_1","ts":1607527992000,"vc":20}
 * {"id":"sensor_1","ts":1607527994000,"vc":50}
 * {"id":"sensor_1","ts":1607527996000,"vc":50}
 *
 */
public class Flink_RedisSink_List {
    @SneakyThrows
    public static void main(String[] args) {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_3", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("传感器", 1607527995000L, 30));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        FlinkJedisConfigBase conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop162")
                .setPort(6379)
                .setMaxTotal(1000)
                .setMaxIdle(10)
                .setMinIdle(1)
                .setDatabase(0)
                .setTimeout(10000)
                .build();

        env 
                .fromCollection(waterSensors)
                .addSink(new RedisSink<>(conf, new RedisMapper<WaterSensor>() {
                    // string(set) set(sadd) list(lpush rpush) hash(hset) zet
                    // 返回一个命令描述符
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        // 第二个参数只对hash和zset有效，其它的忽略 外部的key
                        return new RedisCommandDescription(RedisCommand.RPUSH, null);
                    }

                    // key还是那个key, value变成了key对应的list
                    @Override
                    public String getKeyFromData(WaterSensor waterSensor) {
                        return waterSensor.getId();
                    }

                    @Override
                    public String getValueFromData(WaterSensor waterSensor) {
                        return JSON.toJSONString(waterSensor);
                    }
                }));

        env.execute();
    }
}

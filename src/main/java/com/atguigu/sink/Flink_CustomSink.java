package com.atguigu.sink;

import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 自定义sink
 * <p>
 * 此案例前置条件
 * create database test;
 * use test;
 * CREATE TABLE `sensor` (
 * `id` varchar(20) NOT NULL,
 * `ts` bigint(20) NOT NULL,
 * `vc` int(11) NOT NULL,
 * PRIMARY KEY (`id`,`ts`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * <p>
 * nc -lk 9999
 */
public class Flink_CustomSink {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.socketTextStream("hadoop162", 9999)
                .map(new RichMapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .addSink(new RichSinkFunction<WaterSensor>() {

                    private PreparedStatement ps;
                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName("com.mysql.jdbc.Driver");
                        // 获取连接
                        conn = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test?useSSL=false", "root", "aaaaaa");
                        // 预编译
                        // replace into 可以实现幂等
                        ps = conn.prepareStatement("REPLACE INTO sensor(id, ts, vc) VALUES(?, ?, ?)");
                    }

                    @Override
                    public void invoke(WaterSensor waterSensor, Context context) {
                        try {
                            ps.setString(1, waterSensor.getId());
                            ps.setLong(2, waterSensor.getTs());
                            ps.setInt(3, waterSensor.getVc());
                            ps.execute();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        if (ps != null) {
                            ps.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    }

                });

        env.execute();
    }
}

package com.atguigu.sink;

import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Administrator
 * @date 2022/11/12 13:18
 */
public class Flink_JDBC_SINK {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        String sql = "REPLACE INTO sensor(id, ts, vc) VALUES(?, ?, ?)";
        env.socketTextStream("hadoop162", 9999)
                .map(new RichMapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(JdbcSink.sink(
                        sql,
                        (JdbcStatementBuilder<WaterSensor>) (ps, waterSensor) -> {
                            //拿到数据，然后设置占位符
                            ps.setString(1, waterSensor.getId());
                            ps.setLong(2, waterSensor.getTs());
                            ps.setInt(3, waterSensor.getVc());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(5)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUrl("jdbc:mysql://hadoop162:3306/test?useSSL=false")
                                .withUsername("root")
                                .withPassword("aaaaaa")
                                .build()

                ));

        env.execute();

    }
}

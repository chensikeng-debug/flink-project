package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Administrator
 * @date 2022/11/10 19:40
 *
 * 启动es和kibana
 * 三台机器都执行/opt/module/elasticsearch-6.6.0/bin » ./elasticsearch -d
 * /opt/module/kibana-6.6.0/bin » nohup ./kibana &
 * 或 ./kibana &
 *
 * 进入kinaba页面 http://hadoop162:5601/app/kibana#/dev_tools/console?_g=()
 *
 * GET /_cat/indices
 *
 * GET /sensor/_search
 *
 *
 */
public class Flink_EsSink_Bounded {
    @SneakyThrows
    public static void main(String[] args) {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_3", 1607527995000L, 30));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("hadoop162", 9200));
        hosts.add(new HttpHost("hadoop163", 9200));
        hosts.add(new HttpHost("hadoop164", 9200));

        env.fromCollection(waterSensors)
                .addSink(new ElasticsearchSink.Builder<WaterSensor>(
                            hosts,
                            new ElasticsearchSinkFunction<WaterSensor>() {
                                @Override
                                public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                                           // 这里就往es中写数据了

                                    IndexRequest index = Requests
                                            .indexRequest()
                                            .index("sensor")
                                            .type("_doc") // type不能下划线开头，但是_doc例外
                                            .id(waterSensor.id) // 如果不指定就会生成随机数，id相同会覆盖
                                            .source(JSON.toJSONString(waterSensor), XContentType.JSON)     //写数据
                                            ;
                                    requestIndexer.add(index);
                                }
                            }

                ).build());

        env.execute();
    }
}

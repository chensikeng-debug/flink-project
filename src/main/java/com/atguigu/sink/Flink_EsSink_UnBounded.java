package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
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
 * 进入kinaba页面 http://hadoop162:5601/app/kibana#/dev_tools/console?_g=()
 *
 * GET /_cat/indices
 *
 * GET /sensor/_search
 * 先DELETE /sensor/
 *
 *
 *
 * 1、运行代码
 * 2、hadoop162机器执行 nc -lk 9999
 * 3、到kibana GET /sensor/_search
 *
 *
 */
public class Flink_EsSink_UnBounded {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("hadoop162", 9200));
        hosts.add(new HttpHost("hadoop163", 9200));
        hosts.add(new HttpHost("hadoop164", 9200));


        ElasticsearchSink.Builder<WaterSensor> esBuilder = new ElasticsearchSink.Builder<>(
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

        );
        esBuilder.setBulkFlushMaxActions(1); //来一条刷一条


        env.socketTextStream("hadoop162", 9999)
                .map(new RichMapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .addSink(esBuilder.build());



        env.execute();
    }
}

package com.atguigu.chapter07;

import com.atguigu.pojo.WaterSensor;
import com.atguigu.utils.MyUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * 案例：
 * 一个窗口5秒
 * 乱序容忍时间 为3秒  表示乱序的程度
 * <p>
 * 水印 = 前到来数据的时间戳 - 乱序容忍时间 - 1 毫秒
 * 水印是流里特殊的一种数据，用来表示时间，它不能倒流，只能增大更新
 * <p>
 * 有了watermark，处理时间不再重要，事件时间很重要，今天依然可以处理1970年的数据，数据也落到了正确的窗口中
 * <p>
 * 师傅等车
 * <p>
 * id,数据时间戳,other,我们看的就是数据时间戳
 * sensor_1,1,10
 * [root@hadoop162 ~]# nc -lk 9999
 * sensor_1,1,10
 * sensor_1,3,30
 * sensor_1,7,70
 * sensor_1,7,70
 * sensor_1,8,80
 * sensor_1,13,130
 * sensor_1,3,30
 * sensor_1,8,80
 * sensor_1,13,13
 * sensor_1,10,100
 * sensor_1,18,180
 * sensor_1,1800,180
 * <p>
 * key=sensor_1, window=[0, 5, 元素个数：2)
 * key=sensor_1, window=[5, 10, 元素个数：3)
 * key=sensor_1, window=[10, 15, 元素个数：3)
 * key=sensor_1, window=[15, 20, 元素个数：1)
 * <p>
 * 计算过程
 * 第1个数据过来 1000-3000-1=-2001ms [0, 5)秒未关窗，数据落在[0, 5)秒窗口      水印只会越来越大，时间不会倒流
 * 第1个数据过来 3000-3000-1=-1ms    [0, 5)秒未关窗，数据落在[0, 5)秒窗口
 * 第3个数据过来 7000-3000-1=3999ms [0, 5)秒未关窗，数据落在[5, 10)秒窗口
 * 第4个数据过来 7000-3000-1=3999ms [0, 5)秒未关窗，数据落在[5, 10)秒窗口
 * 第5个数据过来 8000-3000-1=4999ms [0, 5)秒，关窗，少一毫秒都没关窗，4999刚好关窗。 数据落在[5, 10)秒窗口
 * 关窗之后看一下落在[0,5)的数据有第1，2条
 * <p>
 * 弟6个数据过来 13000-3000-1=9999ms [5, 10)秒，关窗，少一毫秒都没关窗，9999刚好关窗。 数据落在[10, 15)秒窗口
 * 关窗之后看一下落在[5,10)的数据有第3,4,5条
 * <p>
 * 第7个数据过来 3理论上要处在[0, 5)，但是那个窗口已经关闭了，忽略
 * 第8个数据过来 8理论上要处在[5, 10)，但是那个窗口已经关闭了，忽略
 * 第9个数据过来 13000-3000-1=9999ms，数据落在[10, 15)秒窗口，相当于是9999ms的数据
 * 第10个数据过来 10000  ？？？
 * 第11个数据过来 18000-3000-1=14999ms，[10, 15)关窗，数据落在[15, 20)，秒窗口
 * 关窗之后看一下落在[10,15)的数据有6,9,10条
 * <p>
 * 第11个数据过来 1800000-3000-1=1799699ms，[15, 20 )关窗，数据落在[15, 20)，秒窗口
 */
public class Flink09_Watermark_Order {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimeStamp) {
                                        return element.getTs();
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))  // [0-4999)
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        List<WaterSensor> list = MyUtil.toList(elements);
                        out.collect("key=" + key +
                                ", window=[" + ctx.window().getStart() + ", " + ctx.window().getEnd() + "" +
                                ",元素个数：" + list.size() + ")");

                    }
                })
                .print();

        env.execute();
    }
}

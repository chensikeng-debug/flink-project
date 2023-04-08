package com.atguigu.chapter06;

import com.atguigu.pojo.OrderEvent;
import com.atguigu.pojo.TxEvent;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单和流水 对账
 */
public class Flink01_Project_Order {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3])
                    );
                })
                .filter(orderEvent -> "pay".equals(orderEvent.getEventType()));

        SingleOutputStreamOperator<TxEvent> txEventStream = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(
                            datas[0],
                            datas[1],
                            Long.valueOf(datas[2])
                    );
                });

        // 使用connect将两种不同类型的流进行连接
        ConnectedStreams<OrderEvent, TxEvent> orderTxEventStreams = orderEventStream.connect(txEventStream);
        // 连接之后想办法进入同一个key，只有进入了同一个key组的数据，才是可以对账的数据
        orderTxEventStreams.keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    Map<String, TxEvent> txMap = new HashMap();
                    Map<String, OrderEvent> orderMap = new HashMap();

                    @Override
                    public void processElement1(OrderEvent orderEvent, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context context, Collector<String> out) throws Exception {
                        TxEvent txEvent = txMap.get(orderEvent.getTxId());
                        if (null != txEvent) {
                            out.collect("tx=" + orderEvent.getTxId() + "对账成功");
                            // 对账成功之后此记录就不再比对，从记录中删除
                            txMap.remove(orderEvent.getTxId());
                        } else {
                            orderMap.put(orderEvent.getTxId(), orderEvent);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent txEvent, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context context, Collector<String> out) throws Exception {
                        OrderEvent orderEvent = orderMap.get(txEvent.getTxId());
                        if (null != orderEvent) {
                            out.collect("tx=" + orderEvent.getTxId() + "对账成功");
                            orderMap.remove(orderEvent.getTxId());
                        } else {
                            txMap.put(txEvent.getTxId(), txEvent);
                        }
                    }
                })
                .print("对账");

        env.execute();

    }
}

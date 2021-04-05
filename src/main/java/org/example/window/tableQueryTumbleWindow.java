package org.example.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author Abram
 * Desc 演示Flink Table&SQL 案例- 使用事件时间+Watermaker+window完成订单统计
 * 滚动窗口
 */
public class tableQueryTumbleWindow {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings Settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, Settings);

        //2.source
        DataStreamSource<Order> orderDS  = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        //orderDS.print();
        //时间窗口 + WaterMark
        SingleOutputStreamOperator<Order> WaterMarkDS = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, Timestamp) -> event.getCreateTime()));

        tenv.createTemporaryView("tb_order",WaterMarkDS,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());

        //3.transform
        String sqldtl = "select orderId\n" +
                ",userId \n" +
                ",money \n" +
                ",createTime \n " +
                "from tb_order\n "
                ;
        Table orderDtl = tenv.sqlQuery(sqldtl);

        String sql = "select userId\n" +
                ", count(*) as orderCount\n" +
                ", max(money) as maxMoney\n" +
                ",min(money) as minMoney\n " +
                ",min(createTime) \n" +
                ",tumble_start(createTime, INTERVAL '5' SECOND) \n" +
                ",tumble_end(createTime, INTERVAL '5' SECOND) \n" +
                "from tb_order\n " +
                "group by userId,tumble(createTime, INTERVAL '5' SECOND) "
                ;

        Table orderSmry = tenv.sqlQuery(sql);

        //orderTb.printSchema();

        //4.sink
        //toAppendStream → 将计算后的数据append到结果DataStream中去
        //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        //DataStream<Order> resultDS = tenv.toAppendStream(result, Order.class);
        DataStream<Tuple2<Boolean, Row>> resultDtl = tenv.toRetractStream(orderDtl, Row.class);
        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(orderSmry, Row.class);

        resultDtl.print("明细数据");
        resultDS.print("聚合数据");


        //5.execute
        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;//事件时间
    }
}

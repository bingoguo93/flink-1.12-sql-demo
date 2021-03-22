package org.example.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;


public class tableQueryWordCount {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings Settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, Settings);

        //2.source
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2),
                new Order(1L, "beer", 3)
        ));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)
        ));

        //3.transform
        tenv.<Order>createTemporaryView("tableA", orderA, $("user"), $("product"), $("amount"));
        tenv.<Order>createTemporaryView("tableB", orderB, $("user"), $("product"), $("amount"));

        //查询合并
        String sql = "select product,count(*) as cnt,sum(amount) as amount from tableA group by product "
        ;

        Table result = tenv.sqlQuery(sql);

        result.printSchema();
        System.out.println(result);
        //4.sink
        //toAppendStream → 将计算后的数据append到结果DataStream中去
        //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        //DataStream<Order> resultDS = tenv.toAppendStream(result, Order.class);
        DataStream<Tuple2<Boolean, OrderSmary>> resultDS = tenv.toRetractStream(result, OrderSmary.class);

        resultDS.print();

        //5.execute
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderSmary {
        public String product;
        public long cnt;
        public int amount;
    }
}

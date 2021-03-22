package org.example.sql;

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
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author Abram
 * Desc 演示Flink Table&SQL 案例- 读写Mysql MysqlCDC
 * Mysql create table test(id int ,name varchar(50) ,primary key(id) )
 */
public class tableQueryMysql {
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
                    Order order = new Order( random.nextInt(20), "Abram"+random.nextInt(100), System.currentTimeMillis());
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

        tenv.createTemporaryView("tb_order",WaterMarkDS,$("id"),$("name"),$("createTime").rowtime());


        //3.create table 普通表
        String sqlCreate = "create table mysql_test(                          \n " +
                           "   id int                                         \n " +
                           "   ,name String                                   \n " +
                           "   ,primary key (id)     not ENFORCED                " +
                           ")                                                 \n " +
                           "WITH (                                            \n " +
                           "   'connector' = 'jdbc'                           \n " +
                           "   ,'url' = 'jdbc:mysql://localhost:3306/imh_core'\n " +
                           "   ,'table-name' = 'test'                         \n " +
                           "   ,'username'='root'                             \n " +
                           "   ,'password'='Aa123456!'                        \n " +
                           "   ,'driver'='com.mysql.cj.jdbc.Driver'              " +
                           ")                                                    "
                           ;

        tenv.executeSql(sqlCreate);

        //3.create table cdc表
        String sqlcdcCreate = "create table mysqlcdc_test(                          \n " +
                              "   id int                                         \n " +
                              "   ,name String                                   \n " +
                              "   ,primary key (id)     not ENFORCED                " +
                              ")                                                 \n " +
                              "WITH (                                            \n " +
                              "   'connector' = 'mysql-cdc',\n" +
                              "  'hostname' = 'localhost',\n" +
                              "  'port' = '3306',\n" +
                              "  'username' = 'root',\n" +
                              "  'password' = 'Aa123456!',\n" +
                              "  'database-name' = 'imh_core',\n" +
                              "  'table-name' = 'test'              " +
                              ")                                                    "
                              ;
        tenv.executeSql(sqlcdcCreate);

        //sink
        String sqlInset = "insert into mysql_test select id ,name from tb_order" ;

        tenv.executeSql(sqlInset);

        //read 普通表
        String sql = "select * from mysql_test";
        Table orderSmry = tenv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(orderSmry, Row.class);

        resultDS.print("mysql普通表数据");


        //read CDC表
        String sqlcdc = "select * from mysqlcdc_test";
        Table testcdc = tenv.sqlQuery(sqlcdc);

        DataStream<Tuple2<Boolean, Row>> resultCDC = tenv.toRetractStream(testcdc, Row.class);

        resultCDC.print("mysqlCDC表数据");

        //5.execute
        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private Integer id;
        private String name;
        private Long createTime;//事件时间
    }
}
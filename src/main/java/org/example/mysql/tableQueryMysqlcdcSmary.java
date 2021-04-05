package org.example.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author Abram Guo
 * Desc 演示Flink Table&SQL 测试tempral Join 基于proctime
 *
 * Mysql主表 create table test(id int ,name varchar(50) ,primary key(id) );
 * Mysql维表 create table test_dim(id int ,name varchar(50) ,primary key(id) );
 * Mysql结果表 create table test_result(id int ,name varchar(50),proctime datetime ,id_dim int ,name_dim varchar(50) ,primary key(id) );
 *
 * Mysql DML 语句 ，可在MySQL客户端操作test1维表，看看维表变化之后关联结果变化
 * 帮助理解基于事件时间的tempral join
 *         truncate table test_dim;
 *         select * from test_dim;
 *         insert into test_dim select * from test;
 *         update test_dim set name='222';
 *
 * 优化参数
 * 维表
 * "   ,'lookup.cache.ttl'='20s'                         " +
 * "   ,'lookup.cache.max-rows'='100'                    " +
 * 结果表
 * "   ,'sink.buffer-flush.interval'='1s'                         " +
 * "   ,'sink.buffer-flush.max-rows'='1000'                    " +
 */
public class tableQueryMysqlcdcSmary {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settingS = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settingS);

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

        //source create table 普通表 test
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
                "   ,'lookup.cache.ttl'='20s'                         " +
                "   ,'lookup.cache.max-rows'='100'                    " +
                ")                                                    "
                ;

        tenv.executeSql(sqlCreate);
        //source sink test
        String sqlInset = "insert into mysql_test select id ,name from tb_order" ;

        tenv.executeSql(sqlInset);

        //3.create table

        //mysql_dim
        String sqlCreate1 = "create table mysql_dim(                          \n " +
                "   id int                                         \n " +
                "   ,name String                                   \n " +
                "   ,primary key (id)     not ENFORCED                " +
                ")                                                 \n " +
                "WITH (                                            \n " +
                "   'connector' = 'jdbc'                           \n " +
                "   ,'url' = 'jdbc:mysql://localhost:3306/imh_core'\n " +
                "   ,'table-name' = 'test_dim'                         \n " +
                "   ,'username'='root'                             \n " +
                "   ,'password'='Aa123456!'                        \n " +
                "   ,'driver'='com.mysql.cj.jdbc.Driver'              " +
                "   ,'lookup.cache.ttl'='10s'                         " +
                "   ,'lookup.cache.max-rows'='100'                    " +
                ")                                                    "
                ;

        tenv.executeSql(sqlCreate1);


        //mysql_result
        String sqlmysql_result = "create table mysql_result(                          \n " +
                "   id int                                         \n " +
                "   ,name String                                   \n " +
                "   ,proctime timestamp(3)                         \n " +
                "   ,id_dim int                                         \n " +
                "   ,name_dim String                                   \n " +
                "   ,primary key (id)     not ENFORCED                " +
                ")                                                 \n " +
                "WITH (                                            \n " +
                "   'connector' = 'jdbc'                           \n " +
                "   ,'url' = 'jdbc:mysql://localhost:3306/imh_core'\n " +
                "   ,'table-name' = 'test_result'                         \n " +
                "   ,'username'='root'                             \n " +
                "   ,'password'='Aa123456!'                        \n " +
                "   ,'driver'='com.mysql.cj.jdbc.Driver'              " +
                "   ,'sink.buffer-flush.interval'='1s'                         " +
                "   ,'sink.buffer-flush.max-rows'='100'                    " +
                ")                                                    "
                ;

        tenv.executeSql(sqlmysql_result);

        //3.create table cdc表 test
        String sqlcdcCreate = "create table mysqlcdc_test(                          \n " +
                              "   id int                                         \n " +
                              "   ,name String                                   \n " +
                              "   ,proctime as  proctime()                      \n " +
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





        //read CDC表
        //String sqlcdc = "select * from mysqlcdc_test";
        //Table testcdc = tenv.sqlQuery(sqlcdc);
        //DataStream<Tuple2<Boolean, Row>> resultCDC = tenv.toRetractStream(testcdc, Row.class);
        //resultCDC.print("mysqlCDC表数据");

        //关联 基于事件时间的tempral join
        String sqlSmry = "insert into mysql_result " +
                "select a.id" +
                ",a.name" +
                //",localtimestamp " +
                ",a.proctime " +
                ",b.id" +
                ",b.name " +
                "from mysqlcdc_test as a \n" +
                "left join mysql_dim for SYSTEM_TIME as OF a.proctime as b \n" +
                "on a.id = b.id \n" ;
        tenv.executeSql(sqlSmry);


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

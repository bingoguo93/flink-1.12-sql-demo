package org.example.filesystem;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Abram Guo
 * @date 2021-04-05 10:19
 */
public class fileSinkNew {
    public static void main(String[] args) throws Exception {
//env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settingS = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settingS);

//source 目标表
        String sqlParquet="CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        VARCHAR(100),\n" +
                "    order_time   TIMESTAMP(3)\n" +
                "    ) WITH (\n" +
                "    'connector' = 'filesystem'\n" +
                "    ,'path' = 'data/output/Orders'\n" +
//              "    ,'format' = 'parquet'\n" +
                "    ,'format' = 'json'\n" +
//                "    ,'sink.shuffle-by-partition.enable' = 'true'\n" +
//                "    ,'sink.rolling-policy.file-size' = '128MB'\n" +
//                "    ,'compaction.file-size' = '128MB'\n " +
                "    )"
                ;

        tenv.executeSql(sqlParquet);

//datagen 数据生成器
        String sqlOrders="CREATE TEMPORARY TABLE genOrders \n" +
                "    WITH (\n" +
                "    'connector' = 'datagen'\n" +
//                "    ,'number-of-rows' = '100000'" +
                "    ,'rows-per-second' = '100000'" +
                "    )" +
                "    like Orders(EXCLUDING ALL)"
                ;

        tenv.executeSql(sqlOrders);

//Sink 写入
        String sqlInsert="insert into Orders select * from genOrders";
        tenv.executeSql(sqlInsert);


    }

}

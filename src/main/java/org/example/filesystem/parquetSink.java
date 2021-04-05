package org.example.filesystem;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Abram Guo
 * @date 2021-04-05 10:19
 */
public class parquetSink {
    public static void main(String[] args) throws Exception {
//env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settingS = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settingS);

//source
        String sqlOrders="CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        VARCHAR(100),\n" +
                "    order_time   TIMESTAMP(3)\n" +
                "    ) WITH (\n" +
                "    'connector' = 'datagen'\n" +
                "    ,'number-of-rows' = '100'" +
                "    )"
        ;

        tenv.executeSql(sqlOrders);

        String sqlParquet="CREATE TABLE Parquet (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        VARCHAR(100),\n" +
                "    order_time   TIMESTAMP(3)\n" +
                "    ) WITH (\n" +
                "    'connector' = 'filesystem'\n" +
                "    ,'path' = 'data/output/Orders'\n" +
                "    ,'format' = 'parquet'" +
                "    )"
                ;

        tenv.executeSql(sqlParquet);
//Sink
        String sqlInsert="insert into Parquet select * from Orders";
        tenv.executeSql(sqlInsert);


    }

    /*@Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Orders {
        private BigInt order_number;
        private double price;
        private String buyer;
        private Timestamp order_time;
    }*/
}

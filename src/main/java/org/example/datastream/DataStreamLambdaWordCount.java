package org.example.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DataStreamLambdaWordCount {


    public static void main(String[] args) throws Exception {
        //0.env
        // **********************
        // FLINK STREAMING QUERY
        // **********************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //batch
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //streaming
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //auto
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //1.source
        DataStreamSource<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");


        //socker源 nc -lk 9999
        //DataStreamSource<String> lines = env.socketTextStream("cdh6.com", 9999);

        //lines.print();

        //2.切割
        /*
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value 每一行数据
                String[] arr = value.split(" ");
                for (String word : arr
                ) {
                    out.collect(word);

                }

            }
        });
         */
        SingleOutputStreamOperator<String> words = lines.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
        ).returns(Types.STRING);

        //切割
        /*
        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value 单词
                return Tuple2.of(value, 1);
            }
        });
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(
                (String value) -> Tuple2.of(value, 1)
        ).returns(Types.TUPLE(Types.STRING, Types.INT));

        //分组
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);


        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);



        //3.sink
        result.print();

        //4.TODO启动并等待程序结束
       env.execute();
    }

    }

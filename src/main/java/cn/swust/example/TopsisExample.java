package cn.swust.example;

import cn.swust.algorithms.topsis.Topsis;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.ml.common.window.EventTimeTumblingWindows;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class TopsisExample {
    public static void main(String[] args) throws Exception {
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度1
        env.setParallelism(5);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //构建数据
        List<DenseVector> INPUT_DATA =
                Arrays.asList(
                        Vectors.dense(9.0,10.0,175.0,120.0),
                        Vectors.dense(8.0,7.0,164.0,80.0),
                        Vectors.dense(6.0,3.0,157.0,90.0));

        //构建带有时间戳的数据
       /* DataStreamSource<Row> inputData = env.fromCollection(getTestData());
        Table tableWithEvent = createTableWithEvent(inputData, tEnv);
        tEnv.createTemporaryView("tableWithEvent", tableWithEvent);
        tEnv.sqlQuery("select * from tableWithEvent").execute().print();
        Topsis topsis = new Topsis();
        //Open,High,Low,Close,Adj Close,Volume
        topsis.setCriteriaTypes(1, 1, 1, 2,2,2);
        topsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);
        //3天
        //设定窗口策略
        topsis.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));
        Table[] transform = topsis.transform(tableWithEvent);
*/


        DataStreamSource<DenseVector> test = env.fromCollection(INPUT_DATA);
        Table table = tEnv.fromDataStream(test).as("features");
        Topsis topsis = new Topsis();
        topsis.setCriteriaTypes(1, 2, 3, 4);
        topsis.setWeights(0.4,0.3,0.2,0.1);
        topsis.setBestValue(165.0);
        topsis.setIntervalValue(90.0,100.0);

        Table[] transform = topsis.transform(table);
        DataStream<Row> rowDataStream = tEnv.toDataStream(transform[0]);
        // 收集结果
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        for (Row result : results) {
            System.out.println(result.toString());
        }
//        env.execute();
    }
}

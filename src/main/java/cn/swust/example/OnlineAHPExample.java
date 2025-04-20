package cn.swust.example;

import cn.swust.algorithms.ahp.AHP;
import cn.swust.algorithms.ahp.OnlineAHP;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.window.EventTimeTumblingWindows;
import org.apache.flink.ml.linalg.DenseVector;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class OnlineAHPExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);  // 设置全局并行度为 1
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //构建带有时间戳的数据
        DataStreamSource<Row> inputData = env.fromCollection(getTestData());
        Table table = createTableWithEvent(inputData, tEnv);

        Double [][] judgmentMatrix = {
                {1.0,0.5,4.0,3.0,3.0,2.0},
                {2.0,1.0,7.0,5.0,5.0,4.0},
                {0.25,0.143,1.0,0.5,0.67,0.4},
                {0.67,0.2,2.0,1.0,1.0,0.67},
                {0.33,0.2,3.0,1.0,1.0,2.0},
                {0.5,0.25,2.5,3.0,0.5,1.0}
        };
        Integer[] type = {1,1,1,0,0,0};
        OnlineAHP onlineAHP = new OnlineAHP();
        onlineAHP.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        onlineAHP.setJudgmentMatrix(judgmentMatrix);
        onlineAHP.setIndicatorType(type);
        //设置时间窗口长度 3天
        onlineAHP.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        Table tb = onlineAHP.transform(table)[0];
        DataStream<Row> rowDataStream = tEnv.toDataStream(tb);
        // 收集结果
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        System.out.println(tb.getResolvedSchema());
        for (Row result : results) {
            System.out.println(result.toString());
        }
//        env.execute();
    }

    public static List<Row> getTestData() {
        String csvFile = "src/resources/Stock Test Data.csv";
        List<Row> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 跳过标题行
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                Row row = new Row(strs.length);
                row.setField(0,parseTime(strs[0]));

                for (int i = 1; i < strs.length; i++) {
                   row.setField(i, Double.parseDouble(strs[i]));
                }
                rows.add(row);
            }

        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return rows;
    }

    public static Long parseTime(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        try {
            Date date = sdf.parse(time);
            return date.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public static Table createTableWithEvent(DataStream<Row> inputStream, StreamTableEnvironment tEnv) {
        //为每一条数据分配时间戳
        DataStream<Row> inputStreamWithEventTime =
                inputStream.assignTimestampsAndWatermarks(
                        //生成水印时间的策略，严格升序
                        WatermarkStrategy.<Row>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        //返回新的时间戳，以自己给定的时间戳
                                        (SerializableTimestampAssigner<Row>)
                                                (element, recordTimestamp) ->
                                                        element.getFieldAs(0)));
        Table inputTableWithEventTime =
                tEnv.fromDataStream(
                                inputStreamWithEventTime,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.BIGINT())
                                        .column("f1", DataTypes.DOUBLE())
                                        .column("f2", DataTypes.DOUBLE())
                                        .column("f3", DataTypes.DOUBLE())
                                        .column("f4", DataTypes.DOUBLE())
                                        .column("f5", DataTypes.DOUBLE())
                                        .column("f6", DataTypes.DOUBLE())
                                        //由数据给定的时间生成带时区的水印时间
                                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                        //使用数据源提供的水印，水印用于处理事件时间语义
                                        .watermark("rowtime", "SOURCE_WATERMARK()")
                                        .build())
                        .as("id", "High","Low","Open","Close","Volume","Marketcap");
        return inputTableWithEventTime;
    }

}

package cn.swust.example;

import cn.swust.algorithms.topsis.OnlineTopsis;
import cn.swust.algorithms.topsis.OnlineTopsisModel;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OnlineTopsisExample {
    public static void main(String[] args) throws Exception {
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度1
        env.setParallelism(5);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //构建带有时间戳的数据
        DataStreamSource<Row> inputData = env.fromCollection(getTestData());
        Table tableWithEvent = createTableWithEvent(inputData, tEnv);
        tEnv.createTemporaryView("tableWithEvent", tableWithEvent);
//        tEnv.sqlQuery("select * from tableWithEvent").execute().print();

        OnlineTopsis ontopsis = new OnlineTopsis();
        //Open,High,Low,Close,Adj Close,Volume
        ontopsis.setCriteriaTypes(1,1,1,2,2,2);
        ontopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);
        //3天
        //设定窗口策略
        ontopsis.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));


        OnlineTopsisModel model = ontopsis.fit(tableWithEvent);
        Table[] modelData = model.getModelData();

        Table table = model.transform(tableWithEvent)[0];
        table.printSchema();
        DataStream<Row> rowDataStream = tEnv.toDataStream(table);
        // 收集结果
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        for (Row result : results) {
            System.out.println(result.toString());
        }
//        env.execute();
    }
    public static List<Row> getTestData() {
        String csvFile = "src/resources/NFLX.csv";
        List<Row> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 跳过标题行
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                Row row = new Row(2);
                row.setField(0,parseTime(strs[0]));

                DenseVector dv = new DenseVector(strs.length-1);
                for (int i = 1; i < strs.length; i++) {
                    dv.set(i-1, Double.parseDouble(strs[i]));
                }
                row.setField(1,dv);
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
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
                                        .column("f1", DataTypes.RAW(DenseVectorTypeInfo.INSTANCE))
                                        //由数据给定的时间生成带时区的水印时间
                                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                        //使用数据源提供的水印，水印用于处理事件时间语义
                                        .watermark("rowtime", "SOURCE_WATERMARK()")
                                        .build())
                        .as("id", "features");
        return inputTableWithEventTime;
    }
}


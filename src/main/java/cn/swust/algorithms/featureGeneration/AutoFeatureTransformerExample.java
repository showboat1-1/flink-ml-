package cn.swust.algorithms.featureGeneration;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class AutoFeatureTransformerExample {

    public static void main(String[] args) throws Exception {
        // 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建输入数据.
        // 假设我们有两列：一个是日期类型字符串（作为时间类型列），另一个是数值类型列
        List<Row> inputData = Arrays.asList(
                Row.of("2025-01-01", 10.5,1),
                Row.of("2025-01-02", 20.0,0),
                Row.of("2025-01-03", null,1),
                Row.of("2025-01-04", 15.0,2),
                Row.of("2025-01-05", null,0)
        );

        // 定义 RowTypeInfo，显式指定字段名和类型
        String[] inputCols = new String[]{"date", "value","feature"}; // 指定字段名为 "date" 和 "value"
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.DOUBLE,Types.INT},
                inputCols
        );

        // 定义输入数据 DataStream, 指定类型信息
        DataStream<Row> dataStream = env.fromCollection(inputData, rowTypeInfo);

        // 将 DataStream 转换为 Table，并显式设置字段名称
        Table inputTable = tEnv.fromDataStream(dataStream);

        // 创建 AutoFeatureTransformer 算子
        AutoFeatureTransformer transformer = new AutoFeatureTransformer();

        // 处理数据并返回转换后的表
        Table[] resultTables = transformer.transform(inputTable);

        // 获取转换后的结果表
        Table resultTable = resultTables[0];

        System.out.println("均值填充后的数据：");
        tEnv.createTemporaryView("tt1",resultTable);
        tEnv.sqlQuery("select * from tt1").execute().print();

        // 启动 Flink 作业
        env.execute();
    }
}

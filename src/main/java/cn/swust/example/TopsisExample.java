package cn.swust.example;

import cn.swust.algorithms.topsis.Topsis;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.table.api.Expressions.$;

public class TopsisExample {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        // 定义数据
//        String[] columns = {"DateColumns", "Accuracy", "Recall", "F1Score", "Precisions", "AUC", "Specificity"};
//        Object[][] data = {
//                {"2023-01-01", 0.41, 0.98, 0.75, 0.6, 0.14, 0.12},
//                {"2023-01-02", 0.06, 0.89, 0.62, 0.71, 0.0, 1.0},
//                {"2023-01-03", 0.92, 0.19, 0.18, 0.17, 0.29, 0.52},
//                {"2023-01-04", 0.47, 0.27, 0.63, 0.13, 0.28, 0.35},
//                {"2023-01-05", 0.5, 0.8, 0.2, 0.51, 0.59, 0.0},
//                {"2023-01-06", 0.67, 0.14, 0.06, 0.96, 0.98, 0.83},
//                {"2023-01-07", 0.33, 0.07, 0.7, 0.44, 0.11, 0.49},
//                {"2023-01-08", 0.03, 0.93, 0.26, 0.67, 0.3, 0.51},
//                {"2023-01-09", 0.6, 0.16, 1.0, 0.78, 0.95, 0.92},
//                {"2023-01-10", 0.66, 0.95, 0.08, 0.18, 0.03, 0.3},
//                {"2023-01-11", 0.42, 0.25, 0.85, 0.35, 0.27, 0.54},
//                {"2023-01-12", 0.15, 0.82, 0.07, 1.0, 0.78, 0.16},
//                {"2023-01-13", 0.0, 0.83, 0.73, 0.73, 0.78, 0.03},
//                {"2023-01-14", 0.39, 0.08, 0.89, 0.63, 0.32, 0.02},
//                {"2023-01-15", 0.34, 0.31, 0.75, 0.64, 0.9, 0.46},
//                {"2023-01-16", 0.13, 0.72, 0.78, 0.56, 0.78, 0.48},
//                {"2023-01-17", 0.57, 0.42, 0.02, 0.09, 0.01, 0.64},
//                {"2023-01-18", 0.34, 0.5, 0.94, 0.24, 0.4, 0.77}
//
//        };
//        // 定义 RowTypeInfo
//        TypeInformation<?>[] typeInfos = new TypeInformation<?>[]{
//                TypeInformation.of(String.class),
//                TypeInformation.of(Double.class),
//                TypeInformation.of(Double.class),
//                TypeInformation.of(Double.class),
//                TypeInformation.of(Double.class),
//                TypeInformation.of(Double.class),
//                TypeInformation.of(Double.class)
//        };
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos, columns);
//
//        // 使用 fromCollection 并传入 List 而不是直接使用 fromElements
//        DataStream<Row> dataStream = env.fromCollection(Arrays.asList(data))
//                .map(row -> Row.of((Object[]) row))
//                .returns(rowTypeInfo); // 显式设置 RowTypeInfo
//
//        // 使用 fromDataStream 方法，提供字段名以映射到 Row 的字段
//        Table inputTable = tEnv.fromDataStream(dataStream,
//                $("DateColumns"),
//                $("Accuracy"),
//                $("Recall"),
//                $("F1Score"),
//                $("Precisions"),
//                $("AUC"),
//                $("Specificity")
//        );
//
//        // 定义输入特征列和输出列
//        double[] weights = {1, 1, 1, 1, 1, 1};// 示例权重
//        String[] inputCols = {"Accuracy", "Recall", "F1Score", "Precisions", "AUC", "Specificity"};
        String outputCol = "Topsis_Score";

        double[] weights = null;
        //读取数据
        String csvFile = "src/resources/yahoo_stock.csv";
        List<Row> rows = new ArrayList<>();
        String[] inputCols = null;
        String[] coloums = null;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 获取列名
            String line = br.readLine();
            coloums = line.split(",");
            inputCols = Arrays.copyOfRange(coloums, 1, coloums.length);
            weights = new double[inputCols.length];
            Arrays.fill(weights, 1.0);
            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                Row row = new Row(strs.length);
                row.setField(0,String.valueOf(strs[0]));
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

        //转为Table
        DataStreamSource<Row> rowData = env.fromCollection(rows);
        Table test = tEnv.fromDataStream(rowData,
                $("DateColumns"),
                $("High"),
                $("Low"),
                $("OpenPrice"),
                $("ClosePrice"),
                $("Volume"),
                $("AdjClose"));
//        tEnv.createTemporaryView("test", test);
//        tEnv.sqlQuery("select * from test").execute().print();
//
//        System.out.println(Arrays.toString(coloums));
//        System.out.println(Arrays.toString(weights));
//        System.out.println(Arrays.toString(inputCols));

        // 初始化 Topsis 算法实例
        Topsis topsis = new Topsis();
        topsis.setInputCols(inputCols);
        topsis.setOutputCol(outputCol);
        topsis.setWeights(weights);

        // 调用 Topsis 算法进行数据处理
        Table result = topsis.transform(new Table[]{test})[0];
        Table topScores = result.select($("DateColumns"), $("Topsis_Score"));
        DataStream<Row> stream = tEnv.toChangelogStream(topScores);
        //收集结果，排序展示
        List<Row> results = IteratorUtils.toList(stream.executeAndCollect());
        Collections.sort(results, new Comparator<Row>() {
            @Override
            public int compare(Row o1, Row o2) {
                return (Double)o1.getField(1)>(Double)o2.getField(1) ? 1:-1;
            }
        });
        for (Row row : results) {
            System.out.println(row.toString());
        }
//        env.execute();
    }
}

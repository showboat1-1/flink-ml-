package cn.swust.example;

import cn.swust.algorithms.apriori.Apriori;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;


public class AprioriExample {
    public static void main(String[] args) throws Exception {
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度1
        env.setParallelism(1);

        //读取数据源
        List<Row> testData = getTestData();
        DataStreamSource<Row> strDS = env.fromCollection(testData);
        SingleOutputStreamOperator<Row> rowStream = strDS.map(r -> r, Types.ROW(Types.STRING));

        env.execute();

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //获取表数据
        Table tmpTable = tableEnv.fromDataStream(rowStream, $("items"));
        Apriori apr = new Apriori();
        apr.setMinSupport(0.02);
        apr.setMinConfidence(0.02);
        apr.setItemSeparator("/");

        //应用算法
        Table[] transform = apr.transform(tmpTable);

        //创建临时视图
        tableEnv.createTemporaryView("tt", transform[0]);

        //输出结果
        tableEnv.sqlQuery("select * from tt").execute().print();
    }

    public static List<Row> getTestData() {
        String csvFile = "src/resources/Groceries_dataset.csv";
        List<Row> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 跳过标题行
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                //获取最后一列数据
                rows.add(Row.of(strs[strs.length - 1]));
            }

        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return rows;
    }
}

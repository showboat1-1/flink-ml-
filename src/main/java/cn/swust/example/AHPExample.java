package cn.swust.example;

import cn.swust.algorithms.ahp.AHP;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class AHPExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);  // 设置全局并行度为 1
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> test = env.fromCollection(
                getTestData(),
                Types.ROW(Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE));
        Table table = tEnv.fromDataStream(test).as("High","Low","Open","Close","Volume","Marketcap");

        Double [][] judgmentMatrix = {
                {1.0,0.5,4.0,3.0,3.0,2.0},
                {2.0,1.0,7.0,5.0,5.0,4.0},
                {0.25,0.143,1.0,0.5,0.67,0.4},
                {0.67,0.2,2.0,1.0,1.0,0.67},
                {0.33,0.2,3.0,1.0,1.0,2.0},
                {0.5,0.25,2.5,3.0,0.5,1.0}
        };
        Integer[] type = {1,1,1,0,0,0};
        AHP ahp = new AHP();
        ahp.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        ahp.setJudgmentMatrix(judgmentMatrix);
        ahp.setIndicatorType(type);
        Table tb = ahp.transform(table)[0];
        tEnv.createTemporaryView("ttt",tb);
        tEnv.sqlQuery("select * from ttt").execute().print();

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
                Row row = new Row(strs.length-1);
                for (int i = 1; i < strs.length; i++) {
                    row.setField(i-1, Double.parseDouble(strs[i]));
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
}

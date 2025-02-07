package cn.swust.example;

import cn.swust.algorithms.canopy.Canopy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CanopyExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);  // 设置全局并行度为 1
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<DenseVector> testData = getTestData();
        DataStreamSource<DenseVector> test = env.fromCollection(testData);


        Table table = tEnv.fromDataStream(test).as("features");

        Canopy canopy = new Canopy();
        canopy.setSeed(42L);
        canopy.setT1(1.0);
        canopy.setT2(0.5);
        canopy.setMaxIter(100);
        Table[] transform = canopy.transform(table);
        System.out.println(Arrays.toString(TableUtils.getRowTypeInfo(transform[0].getResolvedSchema()).getFieldNames()));
        tEnv.toDataStream(transform[0]).map(r->{
            System.out.println(r);
            return r;
        },Types.ROW(TypeInformation.of(DenseVector.class),TypeInformation.of(DenseVector[].class)));

        env.execute();
    }
    public static List<DenseVector> getTestData() {
        String csvFile = "src/resources/Iris_nofield_name.csv";
        List<DenseVector> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 跳过标题行
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                DenseVector dv = new DenseVector(strs.length-1);
                for (int i = 0; i < strs.length-1; i++) {
                    dv.set(i, Double.parseDouble(strs[i]));
                }
                //获取最后一列数据
                rows.add(dv);
            }

        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return rows;
    }
}

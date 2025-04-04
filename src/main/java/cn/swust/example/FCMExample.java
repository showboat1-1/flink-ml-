package cn.swust.example;

import cn.swust.algorithms.fcm.FCM;
import cn.swust.algorithms.fcm.FCMModel;
import cn.swust.algorithms.fcm.FCMModelData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FCMExample {
    public static void main(String[] args) throws Exception {
        List<DenseVector> testData = getTestData();

        StreamExecutionEnvironment env;
        StreamTableEnvironment tEnv;
        Table dataTable;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        tEnv = StreamTableEnvironment.create(env);
        dataTable = tEnv.fromDataStream(env.fromCollection(testData));

        Table input = dataTable.as("features");

        FCM fcm = new FCM();
        fcm.setSeed(42);
        fcm.setMaxIter(100);

        FCMModel model = fcm.fit(input);
        Table[] modelData = model.getModelData();
        FCMModelData.getModelDataStream(modelData[0]).map(r->{
            for (DenseVector c : r.centroids) {
                System.out.println("聚类中心=\t"+c);
            }
            for (Tuple2<DenseVector, DenseVector> m : r.membershipMatrix) {
                System.out.println("样本=\t"+m.f0+"\t隶属度=\t"+m.f1);
            }
            return r;
        });

        Table[] transform = model.transform(input);
       tEnv.toDataStream(transform[0]).print();

        env.execute();
    }

    public static List<DenseVector> getTestData() {
        String csvFile = "src/resources/Iris.csv";
        List<DenseVector> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 跳过标题行
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] strs = line.split(",");
                //去掉第一列和最后一列
                DenseVector dv = new DenseVector(strs.length - 2);
                for (int i = 1; i < strs.length - 1; i++) {
                    dv.set(i - 1, Double.parseDouble(strs[i]));
                }
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


package cn.swust.algorithms.canopy;


import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.CosineDistanceMeasure;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiFunctionWithException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CanopyTest  extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTable;

    private static final List<DenseVector> INPUT_DATA =
            Arrays.asList(
                    new DenseVector(new double[]{1.0, 1.5}),
                    new DenseVector(new double[]{1.5, 2.0}),
                    new DenseVector(new double[]{0.5, 0.7}),
                    new DenseVector(new double[]{6.0, 7.5}),
                    new DenseVector(new double[]{5.5, 7.0}),
                    new DenseVector(new double[]{5.5, 4.5}));

    public static <T extends Stage<T>> T saveAndReload(
            StreamTableEnvironment tEnv,
            T stage,
            String path,
            BiFunctionWithException<StreamTableEnvironment, String, T, IOException> loadFunc)
            throws Exception {
        StreamExecutionEnvironment env = TableUtils.getExecutionEnvironment(tEnv);

        stage.save(path);
        try {
            env.execute();
        } catch (RuntimeException e) {
            if (!e.getMessage()
                    .equals("No operators defined in streaming topology. Cannot execute.")) {
                throw e;
            }
        }

        return loadFunc.apply(tEnv, path);
    }

    /**
     * Gets a {@link StreamExecutionEnvironment} with the most common configurations of the Flink ML
     * program.
     */
    public static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);//Test中限制了并发度不能大于4
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    @Before
    public void before() {
        env = getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        inputDataTable =
                tEnv.fromDataStream(env.fromCollection(INPUT_DATA).map(x -> x)).as("features");
    }

    @Test
    public void testParam() {
        Canopy canopy = new Canopy();
        assertEquals("features", canopy.getFeaturesCol());
        assertEquals(1.0,canopy.getT1(), 0.00001);
        assertEquals(0.5,canopy.getT2(), 0.00001);
        assertEquals(20,canopy.getMaxIter());
        assertEquals(EuclideanDistanceMeasure.NAME, canopy.getDistanceMeasure());
        assertEquals("prediction", canopy.getPredictionCol());
        assertEquals(Canopy.class.getName().hashCode(), canopy.getSeed());

        canopy
                .setFeaturesCol("test_features")
                .setDistanceMeasure(CosineDistanceMeasure.NAME)
                .setPredictionCol("cluster_id")
                .setT1(2.0)
                .setT2(1.0)
                .setMaxIter(80)
                .setSeed(42L);

        assertEquals("test_features", canopy.getFeaturesCol());
        assertEquals(CosineDistanceMeasure.NAME, canopy.getDistanceMeasure());
        assertEquals("cluster_id", canopy.getPredictionCol());
        assertEquals(2.0,canopy.getT1(),0.00001);
        assertEquals(1.0,canopy.getT2(),0.00001);
        assertEquals(80,canopy.getMaxIter());
        assertEquals(42L,canopy.getSeed());
    }

    @Test
    public void testOutputSchema() throws Exception {
        Table input = inputDataTable.as("test_feature");

        Canopy canopy =
                new Canopy().setFeaturesCol("test_feature").setPredictionCol("test_prediction");
        Table output = canopy.transform(input)[0];
        env.execute();

        assertEquals(
                Arrays.asList("centroids", "test_prediction"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testSaveLoadTransform() throws Exception {
        Canopy canopy =
                new Canopy()
                        .setPredictionCol("pred")
                        .setSeed(42L);

        canopy =
                saveAndReload(
                        tEnv,
                        canopy,
                        tempFolder.newFolder().getAbsolutePath(),
                        Canopy::load);

        Table[] outputs = canopy.transform(inputDataTable);

        DataStream<Row> rowDataStream = tEnv.toDataStream(outputs[0]);
        rowDataStream.print();
        env.execute();

        //收集结果和产生顺序不一致，每一次运行选择的随机数不一致不便检查
//        List<Row> list = IteratorUtils.toList(rowDataStream.executeAndCollect());
    }

}

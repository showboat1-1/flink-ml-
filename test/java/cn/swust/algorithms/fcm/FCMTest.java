package cn.swust.algorithms.fcm;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.SparseVectorTypeInfo;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiFunctionWithException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/** Tests {@link FCM} and {@link FCMModel}. */
public class FCMTest extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    private static final List<DenseVector> DATA =
            Arrays.asList(
            Vectors.dense(1.0, 2.0),
            Vectors.dense(1.5, 1.8),
            Vectors.dense(5.0, 8.0),
            Vectors.dense(8.0, 8.0),
            Vectors.dense(1.0, 0.6),
            Vectors.dense(9.0, 11.0));
    private static final List<Set<DenseVector>> expectedGroups =
            Arrays.asList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(9.0, 11.0))),
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(8.0, 8.0),
                                    Vectors.dense(5.0, 8.0))),
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(1.0, 2.0),
                                    Vectors.dense(1.5, 1.8),
                                    Vectors.dense(1.0, 0.6))));
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private Table dataTable;

    @Before
    public void before() {
        // 创建执行环境
        env = getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        dataTable = tEnv.fromDataStream(env.fromCollection(DATA)).as("features");
    }

    /**
     * Aggregates feature by predictions. Results are returned as a list of sets, where elements in
     * the same set are features whose prediction results are the same.
     *
     * @param rows A list of rows containing feature and prediction columns
     * @param featuresCol Name of the column in the table that contains the features
     * @param predictionCol Name of the column in the table that contains the prediction result
     * @return A map containing the collected results
     */
    protected static List<Set<DenseVector>> groupFeaturesByPrediction(
            List<Row> rows, String featuresCol, String predictionCol) {
        Map<Integer, Set<DenseVector>> map = new HashMap<>();
        for (Row row : rows) {
            DenseVector vector = ((Vector) row.getField(featuresCol)).toDense();
            int predict = (Integer) row.getField(predictionCol);
            map.putIfAbsent(predict, new HashSet<>());
            map.get(predict).add(vector);
        }
        return new ArrayList<>(map.values());
    }

    /**
     * Converts data types in the table to sparse types and integer types.
     *
     * <ul>
     *   <li>If a column in the table is of DenseVector type, converts it to SparseVector.
     *   <li>If a column in the table is of Double type, converts it to integer.
     * </ul>
     */
    public static Table convertDataTypesToSparseInt(StreamTableEnvironment tEnv, Table table) {
        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(table.getResolvedSchema());
        TypeInformation<?>[] fieldTypes = inputTypeInfo.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getTypeClass().equals(DenseVector.class)) {
                fieldTypes[i] = SparseVectorTypeInfo.INSTANCE;
            } else if (fieldTypes[i].getTypeClass().equals(Double.class)) {
                fieldTypes[i] = Types.INT;
            }
        }

        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(fieldTypes),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames()));
        DataStream<Row> dataStream = tEnv.toDataStream(table);
        dataStream =
                dataStream.map(
                        new MapFunction<Row, Row>() {
                            @Override
                            public Row map(Row row) {
                                int arity = row.getArity();
                                for (int i = 0; i < arity; i++) {
                                    Object obj = row.getField(i);
                                    if (obj instanceof Vector) {
                                        row.setField(i, ((Vector) obj).toSparse());
                                    } else if (obj instanceof Number) {
                                        row.setField(i, ((Number) obj).intValue());
                                    }
                                }
                                return row;
                            }
                        },
                        outputTypeInfo);
        return tEnv.fromDataStream(dataStream);
    }

    /** Gets the types of data in each column of the input table. */
    public static Class<?>[] getColumnDataTypes(Table table) {
        return table.getResolvedSchema().getColumnDataTypes().stream()
                .map(DataType::getConversionClass)
                .toArray(Class<?>[]::new);
    }

    /**
     * Saves a stage to filesystem and reloads it by invoking the static load() method of the given
     * stage.
     */
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

    public static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    @Test
    public void testParam() {
        FCM fcm = new FCM();
        assertEquals("features", fcm.getFeaturesCol());
        assertEquals("prediction", fcm.getPredictionCol());
        assertEquals(EuclideanDistanceMeasure.NAME,fcm.getDistanceMeasure());
        assertEquals(3, fcm.getK());
        assertEquals(20, fcm.getMaxIter());
        assertEquals(FCM.class.getName().hashCode(), fcm.getSeed());

        fcm.setK(9)
                .setFeaturesCol("test_feature")
                .setPredictionCol("test_prediction")
                .setK(3)
                .setMaxIter(60)
                .setSeed(100);

        assertEquals("test_feature", fcm.getFeaturesCol());
        assertEquals("test_prediction", fcm.getPredictionCol());
        assertEquals(3, fcm.getK());
        assertEquals(60, fcm.getMaxIter());
        assertEquals(100, fcm.getSeed());
    }

    @Test
    public void testOutputSchema() throws Exception {
        Table input = dataTable.as("test_feature");

        FCM fcm =
                new FCM().setFeaturesCol("test_feature").setPredictionCol("test_prediction");
        FCMModel model = fcm.fit(input);
//        Table[] modelData = model.getModelData();
//        FCMModelData.getModelDataStream(modelData[0]).map(r->{
//            for (DenseVector centroid : r.centroids) {
//                System.out.println(centroid);
//            }
//            for (Tuple2<DenseVector, DenseVector> membershipMatrix : r.membershipMatrix) {
//                System.out.println(membershipMatrix.f0 + " " + membershipMatrix.f1);
//            }
//            return r;
//        }, TypeInformation.of(FCMModelData.class));
        Table output = model.transform(input)[0];
//        tEnv.toDataStream(output).print();
        env.execute();

        assertEquals(
                Arrays.asList("test_feature", "test_prediction"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testFewerDistinctPointsThanCluster() {
        List<DenseVector> data =
                Arrays.asList(
                        Vectors.dense(0.0, 0.1), Vectors.dense(0.0, 0.1), Vectors.dense(0.0, 0.1));

        Table input = tEnv.fromDataStream(env.fromCollection(data)).as("features");

        FCM fcm = new FCM().setK(2);
        FCMModel model = fcm.fit(input);
        Table output = model.transform(input)[0];

        List<Set<DenseVector>> expectedGroups =
                Collections.singletonList(Collections.singleton(Vectors.dense(0.0, 0.1)));

        List<Row> results = IteratorUtils.toList(output.execute().collect());
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(
                        results, fcm.getFeaturesCol(), fcm.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups, actualGroups));
    }

    @Test
    public void testFitAndPredict() {
        FCM fcm = new FCM();
        FCMModel model = fcm.fit(dataTable);
        Table output = model.transform(dataTable)[0];

        assertEquals(
                Arrays.asList("features", "prediction"),
                output.getResolvedSchema().getColumnNames());

        List<Row> results = IteratorUtils.toList(output.execute().collect());
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(
                        results, fcm.getFeaturesCol(), fcm.getPredictionCol());
        assertTrue(jugleResult(actualGroups, expectedGroups));
    }

    private Boolean jugleResult(List<Set<DenseVector>>actualGroups, List<Set<DenseVector>> expectedGroups) {
        actualGroups.sort(Comparator.comparingInt(Set::size));
        for (int i = 0; i < actualGroups.size(); i++) {
            for (int j = 0; j < expectedGroups.size(); j++) {
                if(actualGroups.get(i).size()!=expectedGroups.get(j).size()) return false;
                if(actualGroups.get(i).containsAll(expectedGroups.get(j))) return true;
            }
        }
        return true;
    }

    @Test
    public void testInputTypeConversion() {
        dataTable = convertDataTypesToSparseInt(tEnv, dataTable);
        assertArrayEquals(
                new Class<?>[] {SparseVector.class}, getColumnDataTypes(dataTable));

        FCM fcm = new FCM();
        FCMModel model = fcm.fit(dataTable);
        Table output = model.transform(dataTable)[0];

        assertEquals(
                Arrays.asList("features", "prediction"),
                output.getResolvedSchema().getColumnNames());

        List<Row> results = IteratorUtils.toList(output.execute().collect());
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(
                        results, fcm.getFeaturesCol(), fcm.getPredictionCol());
        assertTrue(jugleResult(actualGroups, expectedGroups));
    }

    @Test
    public void testSaveLoadAndPredict() throws Exception {
        String path = "target/apriori_data_items_model";

        FCM fcm = new FCM();
        FCM loadedFCM =
                saveAndReload(
                        tEnv, fcm, tempFolder.newFolder().getAbsolutePath(), FCM::load);
        FCMModel model = loadedFCM.fit(dataTable);
        FCMModel loadedModel =
                saveAndReload(
                        tEnv, model, tempFolder.newFolder().getAbsolutePath(), FCMModel::load);
        Table output = loadedModel.transform(dataTable)[0];
        assertEquals(
                Arrays.asList("centroids", "membershipMatrix"),
                loadedModel.getModelData()[0].getResolvedSchema().getColumnNames());
        assertEquals(
                Arrays.asList("features", "prediction"),
                output.getResolvedSchema().getColumnNames());

        List<Row> results = IteratorUtils.toList(output.execute().collect());
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(
                        results, fcm.getFeaturesCol(), fcm.getPredictionCol());
        assertTrue(jugleResult(actualGroups, expectedGroups));
    }

    @Test
    public void testGetModelData() throws Exception {
        FCM fcm = new FCM().setMaxIter(100);
        FCMModel model = fcm.fit(dataTable);
        assertEquals(
                Arrays.asList("centroids", "membershipMatrix"),
                model.getModelData()[0].getResolvedSchema().getColumnNames());

        DataStream<FCMModelData> modelData =
                FCMModelData.getModelDataStream(model.getModelData()[0]);
        List<FCMModelData> collectedModelData =
                IteratorUtils.toList(modelData.executeAndCollect());

        assertEquals(1, collectedModelData.size());
        DenseVector[] centroids = collectedModelData.get(0).centroids;
        assertEquals(3, centroids.length);
        Arrays.sort(centroids, Comparator.comparingDouble(vector -> vector.get(0)));
        assertArrayEquals(centroids[0].values, new double[] {1.1704, 1.4739}, 1e-4);
        assertArrayEquals(centroids[1].values, new double[] {5.8931, 7.9994}, 1e-4);
        assertArrayEquals(centroids[2].values, new double[] {8.8858, 10.6673}, 1e-4);
    }

    @Test
    public void testSetModelData() {
        FCM fcm = new FCM().setMaxIter(100);
        FCMModel modelA = fcm.fit(dataTable);
        FCMModel modelB = new FCMModel().setModelData(modelA.getModelData());
        ParamUtils.updateExistingParams(modelB, modelA.getParamMap());

        Table output = modelB.transform(dataTable)[0];
        List<Row> results = IteratorUtils.toList(output.execute().collect());
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(
                        results, fcm.getFeaturesCol(), fcm.getPredictionCol());
        assertTrue(jugleResult(actualGroups, expectedGroups));
    }

}

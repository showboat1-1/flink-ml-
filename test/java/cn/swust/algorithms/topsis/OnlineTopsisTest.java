package cn.swust.algorithms.topsis;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.window.EventTimeTumblingWindows;
import org.apache.flink.ml.common.window.GlobalWindows;
import org.apache.flink.ml.feature.standardscaler.OnlineStandardScaler;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
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
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class OnlineTopsisTest extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;

    private final List<Row> INPUT_DATA = Arrays.asList(
            Row.of(1517414400000L, Vectors.dense(262.000000, 267.899994, 250.029999, 254.259995, 254.259995, 11896100)),
            Row.of(1517500800000L, Vectors.dense(247.699997, 266.700012, 245.000000, 265.720001, 265.720001, 12595800)),
            Row.of(1517587200000L, Vectors.dense(266.579987, 272.450012, 264.329987, 264.559998, 264.559998, 8981500)),
            Row.of(1517673600000L, Vectors.dense(267.079987, 267.619995, 250.000000, 250.100006, 250.100006, 9306700)),
            Row.of(1517760000000L, Vectors.dense(253.850006, 255.800003, 236.110001, 249.470001, 249.470001, 16906900)),
            Row.of(1517846400000L, Vectors.dense(252.139999, 259.149994, 249.000000, 257.950012, 257.950012, 8534900)),
            Row.of(1517932800000L, Vectors.dense(257.290009, 261.410004, 254.699997, 258.269989, 258.269989, 6855200)),
            Row.of(1518019200000L, Vectors.dense(260.470001, 269.880005, 260.329987, 266.000000, 266.000000, 10972000)),
            Row.of(1518105600000L, Vectors.dense(270.029999, 280.500000, 267.630005, 280.269989, 280.269989, 10759700)));

    private final List<TopsisModelData> expectedModelData =
            Arrays.asList(
                    new TopsisModelData(1517875199999L,
                            1517673600000L,
                            0.5704),
                    new TopsisModelData(1518134399999L,
                            1517932800000L,
                            0.4296),
                    new TopsisModelData(1517615999999L,
                            1517414400000L,
                            0.3537));

    private final List<Row> exceptOutputData = Arrays.asList(
            Row.of(1517414400000L, Vectors.dense(262.000000, 267.899994, 250.029999, 254.259995, 254.259995, 11896100),0.3537),
            Row.of(1517500800000L, Vectors.dense(247.699997, 266.700012, 245.000000, 265.720001, 265.720001, 12595800),0.3537),
            Row.of(1517587200000L, Vectors.dense(266.579987, 272.450012, 264.329987, 264.559998, 264.559998, 8981500),0.3537),
            Row.of(1517673600000L, Vectors.dense(267.079987, 267.619995, 250.000000, 250.100006, 250.100006, 9306700),0.5704),
            Row.of(1517760000000L, Vectors.dense(253.850006, 255.800003, 236.110001, 249.470001, 249.470001, 16906900),0.5704),
            Row.of(1517846400000L, Vectors.dense(252.139999, 259.149994, 249.000000, 257.950012, 257.950012, 8534900),0.5704),
            Row.of(1517932800000L, Vectors.dense(257.290009, 261.410004, 254.699997, 258.269989, 258.269989, 6855200),0.4296),
            Row.of(1518019200000L, Vectors.dense(260.470001, 269.880005, 260.329987, 266.000000, 266.000000, 10972000),0.4296),
            Row.of(1518105600000L, Vectors.dense(270.029999, 280.500000, 267.630005, 280.269989, 280.269989, 10759700),0.4296));

    private Table inputTableWithEventTime;

    public Table createTableWithEvent(DataStream<Row> inputStream, StreamTableEnvironment tEnv) {
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

    /**
     * Gets a {@link StreamExecutionEnvironment} with the most common configurations of the Flink ML
     * program.
     */
    public static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    @Before
    public void before() {
        env = getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        inputTableWithEventTime = createTableWithEvent(env.fromCollection(INPUT_DATA),tEnv);
    }

    @Test
    public void testParam() {
        OnlineStandardScaler standardScaler = new OnlineStandardScaler();
        OnlineTopsis onlineTopsis = new OnlineTopsis();

        assertEquals("features", onlineTopsis.getFeaturesCol());
        assertEquals("prediction", onlineTopsis.getPredictionCol());
        assertEquals(GlobalWindows.getInstance(), onlineTopsis.getWindows());
        assertEquals(0L, onlineTopsis.getMaxAllowedModelDelayMs());

        onlineTopsis
                .setFeaturesCol("test_features")
                .setPredictionCol("test_prediction")
                .setWindows(EventTimeTumblingWindows.of(Time.milliseconds(3000)))
                .setMaxAllowedModelDelayMs(3000L)
                .setCriteriaTypes(1, 2, 3, 4)
                .setWeights(0.4,0.3,0.2,0.1)
                .setBestValue(165.0)
                .setIntervalValue(90.0,100.0);


        assertEquals("test_features", onlineTopsis.getFeaturesCol());
        assertEquals("test_prediction", onlineTopsis.getPredictionCol());
        assertEquals(
                EventTimeTumblingWindows.of(Time.milliseconds(3000)), onlineTopsis.getWindows());
        assertEquals(3000L, onlineTopsis.getMaxAllowedModelDelayMs());
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, onlineTopsis.getCriteriaTypes());
        assertArrayEquals(new Double[]{0.4,0.3,0.2,0.1}, onlineTopsis.getWeights());
        assertEquals(165.0, onlineTopsis.getBestValue(), 0.00001);
        assertArrayEquals(new Double[]{90.0,100.0}, onlineTopsis.getIntervalValue());
    }

    @Test
    public void testOutputSchema() {
        Table renamedTable = inputTableWithEventTime.as("test_id", "test_features");

        OnlineTopsis onlineTopsis = new OnlineTopsis()
                .setFeaturesCol("test_features")
                .setPredictionCol("test_prediction");

        onlineTopsis.setCriteriaTypes(1,1,1,2,2,2);
        onlineTopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);

        Table output = onlineTopsis.fit(renamedTable).transform(renamedTable)[0];


        assertEquals(
                Arrays.asList("test_id", "test_features", "rowtime","test_prediction"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testFitAndPredictWithEventTimeWindow() throws Exception {
        OnlineTopsis onlineTopsis = new OnlineTopsis();
        onlineTopsis.setCriteriaTypes(1,1,1,2,2,2);
        onlineTopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);

        Table output;
        Long windowSizeDays = 3L;

        // Tests event time window with maxAllowedModelDelayMs as 0.
        onlineTopsis.setWindows(EventTimeTumblingWindows.of(Time.days(windowSizeDays)));
        OnlineTopsisModel model = onlineTopsis.fit(inputTableWithEventTime);
        Table modelData = model.getModelData()[0];
        output = model.transform(inputTableWithEventTime)[0];

        verifyUsedModelAndOutput(output,modelData,exceptOutputData,expectedModelData);
    }

    private void verifyUsedModelAndOutput(Table output, Table modelData,
                                        List<Row> exceptOutputData, List<TopsisModelData> expectedModelData) throws Exception {
        List<Row> outputResult = IteratorUtils.toList(tEnv.toDataStream(output).executeAndCollect());
        List<Row> modelDataResult = IteratorUtils.toList(tEnv.toDataStream(modelData).executeAndCollect());
        outputResult.sort((o1, o2) -> (Long)o1.getField(0) > ((Long)o2.getField(0)) ? 1 : -1);
        modelDataResult.sort(new Comparator<Row>() {
            @Override
            public int compare(Row o1, Row o2) {
                Double o1Field = (Double) o1.getField(2);
                Double o2Field = (Double) o2.getField(2);
                return o1Field < o2Field ? 1 : -1;
            }
        });
        assertEquals(expectedModelData.size(),modelDataResult.size());
        assertEquals(exceptOutputData.size(),outputResult.size());
        for (int i = 0; i < modelDataResult.size(); i++) {
            assertEquals(expectedModelData.get(i).score, (Double) modelDataResult.get(i).getField(2),0.0001);
            assertEquals((Long) expectedModelData.get(i).minTimestamp,(Long)modelDataResult.get(i).getField(3));
            assertEquals((Long) expectedModelData.get(i).timestamp,(Long)modelDataResult.get(i).getField(1));
        }
        for (int i = 0; i < outputResult.size(); i++) {
            assertEquals(exceptOutputData.get(i).getField(0),outputResult.get(i).getField(0));
            assertEquals(exceptOutputData.get(i).getField(1),outputResult.get(i).getField(1));
            assertEquals((Double) exceptOutputData.get(i).getField(2),((DenseVector)outputResult.get(i).getField(3)).get(0),0.0001);
        }
    }

    private void verifyOutput(Table output,List<Row> exceptOutputData ) throws Exception {
        List<Row> outputResult = IteratorUtils.toList(tEnv.toDataStream(output).executeAndCollect());
        outputResult.sort((o1, o2) -> (Long)o1.getField(0) > ((Long)o2.getField(0)) ? 1 : -1);
        assertEquals(exceptOutputData.size(),outputResult.size());
        for (int i = 0; i < outputResult.size(); i++) {
            assertEquals(exceptOutputData.get(i).getField(0),outputResult.get(i).getField(0));
            assertEquals(exceptOutputData.get(i).getField(1),outputResult.get(i).getField(1));
            assertEquals((Double) exceptOutputData.get(i).getField(2),((DenseVector)outputResult.get(i).getField(3)).get(0),0.0001);
        }
    }

    private void verifyModelData(TopsisModelData exceptModelData,TopsisModelData outPutModelData){
        assertEquals(exceptModelData.score,outPutModelData.score,0.0001);
        assertEquals(exceptModelData.minTimestamp,outPutModelData.minTimestamp);
        assertEquals(exceptModelData.timestamp,outPutModelData.timestamp);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetModelData() throws Exception {
        OnlineTopsis onlineTopsis = new OnlineTopsis();

        onlineTopsis.setCriteriaTypes(1,1,1,2,2,2);
        onlineTopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);

        OnlineTopsisModel model = onlineTopsis
                .setWindows(EventTimeTumblingWindows.of(Time.days(3L)))
                .fit(inputTableWithEventTime);

        Table modelDataTable = model.getModelData()[0];

        assertEquals(
                Arrays.asList("data", "timestamp", "score", "minTimestamp"),
                modelDataTable.getResolvedSchema().getColumnNames());

        List<TopsisModelData> collectedModelData =
                (List<TopsisModelData>)
                        IteratorUtils.toList(
                                TopsisModelData.getModelDataStream(modelDataTable)
                                        .executeAndCollect());
        collectedModelData.sort(new Comparator<TopsisModelData>() {
            @Override
            public int compare(TopsisModelData o1, TopsisModelData o2) {
                return o1.score < o2.score ? 1 : -1;
            }
        });

        assertEquals(expectedModelData.size(), collectedModelData.size());
        for (int i = 0; i < expectedModelData.size(); i++) {
            verifyModelData(expectedModelData.get(i), collectedModelData.get(i));
        }
    }

    @Test
    public void testSetModelData() throws Exception {
        OnlineTopsis onlineTopsis = new OnlineTopsis().setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        onlineTopsis.setCriteriaTypes(1,1,1,2,2,2);
        onlineTopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);

        OnlineTopsisModel model = onlineTopsis.fit(inputTableWithEventTime);

        OnlineTopsisModel newModel = new OnlineTopsisModel();
        ParamUtils.updateExistingParams(newModel, model.getParamMap());
        newModel.setModelData(model.getModelData());
        Table output = newModel.transform(inputTableWithEventTime)[0];
        verifyOutput(output,exceptOutputData);
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

    @Test
    public void testSaveLoadPredict() throws Exception {
        OnlineTopsis onlineTopsis = new OnlineTopsis().setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        onlineTopsis.setCriteriaTypes(1,1,1,2,2,2);
        onlineTopsis.setWeights(0.1,0.1,0.1,0.1,0.1,0.1);

        onlineTopsis =
                saveAndReload(
                        tEnv,
                        onlineTopsis,
                        TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                        OnlineTopsis::load);
        OnlineTopsisModel model = onlineTopsis.fit(inputTableWithEventTime);
        Table[] modelData = model.getModelData();

        model =
                saveAndReload(
                        tEnv,
                        model,
                        TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                        OnlineTopsisModel::load);
        model.setModelData(modelData);

        Table output = model.transform(inputTableWithEventTime)[0];
        verifyOutput(output,exceptOutputData);
    }
}

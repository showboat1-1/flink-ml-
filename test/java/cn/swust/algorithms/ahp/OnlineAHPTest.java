package cn.swust.algorithms.ahp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.window.EventTimeTumblingWindows;
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
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class OnlineAHPTest extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTableWithEvent;

    Double [][] judgmentMatrix = {
            {1.0,0.5,4.0,3.0,3.0,2.0},
            {2.0,1.0,7.0,5.0,5.0,4.0},
            {0.25,0.143,1.0,0.5,0.67,0.4},
            {0.67,0.2,2.0,1.0,1.0,0.67},
            {0.33,0.2,3.0,1.0,1.0,2.0},
            {0.5,0.25,2.5,3.0,0.5,1.0}
    };
    Integer[] type = {1,1,1,0,0,0};

    private final List<Row> INPUT_DATA =
            Arrays.asList(
                    Row.of(1601913600000L,55.11235847,49.78789992,52.67503496,53.21924296,0.0,89128128.86084658),
                    Row.of(1602000000000L,53.40227002,40.73457791,53.29196931,42.40159861,583091.4598,71011441.25451232),
                    Row.of(1602086400000L,42.40831364,35.97068975,42.39994711,40.08397561,682834.18632335,67130036.89981823),
                    Row.of(1602172800000L,44.90251114,36.69605677,39.88526234,43.76446306,1658816.92260445,220265142.10956782),
                    Row.of(1602259200000L,47.56953274,43.2917758,43.76446306,46.81774415,815537.6608,235632208.16269898),
                    Row.of(1602345600000L,51.4056548,46.70332768,46.81814554,49.13371767,1074627.02770495,247288428.75616974),
                    Row.of(1602432000000L,51.45337431,48.71603954,49.13312951,49.66072573,692150.60094118,249940843.43242228),
                    Row.of(1602518400000L,54.42141763,48.75407708,49.66157329,52.23869222,1354836.06481028,262915666.29307267),
                    Row.of(1602518400000L,57.48190422,49.59873521,52.23839242,51.12431679,1386221.2066711,257307050.43080166));

    private final List<Row> exceptOutputData =
            Arrays.asList(
                    Row.of(1601913600000L,55.11235847,49.78789992,52.67503496,53.21924296,0.0,89128128.86084658,0.1342),
                    Row.of(1602000000000L,53.40227002,40.73457791,53.29196931,42.40159861,583091.4598,71011441.25451232,0.0931),
                    Row.of(1602086400000L,42.40831364,35.97068975,42.39994711,40.08397561,682834.18632335,67130036.89981823,0.0337),
                    Row.of(1602172800000L,44.90251114,36.69605677,39.88526234,43.76446306,1658816.92260445,220265142.10956782,0.0366),
                    Row.of(1602259200000L,47.56953274,43.2917758,43.76446306,46.81774415,815537.6608,235632208.16269898,0.0970),
                    Row.of(1602345600000L,51.4056548,46.70332768,46.81814554,49.13371767,1074627.02770495,247288428.75616974,0.1235),
                    Row.of(1602432000000L,51.45337431,48.71603954,49.13312951,49.66072573,692150.60094118,249940843.43242228,0.0543),
                    Row.of(1602518400000L,54.42141763,48.75407708,49.66157329,52.23869222,1354836.06481028,262915666.29307267,0.0215),
                    Row.of(1602518400000L,57.48190422,49.59873521,52.23839242,51.12431679,1386221.2066711,257307050.43080166,0.1326));

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
                                        .column("f1", DataTypes.DOUBLE())
                                        .column("f2", DataTypes.DOUBLE())
                                        .column("f3", DataTypes.DOUBLE())
                                        .column("f4", DataTypes.DOUBLE())
                                        .column("f5", DataTypes.DOUBLE())
                                        .column("f6", DataTypes.DOUBLE())
                                        //由数据给定的时间生成带时区的水印时间
                                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                        //使用数据源提供的水印，水印用于处理事件时间语义
                                        .watermark("rowtime", "SOURCE_WATERMARK()")
                                        .build())
                        .as("id", "High","Low","Open","Close","Volume","Marketcap");
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
        env.setParallelism(4);//Test中限制了并发度不能大于4
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }
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
    @Before
    public void before() {
        env = getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        //构建数据源
        DataStream<Row> dataStream = env.fromCollection(INPUT_DATA);
        inputDataTableWithEvent = createTableWithEvent(dataStream, tEnv);
    }

    @Test
    public void testParam() {
        OnlineAHP onlineAHP = new OnlineAHP();
        onlineAHP.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        onlineAHP.setJudgmentMatrix(judgmentMatrix);
        onlineAHP.setIndicatorType(type);
        onlineAHP.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        assertArrayEquals(new Integer[]{1,1,1,0,0,0}, onlineAHP.getIndicatorType());
        assertArrayEquals(new Double[][]{
                {1.0,0.5,4.0,3.0,3.0,2.0},
                {2.0,1.0,7.0,5.0,5.0,4.0},
                {0.25,0.143,1.0,0.5,0.67,0.4},
                {0.67,0.2,2.0,1.0,1.0,0.67},
                {0.33,0.2,3.0,1.0,1.0,2.0},
                {0.5,0.25,2.5,3.0,0.5,1.0}
        }, onlineAHP.getJudgmentMatrix());
        assertEquals(new String[]{"High","Low","Open","Close","Volume","Marketcap"},onlineAHP.getInputCols());
        assertEquals("output",onlineAHP.getOutputCol());
        assertEquals(
                EventTimeTumblingWindows.of(Time.days(3L)), onlineAHP.getWindows());
    }

    @Test
    public void testOutputSchema() {
        Table input = inputDataTableWithEvent.as("id","t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap");

        OnlineAHP onlineAHP = new OnlineAHP();
        onlineAHP.setInputCols("t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap");
        onlineAHP.setJudgmentMatrix(judgmentMatrix);
        onlineAHP.setIndicatorType(type);
        onlineAHP.setOutputCol("test_output");
        onlineAHP.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        Table output = onlineAHP.transform(input)[0];

        assertEquals(
                Arrays.asList("id","t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap","rowtime","test_output"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testSaveLoadTransform() throws Exception {
        OnlineAHP onlineAHP = new OnlineAHP();
        onlineAHP.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        onlineAHP.setJudgmentMatrix(judgmentMatrix);
        onlineAHP.setIndicatorType(type);
        onlineAHP.setWindows(EventTimeTumblingWindows.of(Time.days(3L)));

        onlineAHP =
                saveAndReload(
                        tEnv,
                        onlineAHP,
                        tempFolder.newFolder().getAbsolutePath(),
                        OnlineAHP::load);

        Table[] outputs = onlineAHP.transform(inputDataTableWithEvent);

        DataStream<Row> rowDataStream = tEnv.toDataStream(outputs[0]);
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        verifyOutputResult(results,exceptOutputData);
    }

    private void verifyOutputResult(List<Row> results, List<Row> exceptOutputData) {
        //排序
        results.sort((o1,o2)->(Long)o1.getField(0) < (Long) o2.getField(0) ? -1 : 1);
        exceptOutputData.sort((o1,o2)->(Long)o1.getField(0) < (Long) o2.getField(0) ? -1 : 1);
        //比较两者大小
        assertEquals(results.size(), exceptOutputData.size());
        //比较每一个元素
        for (int i = 0; i < results.size(); i++) {
            int n = results.get(i).getArity();
            for (int j = 0; j < n -2; j++) {
                assertEquals(results.get(i).getField(j), exceptOutputData.get(i).getField(j));
            }
            assertEquals((Double)results.get(i).getField(n-1), (Double)exceptOutputData.get(i).getField(n-2), 0.0001);
        }
    }

}

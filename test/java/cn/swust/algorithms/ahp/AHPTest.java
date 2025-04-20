package cn.swust.algorithms.ahp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AHPTest extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTable;

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
                    Row.of(55.11235847,49.78789992,52.67503496,53.21924296,553091.4598,89128128.86084658),
                    Row.of(53.40227002,40.73457791,53.29196931,42.40159861,583091.4598,71011441.25451232),
                    Row.of(42.40831364,35.97068975,42.39994711,40.08397561,682834.18632335,67130036.89981823),
                    Row.of(44.90251114,36.69605677,39.88526234,43.76446306,1658816.92260445,220265142.10956782));

    private final List<Row> exceptOutputData =
            Arrays.asList(
                    Row.of(55.11235847,49.78789992,52.67503496,53.21924296,553091.4598,89128128.86084658,0.1841),
                    Row.of(53.40227002,40.73457791,53.29196931,42.40159861,583091.4598,71011441.25451232,0.1189),
                    Row.of(42.40831364,35.97068975,42.39994711,40.08397561,682834.18632335,67130036.89981823,0.0396),
                    Row.of(44.90251114,36.69605677,39.88526234,43.76446306,1658816.92260445,220265142.10956782,0.0218));

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
        inputDataTable = tEnv.fromDataStream(dataStream).as("High","Low","Open","Close","Volume","Marketcap");
    }

    @Test
    public void testParam() {
        AHP ahp = new AHP();
        ahp.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        ahp.setJudgmentMatrix(judgmentMatrix);
        ahp.setIndicatorType(type);
        ahp.setOutputCol("score");

        assertArrayEquals(new Integer[]{1,1,1,0,0,0}, ahp.getIndicatorType());
        assertArrayEquals(new Double[][]{
                {1.0,0.5,4.0,3.0,3.0,2.0},
                {2.0,1.0,7.0,5.0,5.0,4.0},
                {0.25,0.143,1.0,0.5,0.67,0.4},
                {0.67,0.2,2.0,1.0,1.0,0.67},
                {0.33,0.2,3.0,1.0,1.0,2.0},
                {0.5,0.25,2.5,3.0,0.5,1.0}
        }, ahp.getJudgmentMatrix());
        assertEquals(new String[]{"High","Low","Open","Close","Volume","Marketcap"},ahp.getInputCols());
        assertEquals("score",ahp.getOutputCol());
    }

    @Test
    public void testOutputSchema() {
        Table input = inputDataTable.as("t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap");
        AHP ahp = new AHP();
        ahp.setInputCols("t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap");
        ahp.setJudgmentMatrix(judgmentMatrix);
        ahp.setIndicatorType(type);
        ahp.setOutputCol("test_output");

        Table output = ahp.transform(input)[0];

        assertEquals(
                Arrays.asList("t_High","t_Low","t_Open","t_Close","t_Volume","t_Marketcap","test_output"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testSaveLoadTransform() throws Exception {
        AHP ahp = new AHP();
        ahp.setInputCols("High","Low","Open","Close","Volume","Marketcap");
        ahp.setJudgmentMatrix(judgmentMatrix);
        ahp.setIndicatorType(type);
        ahp.setOutputCol("score");

        ahp =
                saveAndReload(
                        tEnv,
                        ahp,
                        tempFolder.newFolder().getAbsolutePath(),
                        AHP::load);

        Table[] outputs = ahp.transform(inputDataTable);

        DataStream<Row> rowDataStream = tEnv.toDataStream(outputs[0]);
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        verifyOutputResult(results,exceptOutputData);
    }

    private void verifyOutputResult(List<Row> results, List<Row> exceptOutputData) {
        //排序
        results.sort((o1,o2)->(Double)o1.getField(0) < (Double)o2.getField(0) ? -1 : 1);
        exceptOutputData.sort((o1,o2)->(Double)o1.getField(0) < (Double)o2.getField(0) ? -1 : 1);
        //比较两者大小
        assertEquals(results.size(), exceptOutputData.size());
        //比较每一个元素
        for (int i = 0; i < results.size(); i++) {
            int n = results.get(i).getArity();
            for (int j = 0; j < n -1; j++) {
                assertEquals(results.get(i).getField(j), exceptOutputData.get(i).getField(j));
            }
            assertEquals((Double)results.get(i).getField(n-1), (Double)exceptOutputData.get(i).getField(n-1), 0.0001);
        }
    }
}
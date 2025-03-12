package cn.swust.algorithms.apriori;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class AprioriTest extends AbstractTestBase {
    private StreamTableEnvironment tEnv;
    private Table inputDataTable;

    /**
     * 测试数据
     * [A C D]
     * [B C E]
     * [A B C E]
     * [B E]
     */
    private static final List<Row> INPUT_DATA =
            Arrays.asList(
                    Row.of("A/C/D"),
                    Row.of("B/C/E"),
                    Row.of("A/B/C/E"),
                    Row.of("B/E"));

    private List<Row> exceptOutputData =
            Arrays.asList(
                    Row.of(
                            Arrays.asList("E","B","C"),
                            2,//count
                            0.5,//minsupport
                            1.0,//minconfidence
                            1.333,//lift
                            Arrays.asList("E"),//prefix
                            Arrays.asList("B","C")),//suffix
                    Row.of(
                            Arrays.asList("B","C","E"),
                            2,//count
                            0.5,//minsupport
                            1.0,//minconfidence
                            1.333,//lift
                            Arrays.asList("B"),//prefix
                            Arrays.asList("C","E")),//suffix
                    Row.of(
                            Arrays.asList("C","E","B"),
                            2,//count
                            0.5,//minsupport
                            0.667,//minconfidence
                            1.333,//lift
                            Arrays.asList("C","E"),//prefix
                            Arrays.asList("B")),//suffix
                    Row.of(
                            Arrays.asList("B","C","E"),
                            2,//count
                            0.5,//minsupport
                            0.667,//minconfidence
                            1.333,//lift
                            Arrays.asList("B","C"),//prefix
                            Arrays.asList("E")),//suffix
                    Row.of(
                            Arrays.asList("C","A"),
                            2,//count
                            0.5,//minsupport
                            1.0,//minconfidence
                            1.333,//lift
                            Arrays.asList("C"),//prefix
                            Arrays.asList("A")),//suffix
                    Row.of(
                            Arrays.asList("A","C"),
                            2,//count
                            0.5,//minsupport
                            0.667,//minconfidence
                            1.333,//lift
                            Arrays.asList("A"),//prefix
                            Arrays.asList("C")),//suffix
                    Row.of(
                            Arrays.asList("E","B"),
                            3,//count
                            0.75,//minsupport
                            1.0,//minconfidence
                            1.333,//lift
                            Arrays.asList("E"),//prefix
                            Arrays.asList("B")),//suffix
                    Row.of(
                            Arrays.asList("B","E"),
                            3,//count
                            0.75,//minsupport
                            1.0,//minconfidence
                            1.333,//lift
                            Arrays.asList("B"),
                            Arrays.asList("E")));

    private void verifyOutputResult(Table output, String[] outputCols) throws Exception {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) output).getTableEnvironment();
        DataStream<Row> stream = tEnv.toDataStream(output);
        List<Row> results = IteratorUtils.toList(stream.executeAndCollect());
        //判断大小是否一致
        assertEquals(exceptOutputData.size(),results.size());

        //排序保证内容顺序一致
        exceptOutputData.sort(AprioriTest::compare);
        results.sort(AprioriTest::compare);

        //判断内容是否一致
        for(int i = 0; i < exceptOutputData.size(); ++i) {
            Row except = exceptOutputData.get(i);
            Row actual = results.get(i);
            assertEquals(except.getField(0), actual.getField(0));
            assertEquals(except.getField(1),actual.getField(1));
            double supportTrustThresholds = Math.abs((Double) except.getField(2) - (Double) actual.getField(2));
            double confidenceTrustThresholds = Math.abs((Double) except.getField(3) - (Double) actual.getField(3));
            double liftTrustThresholds = Math.abs((Double) except.getField(4) - (Double) actual.getField(4));
            assertTrue(supportTrustThresholds < 0.001);
            assertTrue(confidenceTrustThresholds < 0.001);
            assertTrue(liftTrustThresholds < 0.001);
            assertEquals(except.getField(5), actual.getField(5));
            assertEquals(except.getField(6), actual.getField(6));
        }
    }

    private static int compare(Row first, Row second) {
        //根据第一个集合进行排序
        return first.getField(0).toString().compareTo(second.getField(0).toString());
    }
    @Before
    public void before() {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
        //构建数据源
        DataStream<Row> dataStream = env.fromCollection(INPUT_DATA);
        inputDataTable = tEnv.fromDataStream(dataStream).as("items");
    }

    @Test
    public void testParam() {
        Apriori apriori =
                new Apriori()
                        .setMinSupport(0.3)
                        .setMinConfidence(0.4)
                        .setLift(1.2)
                        .setItemSeparator("/")
                        .setInputCols("items");

        assertEquals(0.3, apriori.getMinSupport(), 0.0001);
        assertEquals(0.4, apriori.getMinConfidence(), 0.0001);
        assertEquals(1.2, apriori.getLift(), 0.0001);
        assertEquals("/", apriori.getItemSeparator());
        assertEquals("items", apriori.getInputCols()[0]);
    }

    @Test
    public void testOutputSchema() {
        Apriori apriori =
                new Apriori()
                        .setMinSupport(0.3)
                        .setMinConfidence(0.4)
                        .setLift(1.2)
                        .setItemSeparator("/")
                        .setInputCols("items");

        Table output = apriori.transform(inputDataTable)[0];

        assertEquals(
                Arrays.asList("itemSet","count","support", "confidence", "lift", "prefix", "suffix"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testSaveLoadAndTransform() throws Exception {
        Apriori apriori =
                new Apriori()
                        .setMinSupport(0.3)
                        .setMinConfidence(0.4)
                        .setLift(1.2)
                        .setItemSeparator("/")
                        .setInputCols("items");

        String path = "target/apriori_data_items_model";
        if (!new File(path).exists()) {
            apriori.save(path);
        }
        Apriori loadedApriori = Apriori.load(tEnv, path);
        // 验证加载的参数
        assertEquals(apriori.getInputCols(), loadedApriori.getInputCols());

        Table resultTable = loadedApriori.transform(inputDataTable)[0];
        RowTypeInfo rowTypeInfo = TableUtils.getRowTypeInfo(resultTable.getResolvedSchema());

        verifyOutputResult(resultTable,rowTypeInfo.getFieldNames());
    }
}
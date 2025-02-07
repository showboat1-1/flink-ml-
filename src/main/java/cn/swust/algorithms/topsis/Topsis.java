package cn.swust.algorithms.topsis;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 实现 TOPSIS（优先级评估法）的算法。
 */
public class Topsis implements AlgoOperator<Topsis>, TopsisParams<Topsis> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public Topsis() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        // 获取输入的表
        Table inputTable = inputs[0];
        // 创建 StreamTableEnvironment 对象
        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputTable).getTableEnvironment();

        // 获取输入列（特征列）和输出列名称
        String[] inputCols = getInputCols();  // 输入特征列（假设是数组）
        String outputCol = getOutputCol();    // 输出列

        // 获取权重
        double[] weights = getWeights();

        // 注册输入表为临时视图
        tEnv.createTemporaryView("input_table", inputTable);
//        inputTable.execute().print();
//        System.out.println(Arrays.toString(inputCols));
//        System.out.println(outputCol);
//        System.out.println(Arrays.toString(weights));

        // Step 1: 创建规范化矩阵的SQL
        String normalizationSql = createNormalizationSql(inputCols);
        Table normalizedTable = tEnv.sqlQuery(normalizationSql);
        tEnv.createTemporaryView("normalized_data", normalizedTable);
//        normalizedTable.execute().print();

        //step 2:加权标准化矩阵
        String weightedNormalizationSql = createWeightedNormalizationSql(inputCols, weights);
        Table weightedNormalizedTable = tEnv.sqlQuery(weightedNormalizationSql);
        tEnv.createTemporaryView("weighted_normalized_data", weightedNormalizedTable);
        //weightedNormalizationSql.execute().print();

        // Step 3: 计算理想解和负理想解
        String idealSolutionsSql = createIdealSolutionsSql(inputCols);
        Table idealSolutions = tEnv.sqlQuery(idealSolutionsSql);
        tEnv.createTemporaryView("ideal_solutions", idealSolutions);
//        idealSolutions.execute().print();


        // Step 4: 计算最终的TOPSIS得分
        String finalScoreSql = createFinalScoreSql(inputCols, outputCol);
        Table resultTable = tEnv.sqlQuery(finalScoreSql);

        RowTypeInfo rowTypeInfo = TableUtils.getRowTypeInfo(resultTable.getResolvedSchema());
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        //每一条数据的eventTime 存放在 DateColumns 中（rowtime）
        String[] fieldNames = rowTypeInfo.getFieldNames();

        //仅保留最后一条数据
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(resultTable);
        //按时间序列进行分区
        KeyedStream<Row, Object> keyData = rowDataStream.keyBy(r -> r.getField("DateColumns"));
        //归约
        DataStream<Row> reduceData = DataStreamUtils.reduce(keyData, new ReduceFunction<Row>() {
            @Override
            public Row reduce(Row value1, Row value2) throws Exception {
                return value2;
            }
        });
        Table finalTable = tEnv.fromChangelogStream(reduceData);
//        resultTable.execute().print();

        return new Table[]{finalTable};
    }

    private String createNormalizationSql(String[] inputCols) {
        StringBuilder sql = new StringBuilder();

        // Step 1: 计算每个特征列的平方和的平方根 (归一化因子)
        sql.append("WITH norm_factors AS (");
        sql.append("SELECT ");
        for (int i = 0; i < inputCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("SQRT(SUM(POWER(%s, 2))) AS norm_%s", inputCols[i], inputCols[i]));
        }
        sql.append(" FROM input_table) ");

        // Step 2: 只选择归一化因子进行输出，并保留 norm_Accuracy 最大的一行
        sql.append(", norm_max AS (");
        sql.append("SELECT ");
        for (int i = 0; i < inputCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("norm_%s", inputCols[i]));
        }
        sql.append(" FROM norm_factors) ");

        // Step 3: 确保只返回归一化因子计算完最后一行
        sql.append(", norm_max_filtered AS (");
        sql.append("SELECT * FROM norm_max ");
        sql.append("WHERE norm_").append(inputCols[0]).append(" = (SELECT MAX(norm_").append(inputCols[0]).append(") FROM norm_max)) ");


        // Step 4: 计算每行数据的归一化值，避免笛卡尔积（直接引用norm_factors中的值）
        sql.append("SELECT i.DateColumns, ");
        for (int i = 0; i < inputCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("i.%s / n.norm_%s AS norm_%s", inputCols[i], inputCols[i], inputCols[i]));
        }
        sql.append(" FROM input_table i, norm_max_filtered n");

        return sql.toString();
    }

    private String createWeightedNormalizationSql(String[] inputCols, double[] weights) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT i.DateColumns, ");
        for (int i = 0; i < inputCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("i.norm_%s * %f AS weighted_norm_%s", inputCols[i], weights[i], inputCols[i]));
        }
        sql.append(" FROM normalized_data i");
        return sql.toString();
    }

    private String createIdealSolutionsSql(String[] inputCols) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        // 对每一列（特征列）计算最大值和最小值
        for (int i = 0; i < inputCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }

            String normalizedCol = "norm_" + inputCols[i];

            // 计算每列的最大值和最小值
            sql.append(String.format("MAX(%s) AS max_%s", normalizedCol, inputCols[i]));
            sql.append(String.format(", MIN(%s) AS min_%s", normalizedCol, inputCols[i]));
        }

        // 从归一化数据表中选择最大值和最小值
        sql.append(" FROM normalized_data");
        sql.append(" GROUP BY 1=1");
        return sql.toString();
    }


    private String createFinalScoreSql(String[] inputCols, String outputCol) {
        StringBuilder sql = new StringBuilder();
        sql.append("WITH distances AS (");
        sql.append("SELECT n.*, ");

        // 计算到理想解的距离
        StringBuilder distancePos = new StringBuilder();
        StringBuilder distanceNeg = new StringBuilder();

        for (String col : inputCols) {
            if (distancePos.length() > 0) {
                distancePos.append(" + ");
                distanceNeg.append(" + ");
            }

            distancePos.append(String.format(
                    "POWER(n.norm_%s - i.max_%s, 2)", col, col));
            distanceNeg.append(String.format(
                    "POWER(n.norm_%s - i.min_%s, 2)", col, col));
        }
        //d_pos:是当前方案与 理想解 的欧几里得距离。
        //d_neg:是当前方案与 负理想解 的欧几里得距离。
        sql.append(String.format("SQRT(%s) as d_pos, ", distancePos.toString()));
        sql.append(String.format("SQRT(%s) as d_neg ", distanceNeg.toString()));
        sql.append("FROM normalized_data n, ideal_solutions i"); // 使用普通的 JOIN

        sql.append(") ");

        // 计算最终得分
        sql.append("SELECT *, ");
        sql.append(String.format("d_neg / (d_pos + d_neg) as %s ", outputCol));
        sql.append("FROM distances");
        //sql.append(String.format("ORDER BY %s DESC", outputCol)); // 按照得分从高到低排序，但是flink sql不允许用order by排序
        return sql.toString();
    }


    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static Topsis load(StreamTableEnvironment env, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

}

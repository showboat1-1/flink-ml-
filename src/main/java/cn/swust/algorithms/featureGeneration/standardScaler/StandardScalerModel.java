package cn.swust.algorithms.featureGeneration.standardScaler;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

public class StandardScalerModel implements Model<StandardScalerModel>, StandardScalerParams<StandardScalerModel> {
    private final Map<Param<?>, Object> paramMap;

    public StandardScalerModel(Map<Param<?>, Object> paramMap){
        this.paramMap = paramMap;
    }
    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "Input tables must be exactly 1.");

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputs[0]);

        DataStream<Row> resultStream = computeScaledFeatures(dataStream);
        String outputCol = getOutputCol();
        Schema schema = Schema.newBuilder().column(outputCol, org.apache.flink.table.api.DataTypes.DOUBLE()).build();
        Table resultTable = tEnv.fromDataStream(resultStream, schema);
        resultTable = resultTable.as(outputCol);


        return new Table[]{resultTable};
    }

    private DataStream<Row> computeScaledFeatures(DataStream<Row> inputStream){

        String inputCol = getInputCol();

        TypeInformation<?>[] types = new TypeInformation[]{Types.DOUBLE};
        String[] fieldNames = new String[]{getOutputCol()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types,fieldNames);

        return inputStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                Number value = (Number) row.getField(inputCol);

                if (value == null) {
                    return Row.of((Double) null);
                }

                double doubleValue = value.doubleValue();
                //这里实际应该加上一些逻辑，比如计算均值和方差，然后再进行标准化。这里为了示例，直接返回原始值
                //double scaledValue = (doubleValue - mean) / stdDev; // 实际的标准化公式
                return Row.of(doubleValue);
            }
        }).returns(rowTypeInfo);

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static StandardScalerModel load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
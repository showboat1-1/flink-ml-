package cn.swust.algorithms.featureGeneration.mean;

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

public class MeanImputerModel implements Model<MeanImputerModel>, MeanImputerParams<MeanImputerModel> {
    private final Map<Param<?>, Object> paramMap;
    private final double meanValue;

    public MeanImputerModel(Map<Param<?>, Object> paramMap, double meanValue) {
        this.paramMap = paramMap;
        this.meanValue = meanValue;
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "Input tables must be exactly 1.");

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputs[0]);

        DataStream<Row> resultStream = imputeMissingValues(dataStream);
        String outputCol = getOutputCol();
        Schema schema = Schema.newBuilder().column(outputCol, org.apache.flink.table.api.DataTypes.DOUBLE()).build();
        Table resultTable = tEnv.fromDataStream(resultStream, schema);
        resultTable = resultTable.as(outputCol);

        return new Table[]{resultTable};
    }

    private DataStream<Row> imputeMissingValues(DataStream<Row> inputStream) {
        String inputCol = getInputCol();

        TypeInformation<?>[] types = new TypeInformation[]{Types.DOUBLE};
        String[] fieldNames = new String[]{getOutputCol()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        return inputStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                Number value = (Number) row.getField(inputCol);

                if (value == null) {
                    return Row.of(meanValue);
                }

                return Row.of(value.doubleValue());
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

    public static MeanImputerModel load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
package cn.swust.algorithms.featureGeneration.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

public class TimeFeatureExtractor2 implements AlgoOperator<TimeFeatureExtractor2>, TimeFeatureExtractorParams<TimeFeatureExtractor2> {


    private static final String INPUT_FORMAT_DATE = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(INPUT_FORMAT_DATE);
    private final Map<Param<?>, Object> paramMap = new HashMap<>();


    public TimeFeatureExtractor2() {
        // 初始化参数默认值
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "输入表只能有一个");

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputs[0]);

        DataStream<Row> resultStream = computeTimeFeatures(dataStream);
        String[] outputCols = getOutputCols();
        Schema schema = Schema.newBuilder()
                .column(outputCols[0], DataTypes.INT())
                .column(outputCols[1], DataTypes.STRING())
                .column(outputCols[2], DataTypes.STRING())
                .build();
        Table resultTable = tEnv.fromDataStream(resultStream, schema);
        resultTable = resultTable.as(outputCols[0], outputCols[1], outputCols[2]);

        return new Table[]{resultTable};
    }

    public DataStream<Row> computeTimeFeatures(DataStream<Row> inputStream) {
        String inputCol = getInputCol();


        TypeInformation<?>[] types = new TypeInformation[]{Types.INT, Types.STRING, Types.STRING};
        String[] fieldNames = new String[]{"year", "month", "day"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types,fieldNames);


        return inputStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                String dateString = (String) row.getField(inputCol);

                if (dateString == null) {
                    return Row.of(null, null, null);
                }
                try {
                    LocalDate date = LocalDate.parse(dateString, DATE_FORMATTER);
                    int year = date.getYear();
                    String month = String.format("%02d", date.getMonthValue());
                    String day = String.format("%02d", date.getDayOfMonth());
                    return Row.of(year, month, day);
                } catch (DateTimeParseException e) {
                    return Row.of(null, null, null);
                }
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

    public static TimeFeatureExtractor2 load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
package cn.swust.algorithms.featureGeneration.mean;

import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MeanImputer implements Estimator<MeanImputer, MeanImputerModel>, MeanImputerParams<MeanImputer> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public MeanImputer() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public MeanImputerModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "Input tables must be exactly 1.");

        // Calculate mean value from the input table
        double meanValue = 0;
        try {
            meanValue = calculateMean(inputs[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new MeanImputerModel(paramMap, meanValue);
    }

    private double calculateMean(Table inputTable) throws Exception {
        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputTable).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputTable);

        // Create accumulators for sum and count
        final double[] sum = {0.0};
        final long[] count = {0};

        dataStream.executeAndCollect().forEachRemaining(row -> {
            Number value = (Number) row.getField(getInputCol());
            if (value != null) {
                sum[0] += value.doubleValue();
                count[0]++;
            }
        });

        return count[0] == 0 ? 0 : sum[0] / count[0];
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static MeanImputer load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
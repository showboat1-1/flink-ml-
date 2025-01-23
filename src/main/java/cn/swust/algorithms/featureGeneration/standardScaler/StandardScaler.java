package cn.swust.algorithms.featureGeneration.standardScaler;

import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class StandardScaler implements Estimator<StandardScaler, StandardScalerModel>, StandardScalerParams<StandardScaler> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    public StandardScaler(){
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public StandardScalerModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "Input tables must be exactly 1.");
        return new StandardScalerModel(paramMap);
    }
    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static StandardScaler load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
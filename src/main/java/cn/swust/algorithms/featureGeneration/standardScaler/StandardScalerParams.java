package cn.swust.algorithms.featureGeneration.standardScaler;

import org.apache.flink.ml.common.param.HasInputCol;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;

public interface StandardScalerParams<T> extends HasInputCol<T> {
    Param<String> OUTPUT_COL = new StringParam("outputCol", "output column name", "scaled_value");

    default String getOutputCol() {
        return get(OUTPUT_COL);
    }

    default T setOutputCol(String value) {
        return set(OUTPUT_COL, value);
    }
}
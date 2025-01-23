package cn.swust.algorithms.featureGeneration.time;

import org.apache.flink.ml.common.param.HasInputCol;
import org.apache.flink.ml.param.StringArrayParam;

public interface TimeFeatureExtractorParams<T> extends HasInputCol<T> {
    StringArrayParam OUTPUT_COLS = new StringArrayParam("outputCols", "output column names", new String[] { "year", "month", "day" });


    default String[] getOutputCols() {
        return get(OUTPUT_COLS);
    }

    default T setOutputCols(String... value) {
        return set(OUTPUT_COLS, value);
    }
}
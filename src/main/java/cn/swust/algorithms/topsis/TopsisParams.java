package cn.swust.algorithms.topsis;

import org.apache.flink.ml.common.param.HasInputCols;
import org.apache.flink.ml.common.param.HasOutputCol;
import org.apache.flink.ml.param.DoubleArrayParam;

public interface TopsisParams<T> extends HasInputCols<T>, HasOutputCol<T> {
    DoubleArrayParam WEIGHTS = new DoubleArrayParam("weights", "Weights for input columns", null);

    default double[] getWeights() {
        Double[] boxedWeights = get(WEIGHTS);
        if (boxedWeights == null) {
            return null;
        }
        double[] weights = new double[boxedWeights.length];
        for (int i = 0; i < boxedWeights.length; i++) {
            weights[i] = boxedWeights[i];
        }
        return weights;
    }

    default T setWeights(double[] value) {
        return set(WEIGHTS, java.util.Arrays.stream(value).boxed().toArray(Double[]::new));
    }
}

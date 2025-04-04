package cn.swust.algorithms.topsis;

import org.apache.flink.ml.common.param.*;
import org.apache.flink.ml.param.*;

public interface TopsisParams<T> extends HasFeaturesCol<T>, HasPredictionCol<T>{
    //权重
    Param<Double[]> WEIGHTS =
            new DoubleArrayParam(
                    "weights",
                    "The weight value for each column.",
                    null,
                    ParamValidators.nonEmptyArray());
    //标准类型
    Param<Integer[]> CRITERIA_TYPES =
            new IntArrayParam(
                    "criteria_types",
                    "The standard type for each column.\n" +
                            "The corresponding value of the enumeration quantity\n" +
                            "1:Extremely large\n" +
                            "2:Extremely small\n" +
                            "3:Intermediate\n" +
                            "4:Interval",
                    null,
                    ParamValidators.nonEmptyArray());
    //最优值
    Param<Double> BEST_VALUE = new DoubleParam(
            "best_value",
            "When there is an intermediate type, the optimal value needs to be given",
            null
    );
    //区间值
    Param<Double[]> INTERVAL_VALUE = new DoubleArrayParam(
            "interval_value",
            "When there is an interval type, the optimal value needs to be given. " +
                    "Please give the interval value correctly to ensure that the lvalue is less than the rvalue",
            null
    );

    default T setWeights(Double... value) {
        return set(WEIGHTS, value);
    }
    default Double[] getWeights() {
        return get(WEIGHTS);
    }
    default T setCriteriaTypes(Integer... value) {
        return set(CRITERIA_TYPES, value);
    }
    default Integer[] getCriteriaTypes() {
        return get(CRITERIA_TYPES);
    }
    default T setBestValue(Double value) {
        return set(BEST_VALUE, value);
    }
    default Double getBestValue() {
        return get(BEST_VALUE);
    }
    default T setIntervalValue(Double... value) {
        return set(INTERVAL_VALUE, value);
    }
    default Double[] getIntervalValue() {
        return get(INTERVAL_VALUE);
    }

}

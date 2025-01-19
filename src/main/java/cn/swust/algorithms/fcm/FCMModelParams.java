package cn.swust.algorithms.fcm;

import org.apache.flink.ml.common.param.HasDistanceMeasure;
import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasPredictionCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

public interface FCMModelParams<T> extends HasDistanceMeasure<T>, HasFeaturesCol<T>, HasPredictionCol<T> {
    Param<Integer> K =
            new IntParam(
                    "k",
                    "The max number of clusters to create.",
                    3,
                    ParamValidators.gt(1));

    Param<Double> M =
            new DoubleParam(
                    "m",
                    "Fuzziness coefficient.",
                    2.0,
                    ParamValidators.gt(1.0));

    Param<Double> TOL = new DoubleParam(
            "TOL",
            "Convergence tolerance. The algorithm stops when the change in membership matrix is less than this value.",
            0.0001,
            ParamValidators.gt(0.0));

    default double getM() {
        return get(M);
    }
    default T setM(double value) {
        set(M, value);
        return (T) this;
    }

    default double getTOL() {
        return get(TOL);
    }

    default T setTOL(double value) {
        set(TOL, value);
        return (T) this;
    }

    default int getK() {
        return get(K);
    }

    default T setK(int value) {
        set(K, value);
        return (T) this;
    }
}

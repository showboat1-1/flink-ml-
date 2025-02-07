package cn.swust.algorithms.canopy;

import org.apache.flink.ml.common.param.*;
import org.apache.flink.ml.param.DoubleParam;

public interface CanopyParams<T> extends HasFeaturesCol<T>, HasPredictionCol<T>, HasMaxIter<T>, HasSeed<T>,HasDistanceMeasure<T> {
    // T1 is the loose threshold distance
    DoubleParam T1 = new DoubleParam(
            "T1",
            "Loose threshold distance for Canopy clustering.",
            1.0);

    // T2 is the tight threshold distance
    DoubleParam T2 = new DoubleParam(
            "T2",
            "Tight threshold distance for Canopy clustering.",
            0.5);

    default Double getT1() {
        return get(T1);
    }
    default T setT1(Double value) {
        return set(T1, value);
    }

    default Double getT2() {
        return get(T2);
    }
    default T setT2(Double value) {
        return set(T2, value);
    }
}

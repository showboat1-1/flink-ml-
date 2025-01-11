package cn.swust.algorithms.apriori;

import org.apache.flink.ml.common.param.HasInputCols;
import org.apache.flink.ml.common.param.HasMaxIter;
import org.apache.flink.ml.common.param.HasOutputCols;
import org.apache.flink.ml.param.*;

public interface AprioriParams<T> extends HasInputCols<T>,HasOutputCols<T> , HasMaxIter<T> {
    Param<Double> MIN_SUPPORT =
            new DoubleParam(
                    "minSupport",
                    "Minimum support threshold.",
                    0.5,
                    ParamValidators.gtEq(0.0));

    Param<Double> MIN_CONFIDENCE =
            new DoubleParam(
                    "minConfidence",
                    "Minimum confidence threshold.",
                    0.5,
                    ParamValidators.gtEq(0.0));

    Param<Double> LIFT =
            new DoubleParam(
                    "lift",
                    "Minimum lift threshold.",
                    1.0,
                    ParamValidators.gtEq(0.0));

    Param<String> ITEM_SEPARATOR =
            new StringParam(
                    "itemSeparator",
                    "Item separator.",
                    null);
    default T setItemSeparator(String value) {return set(ITEM_SEPARATOR, value);}

    default String getItemSeparator() {return get(ITEM_SEPARATOR);}

    default T setMinSupport(Double value) {return set(MIN_SUPPORT, value);}

    default Double getMinSupport() {return get(MIN_SUPPORT);}

    default T setMinConfidence(Double value) {return set(MIN_CONFIDENCE, value);}

    default Double getMinConfidence() {return get(MIN_CONFIDENCE);}

    default T setLift(Double value) {return set(LIFT, value);}

    default Double getLift() {return get(LIFT);}
}

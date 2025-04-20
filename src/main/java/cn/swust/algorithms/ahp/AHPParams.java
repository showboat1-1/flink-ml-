package cn.swust.algorithms.ahp;

import org.apache.flink.ml.common.param.HasInputCols;
import org.apache.flink.ml.common.param.HasOutputCol;
import org.apache.flink.ml.param.DoubleArrayArrayParam;
import org.apache.flink.ml.param.IntArrayParam;
import org.apache.flink.ml.param.Param;

public interface AHPParams<T> extends HasInputCols<T>, HasOutputCol<T> {

    Param<Double[][]> JUDGMENT_MATRIX = new DoubleArrayArrayParam(
            "judgmentMatrix",
            "Judgment matrix",
            null
    );

    Param<Integer[]>INDICATOR_TYPE = new IntArrayParam(
            "indicatorType",
            "The type of metric for each column. " +
                    "Positive indicators : 1," +
                    "Negative indicators : 0",
            null
    );
    default Integer[] getIndicatorType() {
        return get(INDICATOR_TYPE);
    }
    default T setIndicatorType(Integer... indicatorType) {
        return set(INDICATOR_TYPE, indicatorType);
    }

    default Double[][] getJudgmentMatrix() {
        return get(JUDGMENT_MATRIX);
    }
    default T setJudgmentMatrix(Double[][] judgmentMatrix) {
        return set(JUDGMENT_MATRIX, judgmentMatrix);
    }
}
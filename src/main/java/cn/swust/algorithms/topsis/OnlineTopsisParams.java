package cn.swust.algorithms.topsis;

import org.apache.flink.ml.common.param.HasWindows;

public interface OnlineTopsisParams<T>
        extends HasWindows<T>,OnlineTopsisModelParams<T>{
}

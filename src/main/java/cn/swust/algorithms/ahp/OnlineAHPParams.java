package cn.swust.algorithms.ahp;

import org.apache.flink.ml.common.param.HasWindows;

public interface OnlineAHPParams<T> extends HasWindows<T>,AHPParams<T> {
}

package cn.swust.algorithms.topsis;

import org.apache.flink.ml.common.param.HasMaxAllowedModelDelayMs;

public interface OnlineTopsisModelParams<T> extends TopsisParams<T>, HasMaxAllowedModelDelayMs<T> {
}

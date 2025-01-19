package cn.swust.algorithms.fcm;

import org.apache.flink.ml.common.param.HasMaxIter;
import org.apache.flink.ml.common.param.HasSeed;

public interface FCMParams<T> extends HasSeed<T>, HasMaxIter<T>,FCMModelParams<T> {

}

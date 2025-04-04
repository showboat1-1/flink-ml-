package cn.swust.algorithms.topsis;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Model data of {@link TopsisModelData}.
 *
 * <p>This class also provides methods to convert model data from Table to Datastream, and classes
 * to save/load model data.
 */
public class TopsisModelData {
    /** 聚合后的数据 */
    public DenseVector data;
    /** 模型产生最大时间戳  */
    public long timestamp;
    /** 模型产生最小时间戳  */
    public long minTimestamp;
    /** 得分 */
    public double score;

    public TopsisModelData() {
    }

    public TopsisModelData(DenseVector data, long timestamp,long minTimestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.score = 0.0;
        this.minTimestamp = minTimestamp;
    }

    public TopsisModelData(long timestamp, long minTimestamp, double score) {
        this.timestamp = timestamp;
        this.minTimestamp = minTimestamp;
        this.score = score;
    }

    public TopsisModelData(DenseVector data, long timestamp, double score, long minTimestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.score = score;
        this.minTimestamp = minTimestamp;
    }

    /**
     * Converts the table model to a data stream.
     *
     * @param modelData The table model data.
     * @return The data stream model data.
     */
    public static DataStream<TopsisModelData> getModelDataStream(Table modelData) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) modelData).getTableEnvironment();

        return tEnv.toDataStream(modelData)
                .map(
                        (MapFunction<Row, TopsisModelData>)
                                row ->
                                        new TopsisModelData(
                                                row.getFieldAs("data"),
                                                row.getFieldAs("timestamp"),
                                                row.getFieldAs("score"),
                                                row.getFieldAs("minTimestamp")))
                .setParallelism(1);
    }

    /** Data encoder for the {@link TopsisModelData} model data. */
    public static class ModelDataEncoder implements Encoder<TopsisModelData> {
        private final DenseVectorSerializer serializer = new DenseVectorSerializer();

        @Override
        public void encode(TopsisModelData modelData, OutputStream outputStream)
                throws IOException {
            DataOutputViewStreamWrapper outputViewStreamWrapper =
                    new DataOutputViewStreamWrapper(outputStream);

            serializer.serialize(modelData.data, outputViewStreamWrapper);
            LongSerializer.INSTANCE.serialize(modelData.timestamp, outputViewStreamWrapper);
            LongSerializer.INSTANCE.serialize(modelData.minTimestamp, outputViewStreamWrapper);
            DoubleSerializer.INSTANCE.serialize(modelData.score,outputViewStreamWrapper);
        }
    }

    /** Data decoder for the {@link TopsisModelData} model data. */
    public static class ModelDataDecoder extends SimpleStreamFormat<TopsisModelData> {
        @Override
        public Reader<TopsisModelData> createReader(
                Configuration configuration, FSDataInputStream inputStream) {
            return new Reader<TopsisModelData>() {
                private final DenseVectorSerializer serializer = new DenseVectorSerializer();

                @Override
                public TopsisModelData read() throws IOException {
                    DataInputViewStreamWrapper inputViewStreamWrapper =
                            new DataInputViewStreamWrapper(inputStream);

                    try {
                        DenseVector data = serializer.deserialize(inputViewStreamWrapper);
                        long timestamp =
                                LongSerializer.INSTANCE.deserialize(inputViewStreamWrapper);
                        long minTimestamp =
                                LongSerializer.INSTANCE.deserialize(inputViewStreamWrapper);
                        double score =
                                DoubleSerializer.INSTANCE.deserialize(inputViewStreamWrapper);
                        return new TopsisModelData(data, timestamp,score,minTimestamp);
                    } catch (EOFException e) {
                        return null;
                    }
                }

                @Override
                public void close() throws IOException {
                    inputStream.close();
                }
            };
        }

        @Override
        public TypeInformation<TopsisModelData> getProducedType() {
            return TypeInformation.of(TopsisModelData.class);
        }
    }
}

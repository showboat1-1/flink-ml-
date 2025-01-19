package cn.swust.algorithms.fcm;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Model data of {@link FCMModel}.
 *
 * <p>This class also provides methods to convert model data from Table to Datastream, and classes
 * to save/load model data.
 */

public class FCMModelData {
    //聚类中心
    public DenseVector[] centroids;

    //隶属矩阵 : Tuple2<> f0:样本 f1:对应隶属度
    public Tuple2<DenseVector, DenseVector>[] membershipMatrix;

    public FCMModelData(DenseVector[] centroids, Tuple2<DenseVector, DenseVector>[] membershipMatrix) {
        //检查 列长
        Preconditions.checkArgument(centroids.length == membershipMatrix[0].f1.size());
        this.centroids = centroids;
        this.membershipMatrix = membershipMatrix;
    }

    public FCMModelData() {
    }

    /**
     * Converts the table model to a data stream.
     *
     * @param modelData The table model data.
     * @return The data stream model data.
     */
    public static DataStream<FCMModelData> getModelDataStream(Table modelData) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) modelData).getTableEnvironment();
        return tEnv.toDataStream(modelData)
                .map(
                        x ->
                                new FCMModelData(
                                        Arrays.stream(((Vector[]) x.getField(0)))
                                                .map(Vector::toDense)
                                                .toArray(DenseVector[]::new),
                                        Arrays.stream(((Tuple2<DenseVector,DenseVector>[]) x.getField(1)))
                                                .toArray(Tuple2[]::new)));
    }

    /**
     * Data encoder for {@link FCMModelData}.
     */
    public static class ModelDataEncoder implements Encoder<FCMModelData> {
        private final DenseVectorSerializer serializer = new DenseVectorSerializer();

        @Override
        public void encode(FCMModelData modelData, OutputStream outputStream)
                throws IOException {
            DataOutputViewStreamWrapper outputViewStreamWrapper =
                    new DataOutputViewStreamWrapper(outputStream);
            //序列化聚类中心
            IntSerializer.INSTANCE.serialize(modelData.centroids.length, outputViewStreamWrapper);
            for (DenseVector denseVector : modelData.centroids) {
                serializer.serialize(denseVector, new DataOutputViewStreamWrapper(outputStream));
            }
            //构建2元组序列化器
            DenseVectorSerializer dvs = new DenseVectorSerializer();
            TypeSerializer<?>[] typeSerializers = {dvs, dvs};
            Tuple2<DenseVector, DenseVector> tuple2 = new Tuple2<>();
            TupleSerializer<Tuple2<DenseVector, DenseVector>> t2s =
                    new TupleSerializer<>((Class<Tuple2<DenseVector, DenseVector>>) tuple2.getClass(), typeSerializers);

            //序列化隶属矩阵
            IntSerializer.INSTANCE.serialize(modelData.membershipMatrix.length, outputViewStreamWrapper);
            for (Tuple2<DenseVector, DenseVector> mm : modelData.membershipMatrix) {
                t2s.serialize(mm, new DataOutputViewStreamWrapper(outputStream));
            }
        }
    }

    /**
     * Data decoder for {@link FCMModelData}.
     */
    public static class ModelDataDecoder extends SimpleStreamFormat<FCMModelData> {
        @Override
        public Reader<FCMModelData> createReader(
                Configuration config, FSDataInputStream inputStream) {
            return new Reader<FCMModelData>() {
                private final DenseVectorSerializer serializer = new DenseVectorSerializer();

                @Override
                public FCMModelData read() throws IOException {
                    try {
                        //反序列化聚类中心
                        DataInputViewStreamWrapper inputViewStreamWrapper =
                                new DataInputViewStreamWrapper(inputStream);
                        int numDenseVectors =
                                IntSerializer.INSTANCE.deserialize(inputViewStreamWrapper);
                        DenseVector[] centroids = new DenseVector[numDenseVectors];
                        for (int i = 0; i < numDenseVectors; i++) {
                            centroids[i] = serializer.deserialize(inputViewStreamWrapper);
                        }
                        //反序列化隶属矩阵
                        //构建2元组序列化器
                        DenseVectorSerializer dvs = new DenseVectorSerializer();
                        TypeSerializer<?>[] typeSerializers = {dvs, dvs};
                        Tuple2<DenseVector, DenseVector> tuple2 = new Tuple2<>();
                        TupleSerializer<Tuple2<DenseVector, DenseVector>> t2s =
                                new TupleSerializer<>((Class<Tuple2<DenseVector, DenseVector>>) tuple2.getClass(), typeSerializers);

                        int numMembership =
                                IntSerializer.INSTANCE.deserialize(inputViewStreamWrapper);
                        Tuple2<DenseVector, DenseVector>[] membershipMatrix = new Tuple2[numMembership];
                        for (int i = 0; i < numMembership; i++) {
                            membershipMatrix[i] = t2s.deserialize(inputViewStreamWrapper);
                        }

                        return new FCMModelData(centroids, membershipMatrix);
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
        public TypeInformation<FCMModelData> getProducedType() {
            return TypeInformation.of(FCMModelData.class);
        }
    }
}

package cn.swust.algorithms.fcm;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FCMModel implements Model<FCMModel>,FCMModelParams<FCMModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table modelDataTable;

    public FCMModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public FCMModel setModelData(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        modelDataTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelDataTable};
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<FCMModelData> modelDataStream =
                FCMModelData.getModelDataStream(modelDataTable);

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
//        modelDataStream.map(r->{
//            System.out.println(Arrays.toString(r.centroids));
//            System.out.println(Arrays.toString(r.membershipMatrix));
//            return r;
//        }, TypeInformation.of(FCMModelData.class));

        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol()));

        final String broadcastModelKey = "broadcastModelKey";
        DataStream<Row> predictionResult =
                BroadcastUtils.withBroadcastStream(
                        Collections.singletonList(tEnv.toDataStream(inputs[0])),//样本数据点
                        Collections.singletonMap(broadcastModelKey, modelDataStream),//训练好的模型数据，一般 只需 用 聚类中心
                        inputList -> {
                            DataStream inputData = inputList.get(0);
                            return inputData.map(
                                    new FCMModel.PredictLabelFunction(
                                            broadcastModelKey,
                                            getFeaturesCol(),
                                            DistanceMeasure.getInstance(getDistanceMeasure()),
                                            getK(),
                                            getM()),
                                   outputTypeInfo);
                        });
        return new Table[] {tEnv.fromDataStream(predictionResult)};
    }

    /** A utility function used for prediction. */
    private static class PredictLabelFunction extends RichMapFunction<Row, Row> {

        private final String broadcastModelKey;

        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private final int k;

        private final double m;

        private DenseVector[] centroids;

        public PredictLabelFunction(
                String broadcastModelKey,
                String featuresCol,
                DistanceMeasure distanceMeasure,
                int k,
                double m) {
            this.broadcastModelKey = broadcastModelKey;
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
            this.k = k;
            this.m = m;
        }

        @Override
        public Row map(Row dataPoint) {
            if (centroids == null) {
                FCMModelData modelData =
                        (FCMModelData)
                                getRuntimeContext().getBroadcastVariable(broadcastModelKey).get(0);
                Preconditions.checkArgument(modelData.centroids.length <= k);
                centroids = modelData.centroids;
            }
            DenseVector point = ((Vector) dataPoint.getField(featuresCol)).toDense();
            //计算样本对应每一个聚类中心的隶属度，选择隶属度最大的聚类中心作为样本的聚类中心
            DenseVector membershipVector = FCM.updateMembershipVector(point, centroids, distanceMeasure, m);

            //寻找最大隶属度列
            int closestCentroidId = -1;
            double maxDis = -1;
            for (int i = 0; i < membershipVector.size(); i++) {
                if (membershipVector.get(i) > maxDis) {
                    maxDis = membershipVector.get(i);
                    closestCentroidId = i;
                }
            }
            return Row.join(dataPoint, Row.of(closestCentroidId));
        }
    }


    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveModelData(
                FCMModelData.getModelDataStream(modelDataTable),
                path,
                new FCMModelData.ModelDataEncoder());
        ReadWriteUtils.saveMetadata(this, path);
    }

    // TODO: Add INFO level logging.
    public static FCMModel load(StreamTableEnvironment tEnv, String path) throws IOException {
        Table modelDataTable = ReadWriteUtils.loadModelData(tEnv, path, new FCMModelData.ModelDataDecoder());
        FCMModel model = ReadWriteUtils.loadStageParam(path);
        return model.setModelData(modelDataTable);
    }
}

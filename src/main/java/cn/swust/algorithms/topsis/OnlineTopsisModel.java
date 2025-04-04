package cn.swust.algorithms.topsis;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

public class OnlineTopsisModel implements Model<OnlineTopsisModel>,
        OnlineTopsisModelParams<OnlineTopsisModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table modelDataTable;

    public OnlineTopsisModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }


    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        //构建输出列
        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        TypeInformation<?>[] outputTypes;
        String[] outputNames;
        //id features rowtime prediction
        outputTypes = ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), TypeInformation.of(DenseVector.class));
        outputNames = ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol());
        RowTypeInfo outputTypeInfo = new RowTypeInfo(outputTypes, outputNames);

        //构建结果
        SingleOutputStreamOperator<Row> predictionData = tEnv.toDataStream(inputs[0])
                .connect(
                        TopsisModelData.getModelDataStream(modelDataTable)
                                .broadcast())
                .transform(
                        "PredictionOperator",
                        outputTypeInfo,
                        new PredictionOperator(
                                inputTypeInfo,
                                getFeaturesCol(),
                                getMaxAllowedModelDelayMs()));
//        predictionData.print();

        return new Table[] {tEnv.fromDataStream(predictionData)};
    }

    private static class PredictionOperator extends AbstractStreamOperator<Row>
            implements TwoInputStreamOperator<Row, TopsisModelData, Row>{
        private final RowTypeInfo inputTypeInfo;
        private final String featuresCol;
        private final long maxAllowedModelDelayMs;
        private ListState<StreamRecord> bufferedPointsState;
        private ListState<TopsisModelData> modelDataState;
        /** Model data for inference. */
        private TopsisModelData modelData;
        private long modelTimeStamp;
        private long modelMinTimeStamp;
        private double score;

        public PredictionOperator(RowTypeInfo inputTypeInfo, String featuresCol, long maxAllowedModelDelayMs) {
            this.inputTypeInfo = inputTypeInfo;
            this.featuresCol = featuresCol;
            this.maxAllowedModelDelayMs = maxAllowedModelDelayMs;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            bufferedPointsState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<StreamRecord>(
                                            "bufferedPoints",
                                            new StreamElementSerializer(
                                                    inputTypeInfo.createSerializer(
                                                            getExecutionConfig()))));

            modelDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "modelData",
                                            TypeInformation.of(TopsisModelData.class)));
            modelData =
                    OperatorStateUtils.getUniqueElement(modelDataState, "modelData").orElse(null);
            if (modelData != null) {
                initializeModelData(modelData);
            } else {
                modelTimeStamp = -1;
                score = -1;
                modelMinTimeStamp = Long.MIN_VALUE;
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            if (modelData != null) {
                modelDataState.clear();
                modelDataState.add(modelData);
            }
        }

        @Override
        public void processElement1(StreamRecord<Row> dataPoint) throws Exception {
            if (dataPoint.getTimestamp() - maxAllowedModelDelayMs <= modelTimeStamp &&
                    dataPoint.getTimestamp() - maxAllowedModelDelayMs >=modelMinTimeStamp) {
                doPrediction(dataPoint);
            } else {
                bufferedPointsState.add(dataPoint);
            }
        }

        @Override
        public void processElement2(StreamRecord<TopsisModelData> streamRecord) throws Exception {
            modelData = streamRecord.getValue();
            initializeModelData(modelData);

            // Does prediction on the cached data.
            List<StreamRecord> unprocessedElements = new ArrayList<>();
            boolean predictedCachedData = false;
            for (StreamRecord dataPoint : bufferedPointsState.get()) {
                if (dataPoint.getTimestamp() - maxAllowedModelDelayMs <= modelTimeStamp &&
                        dataPoint.getTimestamp() - maxAllowedModelDelayMs >=modelMinTimeStamp ) {
                    doPrediction(dataPoint);
                    predictedCachedData = true;
                } else {
                    unprocessedElements.add(dataPoint);
                }
            }
            if (predictedCachedData) {
                bufferedPointsState.clear();
                if (unprocessedElements.size() > 0) {
                    bufferedPointsState.update(unprocessedElements);
                }
            }
        }

        private void doPrediction(StreamRecord<Row> streamRecord) {
            Row dataPoint = streamRecord.getValue();
            DenseVector dv = new DenseVector(1);
            dv.set(0,score);
            output.collect(
                    new StreamRecord<>(
                            rowAppend(dataPoint,dv),
                            streamRecord.getTimestamp()));
        }

        private void initializeModelData(TopsisModelData modelData) {
            modelTimeStamp = modelData.timestamp;
            score = modelData.score;
            modelMinTimeStamp = modelData.minTimestamp;
        }

        private static Row rowAppend(Row existing, Object value) {
            Row result = cloneWithReservedFields(existing, 1);
            result.setField(existing.getArity(), value);
            return result;
        }

        private static Row cloneWithReservedFields(Row existing, int reservedFields) {
            Row result = new Row(existing.getKind(), existing.getArity() + reservedFields);
            for (int i = 0; i < existing.getArity(); i++) {
                result.setField(i, existing.getField(i));
            }
            return result;
        }

    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineTopsisModel load(StreamTableEnvironment tEnv, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public OnlineTopsisModel setModelData(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        modelDataTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelDataTable};
    }
}

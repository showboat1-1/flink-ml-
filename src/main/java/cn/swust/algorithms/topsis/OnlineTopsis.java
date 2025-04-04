package cn.swust.algorithms.topsis;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.window.Windows;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

public class OnlineTopsis implements Estimator<OnlineTopsis,OnlineTopsisModel>,
        OnlineTopsisParams<OnlineTopsis> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public OnlineTopsis() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }
    //标准类型常量
    private static final Integer EXTREMELY_LARGE = 1;
    private static final Integer EXTREMELY_SMALL = 2;
    private static final Integer INTERMEDIATE = 3;
    private static final Integer INTERVAL = 4;

    @Override
    public OnlineTopsisModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        //检查标准类型
        checkCriteriaTypes(getCriteriaTypes());

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        //获取窗口处理策略
        Windows windows = getWindows();

        //进行窗口聚合，计算均值
        SingleOutputStreamOperator<TopsisModelData> modelData = DataStreamUtils.windowAllAndProcess(
                tEnv.toDataStream(inputs[0]),
                windows,
                new ComputeAvgDataFunction<>(getFeaturesCol()));

        //应用topsis算法
        SingleOutputStreamOperator<DenseVector> tData =modelData.map(r->r.data);
        //f0:原始数据 f1:得分
        DataStream<Tuple2<DenseVector, DenseVector>> topsisData = applyTopsis(tData);
//        topsisData.print();

        //更新模型数据
        final String broadcastModelKey = "broadcastModelKey";
        DataStream<TopsisModelData> newModelData = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(topsisData),
                Collections.singletonMap(broadcastModelKey, modelData),
                inputList -> {
                    DataStream inputData = inputList.get(0);
                    return inputData.map(
                            new UpdateModelData(broadcastModelKey),
                            TypeInformation.of(TopsisModelData.class));
                });

        OnlineTopsisModel model =
                new OnlineTopsisModel().setModelData(tEnv.fromDataStream(newModelData));

        ParamUtils.updateExistingParams(model, paramMap);
        return model;
    }

    private static class UpdateModelData extends RichMapFunction<Tuple2<DenseVector, DenseVector>, TopsisModelData>{
        private final String broadcastModelKey;
        List<TopsisModelData> modelDataList;

        private UpdateModelData(String broadcastModelKey) {
            this.broadcastModelKey = broadcastModelKey;
        }

        @Override
        public TopsisModelData map(Tuple2<DenseVector, DenseVector> value) throws Exception {
            if (modelDataList == null) {
                modelDataList = getRuntimeContext().getBroadcastVariable(broadcastModelKey);
            }
            TopsisModelData model = null;
            for (TopsisModelData topsisModelData : modelDataList) {
                if (Objects.hashCode(value.f0) == Objects.hashCode(topsisModelData.data)) {
                    model = new TopsisModelData(topsisModelData.data,topsisModelData.timestamp,value.f1.get(0),topsisModelData.minTimestamp);
                    break;
                }
            }
            return model;
        }
    }



    //计算每一个窗口中每一列的均值
    private static class ComputeAvgDataFunction<W extends Window>
            extends ProcessAllWindowFunction<Row, TopsisModelData, W> {

        private final String featuresCol;

        //计算窗口内的均值
        private ComputeAvgDataFunction(String featuresCol) {
            this.featuresCol = featuresCol;
        }

        @Override
        public void process(
                ProcessAllWindowFunction<Row, TopsisModelData, W>.Context context,
                Iterable<Row> elements,
                Collector<TopsisModelData> out) throws Exception {
            DenseVector sum = null;
            long count = 0;
            //记录窗口中最小的时间戳
            long minTimestamp = Long.MAX_VALUE;
            for (Row element : elements) {
                Long timeStamp = (Long) Objects.requireNonNull(element.getField("id"));
                minTimestamp = Math.min(timeStamp,minTimestamp);
                Vector inputVec =
                        ((Vector) Objects.requireNonNull(element.getField(featuresCol))).clone();
                if (sum == null){
                    sum = new DenseVector(inputVec.size());
                }
                /** y += a * x . */
                BLAS.axpy(1, inputVec, sum);
                count++;
            }
            DenseVector avg = new DenseVector(sum.size());
            BLAS.axpy(1.0/count, sum, avg);
            //获取窗口中最大时间戳
            long timestamp = context.window().maxTimestamp();
            out.collect(new TopsisModelData(avg,timestamp,minTimestamp));
        }
    }
    private DataStream<Tuple2<DenseVector,DenseVector>> applyTopsis(DataStream<DenseVector> inputDataStream){
        //1.将原始矩阵正向化，就是要将所有的指标类型统一转化为极大型指标
        //1.1 先对中间型 计算Xi-Xbest
        SingleOutputStreamOperator<DenseVector> dealData1 =
                inputDataStream.map(
                        new MapUDF1(getCriteriaTypes(),getBestValue()),
                        TypeInformation.of(DenseVector.class));
//        dealData1.print();

        //1.2单独处理区间类型指标 Tuple3 : f0:索引 f1:min f2:max
        SingleOutputStreamOperator<List<Tuple3<Integer, Double, Double>>> dealData2 = inputDataStream.transform(
                " CalPerColMaxValue",
                Types.LIST(Types.TUPLE(Types.INT, Types.DOUBLE, Types.DOUBLE)),
                new CalPerColMaxValue(getCriteriaTypes()));
//        dealData2.print();

        //进行结果合并
        int parallelism = dealData2.getParallelism();
        DataStream<List<Tuple3<Integer, Double, Double>>> dealData2final =
                dealData2
                        //当并行度为3时，会产生3条 并行处理 的数据 此时设置计数窗口为并行度，即将产生的三条数据进行归约处理，最终产生一条数据
                        .countWindowAll(parallelism)
                        .reduce(
                                (ReduceFunction<List<Tuple3<Integer, Double, Double>>>) (value1, value2) -> {
                                    if(value1.size() > 0 && value2.size() == 0) return  value1;
                                    if(value1.size() == 0 && value2.size() > 0) return  value2;
                                    List<Tuple3<Integer, Double, Double>> lt = new ArrayList<>();
                                    for (Tuple3<Integer, Double, Double> v1 : value1) {
                                        for (Tuple3<Integer, Double, Double> v2 : value2) {
                                            if (Objects.equals(v1.f0, v2.f0)) {
                                                double maxNum = Math.max(v1.f2, v2.f2);
                                                double minNum = Math.min(v1.f1, v2.f1);
                                                lt.add(Tuple3.of(v1.f0, minNum, maxNum));
                                            }
                                        }
                                    }
                                    return lt;
                                });
//        dealData2final.print();

        //1.3 同时对极小型和中间型进行处理
        DataStream<DenseVector> dealData1final = DataStreamUtils.reduce(dealData1, new ReduceUDF1(getCriteriaTypes()));
//        dealData1final.print();

        //1.4 将处理的结果广播出去，计算最终结果
        final String bcExtremelySmallAndIntermediate = "bcExtremelySmallAndIntermediate";
        final String bcInterval = "bcInterval";
        HashMap<String, DataStream<?>> bcMap =
                new HashMap<String, DataStream<?>>() {
                    {
                        put(bcExtremelySmallAndIntermediate, dealData1final);
                        put(bcInterval, dealData2final);
                    }
                };
        DataStream<Tuple2<DenseVector,DenseVector>> middleDataStream = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(inputDataStream),
                bcMap,
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new Positiveization(
                                    bcExtremelySmallAndIntermediate,
                                    bcInterval,
                                    getCriteriaTypes(),
                                    getBestValue(),
                                    getIntervalValue()),
                            Types.TUPLE(
                                    TypeInformation.of(DenseVector.class),
                                    TypeInformation.of(DenseVector.class)));
                });
//        middleDataStream.print();

        //2.正向矩阵标准化
        //2.1 计算每一的平方
        SingleOutputStreamOperator<DenseVector> middleDealData1 = middleDataStream.map(r -> {
            DenseVector dv = new DenseVector(r.f1.size());
            for (int i = 0; i < dv.size(); i++) {
                dv.set(i,Math.pow(r.f1.get(i),2));
            }
            return dv;
        }, TypeInformation.of(DenseVector.class));
        //2.2 计算每一行的平方和
        DataStream<DenseVector> middleDealData2 = DataStreamUtils.reduce(middleDealData1,
                (ReduceFunction<DenseVector>) (value1, value2) -> {
                    DenseVector dv = new DenseVector(value1.size());
                    for (int i = 0; i < value1.size(); i++) {
                        dv.set(i, value1.get(i) + value2.get(i));
                    }
                    return dv;
                });
//        middleDealData2.print();

        //2.3 标准化
        final String bcMiddleDealData2 = "middleDealData2";
        DataStream<Tuple2<DenseVector,DenseVector>> middleDealDataFinal = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(middleDataStream),
                Collections.singletonMap(bcMiddleDealData2, middleDealData2),
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new Standardization(bcMiddleDealData2, getWeights()),
                            Types.TUPLE(
                                    TypeInformation.of(DenseVector.class),
                                    TypeInformation.of(DenseVector.class)));
                });
//        middleDealDataFinal.print();

        //3.计算得分并归一化
        //3.1计算每一列的最大值
        SingleOutputStreamOperator<DenseVector> thirdDealData = middleDealDataFinal.map(r -> r.f1, TypeInformation.of(DenseVector.class));
        DataStream<DenseVector> thirdDealMaxData = DataStreamUtils.reduce(thirdDealData,
                (ReduceFunction<DenseVector>) (value1, value2) -> {
                    DenseVector dv = new DenseVector(value1.size());
                    for (int i = 0; i < value1.size(); i++) {
                        dv.set(i, Math.max(value1.get(i), value2.get(i)));
                    }
                    return dv;
                });
//        thirdDealMaxData.print();
        //3.2计算每一列的最小值
        DataStream<DenseVector> thirdDealMinData = DataStreamUtils.reduce(thirdDealData,
                (ReduceFunction<DenseVector>) (value1, value2) -> {
                    DenseVector dv = new DenseVector(value1.size());
                    for (int i = 0; i < value1.size(); i++) {
                        dv.set(i, Math.min(value1.get(i), value2.get(i)));
                    }
                    return dv;
                });
//        thirdDealMinData.print();

        //3.3计算得分
        final String bcThirdDealMaxData = "thirdDealMaxData";
        final String bcThirdDealMinData = "thirdDealMinData";
        HashMap<String, DataStream<?>> bcMap2 =
                new HashMap<String, DataStream<?>>() {
                    {
                        put(bcThirdDealMaxData, thirdDealMaxData);
                        put(bcThirdDealMinData, thirdDealMinData);
                    }
                };
        DataStream<Tuple2<DenseVector,DenseVector>> thirdDealDataFinal = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(middleDealDataFinal),
                bcMap2,
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new CalScore(bcThirdDealMaxData, bcThirdDealMinData),
                            Types.TUPLE(
                                    TypeInformation.of(DenseVector.class),
                                    TypeInformation.of(DenseVector.class)));
                });
        return thirdDealDataFinal;
    }

    private static class MapUDF1 extends RichMapFunction<DenseVector, DenseVector> {
        private final Integer[] criteriaTypes;
        private final Double bestValue;

        private MapUDF1(Integer[] criteriaTypes, Double bestValue) {
            this.criteriaTypes = criteriaTypes;
            this.bestValue = bestValue;
        }

        @Override
        public DenseVector map(DenseVector r) throws Exception {
            for (int i = 0; i < criteriaTypes.length; i++) {
                if (Objects.equals(criteriaTypes[i], INTERMEDIATE)) {
                    r.set(i, Math.abs(r.get(i) - bestValue));
                }
            }
            return r;
        }
    }

    private static class ReduceUDF1 implements ReduceFunction<DenseVector> {
        private final Integer[] criteriaTypes;

        private ReduceUDF1(Integer[] criteriaTypes) {
            this.criteriaTypes = criteriaTypes;
        }

        @Override
        public DenseVector reduce(DenseVector value1, DenseVector value2) throws Exception {
            DenseVector dv = new DenseVector(value1.size());
            for (int i = 0; i < criteriaTypes.length; i++) {
                if (Objects.equals(criteriaTypes[i], EXTREMELY_LARGE)) {
                    dv.set(i, -1);
                } else if (Objects.equals(criteriaTypes[i], EXTREMELY_SMALL)) {
                    dv.set(i, Math.max(value1.get(i), value2.get(i)));
                } else if (Objects.equals(criteriaTypes[i], INTERMEDIATE)) {
                    dv.set(i, Math.max(value1.get(i), value2.get(i)));
                } else {
                    dv.set(i, -1);
                }
            }
            return dv;
        }
    }

    private static class CalScore extends RichMapFunction<Tuple2<DenseVector,DenseVector>, Tuple2<DenseVector,DenseVector>> {
        private final String bcThirdDealMaxData;
        private final String bcThirdDealMinData;

        private DenseVector thirdDealMaxData;
        private DenseVector thirdDealMinData;

        private CalScore(String bcThirdDealMaxData, String bcThirdDealMinData) {
            this.bcThirdDealMaxData = bcThirdDealMaxData;
            this.bcThirdDealMinData = bcThirdDealMinData;
        }

        @Override
        public Tuple2<DenseVector, DenseVector> map(Tuple2<DenseVector, DenseVector> value) throws Exception {
            if (thirdDealMaxData == null) {
                thirdDealMaxData = (DenseVector)getRuntimeContext().getBroadcastVariable(bcThirdDealMaxData).get(0);
            }
            if (thirdDealMinData == null) {
                thirdDealMinData = (DenseVector)getRuntimeContext().getBroadcastVariable(bcThirdDealMinData).get(0);
            }
            DenseVector dv = new DenseVector(1);
            Double maxDis = 0.0;
            for (int i = 0; i < value.f1.size(); i++) {
                maxDis += Math.pow(thirdDealMaxData.get(i) - value.f1.get(i), 2);
            }
            maxDis = Math.sqrt(maxDis);

            Double minDis = 0.0;
            for (int i = 0; i < value.f1.size(); i++) {
                minDis += Math.pow(thirdDealMinData.get(i) - value.f1.get(i), 2);
            }
            minDis = Math.sqrt(minDis);

            dv.set(0, minDis / (maxDis + minDis));
            return Tuple2.of(value.f0, dv);
        }
    }

    private static class Standardization extends RichMapFunction<Tuple2<DenseVector,DenseVector>, Tuple2<DenseVector,DenseVector>>{
        private final String bcMiddleDealData2;
        private final Double[] weights;

        private DenseVector squareDenseVector;

        private Standardization(String bcMiddleDealData2, Double[] weights) {
            this.bcMiddleDealData2 = bcMiddleDealData2;
            this.weights = weights;
        }

        @Override
        public Tuple2<DenseVector, DenseVector> map(Tuple2<DenseVector, DenseVector> value) throws Exception {
            if (squareDenseVector == null) {
                squareDenseVector = (DenseVector)getRuntimeContext().getBroadcastVariable(bcMiddleDealData2).get(0);
            }
            DenseVector dv = new DenseVector(value.f1.size());
            for (int i = 0; i < dv.size(); i++) {
                dv.set(i, value.f1.get(i) / Math.sqrt(squareDenseVector.get(i)) * weights[i]);
            }
            return Tuple2.of(value.f0, dv);
        }
    }

    //tuple2 : f0:源操作数 f1：操作后的值
    private static class Positiveization extends RichMapFunction<DenseVector, Tuple2<DenseVector,DenseVector>>{
        private final String bcExtremelySmallAndIntermediate;
        private final String bcInterval;

        private final Integer[] criteriaTypes;
        private final Double bestValue;
        private final Double[] intervalValue;

        private DenseVector extremelySmallAndIntermediateVector;
        private List<Tuple3<Integer, Double, Double>> intervalList;

        private Positiveization(String bcExtremelySmallAndIntermediate, String bcInterval, Integer[] criteriaTypes, Double bestValue, Double[] intervalValue) {
            this.bcExtremelySmallAndIntermediate = bcExtremelySmallAndIntermediate;
            this.bcInterval = bcInterval;
            this.criteriaTypes = criteriaTypes;
            this.bestValue = bestValue;
            this.intervalValue = intervalValue;
        }

        @Override
        public Tuple2<DenseVector,DenseVector> map(DenseVector value) throws Exception {
            if (extremelySmallAndIntermediateVector == null){
                extremelySmallAndIntermediateVector =
                        (DenseVector) getRuntimeContext().getBroadcastVariable(bcExtremelySmallAndIntermediate).get(0);
            }
            if (intervalList == null){
                intervalList = (List<Tuple3<Integer, Double, Double>>)getRuntimeContext().getBroadcastVariable(bcInterval).get(0);
//                System.out.println(intervalList);
            }
            //处理指标
            DenseVector dv = new DenseVector(value.size());
            for (int i = 0; i < criteriaTypes.length; i++) {
                if (Objects.equals(criteriaTypes[i],EXTREMELY_SMALL)) {
                    dv.set(i, extremelySmallAndIntermediateVector.get(i) - value.get(i));
                }else if( Objects.equals(criteriaTypes[i],INTERMEDIATE)){
                    double t = Math.abs(value.get(i) - bestValue) / extremelySmallAndIntermediateVector.get(i);
                    dv.set(i,1 - t);
                }else if( Objects.equals(criteriaTypes[i],INTERVAL)){
                    for (Tuple3<Integer, Double, Double> tuple3 : intervalList) {
                        if(Objects.equals(i, tuple3.f0)){
                            double x = value.get(i);
                            Double leftValue = intervalValue[0];
                            Double rightValue = intervalValue[1];
                            double t1 = leftValue - tuple3.f1;
                            double t2 = tuple3.f2 - rightValue;
                            double m = Math.max(t1,t2);
                            if(x<leftValue){
                                dv.set(i,1 - (leftValue - x) / m);
                            }else if(x>=leftValue && x<=rightValue){
                                dv.set(i,1);
                            }else{
                                dv.set(i,1 - (x - rightValue) / m);
                            }
                        }
                    }
                }else if (Objects.equals(criteriaTypes[i],EXTREMELY_LARGE)) {
                    dv.set(i,value.get(i));
                }
            }
            return Tuple2.of(value,dv);
        }
    }

    //单独处理区间类型指标 Tuple3 : f0:索引 f1:min f2:max
    private static class CalPerColMaxValue
            extends AbstractStreamOperator<List<Tuple3<Integer, Double, Double>>>
            implements OneInputStreamOperator<DenseVector, List<Tuple3<Integer, Double, Double>>>,
            BoundedOneInput {
        private Map<Integer,Tuple3<Integer, Double, Double>> cntMap = new HashMap<>();
        private ListState<Map<Integer,Tuple3<Integer, Double, Double>>> cntMapState;

        private final Integer[] criteriaTypes;

        private CalPerColMaxValue(Integer[] criteriaTypes) {
            this.criteriaTypes = criteriaTypes;
        }

        @Override
        public void endInput() throws Exception {
            output.collect(new StreamRecord<>(new ArrayList<>(cntMap.values())));
            cntMapState.clear();
        }

        @Override
        public void processElement(StreamRecord<DenseVector> element) throws Exception {
            DenseVector value = element.getValue();
            for (int i = 0; i < criteriaTypes.length; i++) {
                if (Objects.equals(criteriaTypes[i], INTERVAL)) {
                    if (cntMap.containsKey(i)) {
                        //已经存在,取出值
                        Tuple3<Integer, Double, Double> tuple3 = cntMap.get(i);
                        //更新值
                        tuple3.f2 = Math.max(tuple3.f2, value.get(i));
                        tuple3.f1 = Math.min(tuple3.f1, value.get(i));
                        cntMap.put(i,tuple3);
                    }else{
                        //不存在
                        cntMap.put(i,new Tuple3<>(i,value.get(i),value.get(i)));
                    }
                }
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            cntMapState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "cntMapState",
                                            Types.MAP(
                                                    Types.INT,
                                                    Types.TUPLE(
                                                            Types.INT, Types.DOUBLE, Types.DOUBLE))));

            OperatorStateUtils.getUniqueElement(cntMapState, "cntMapState")
                    .ifPresent(x -> cntMap = x);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            cntMapState.update(Collections.singletonList(cntMap));
        }
    }


    private void checkCriteriaTypes(Integer[] criteriaTypes) {
        //遍历给定的标准类型
        for (Integer criteriaType : criteriaTypes) {
            if(criteriaType == INTERMEDIATE){
                //中间型,检查最优值是否为空
                Preconditions.checkNotNull(getBestValue(), "The best value of intermediate type criteria must be set");
            }else if(criteriaType == INTERVAL){
                //区间型,检查所给区间是否为空
                Preconditions.checkNotNull(getIntervalValue(), "The interval of interval type criteria must be set");
                //如果非空检查所给区间值是否满足规则
                Preconditions.checkArgument(
                        getIntervalValue()[0] < getIntervalValue()[1],
                        "The interval must be a pair of ascending numbers,ensure that the lvalue is less than the rvalue");
            }
        }
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineTopsis load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}

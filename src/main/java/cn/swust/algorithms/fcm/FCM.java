package cn.swust.algorithms.fcm;

import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.DirichletSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.iteration.*;
import org.apache.flink.iteration.datacache.nonkeyed.ListStateWithCache;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.iteration.ForwardInputsOfLastRound;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.VectorWithNorm;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.ml.clustering.kmeans.KMeans.selectRandomCentroids;
import static org.apache.flink.ml.common.datastream.DataStreamUtils.setManagedMemoryWeight;

public class FCM implements Estimator<FCM,FCMModel>,FCMParams<FCM> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public FCM() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public FCMModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> ((Vector) row.getField(getFeaturesCol())).toDense());

        //初始聚类中心
        DataStream<DenseVector[]> initCentroids = selectRandomCentroids(points, getK(), getSeed());

        //初始隶属矩阵 U(n*k) n:样本数 k:聚类数 Tuple2<样本，样本对应每个聚类的隶属度>
        DataStream<Tuple2<DenseVector, DenseVector>> pointsAndMembership = points.map(new MapFunction<DenseVector, Tuple2<DenseVector, DenseVector>>() {
            @Override
            public Tuple2<DenseVector, DenseVector> map(DenseVector value) throws Exception {
                //随机生成 样本 在每一个聚类 下的隶属度 总和为1
                DenseVector dv = calMembershipVector(getK());
                return new Tuple2<>(value, dv);
            }
        }, Types.TUPLE(TypeInformation.of(DenseVector.class), TypeInformation.of(DenseVector.class)));

        //迭代更新聚类中心
        //迭代配置
        IterationConfig config =
                IterationConfig.newBuilder()
                        .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                        .build();
        //迭代体
        IterationBody body =
                new FCM.FCMIterationBody(
                        getMaxIter(), DistanceMeasure.getInstance(getDistanceMeasure()),getM(),getTOL());

        DataStreamList finalCentroidsStreams = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(initCentroids),
                ReplayableDataStreamList.notReplay(pointsAndMembership),
                config,
                body);

        //获取最终聚类中心
        DataStream<DenseVector[]> centroidsStream = finalCentroidsStreams.get(0);

//        //查看聚类中心结果
//        centroidsStream.map(r->{
//            System.out.println(Arrays.toString(r));
//            return r;
//        }, TypeInformation.of(DenseVector[].class));

        //根据最终聚类中心，计算最终隶属矩阵
        final String broadcastCentroidsKey = "broadcastCentroidsKey";
        DataStream<Tuple2<DenseVector,DenseVector>> membershipVectorStream = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(points),
                Collections.singletonMap(broadcastCentroidsKey, centroidsStream),
                inputList -> {
                    DataStream input = inputList.get(0);
                    return input.map(
                            new CalMembershipMartix(broadcastCentroidsKey, DistanceMeasure.getInstance(getDistanceMeasure()), getM()),
                            Types.TUPLE(TypeInformation.of(DenseVector.class), TypeInformation.of(DenseVector.class)));
                });

        //利用该工具类的特点，会将广播流收集成列表
        final String broadcastMembershipKey = "broadcastMembershipKey";
        DataStream<FCMModelData> modelData = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(centroidsStream),
                Collections.singletonMap(broadcastMembershipKey, membershipVectorStream),
                inputList -> {
                    DataStream input = inputList.get(0);
                    return input.map(
                            new ConvertModelData(broadcastMembershipKey),
                            TypeInformation.of(FCMModelData.class));
                });

        Table finalModelDataTable = tEnv.fromDataStream(modelData);
        FCMModel model = new FCMModel().setModelData(finalModelDataTable);
        ParamUtils.updateExistingParams(model, paramMap);
        return model;
    }

    private static class ConvertModelData extends RichMapFunction<DenseVector[],FCMModelData>{
        private final String broadcastKey;
        private List<Tuple2<DenseVector,DenseVector>> membershipMartix;

        private ConvertModelData(String broadcastKey) {
            this.broadcastKey = broadcastKey;
        }

        @Override
        public FCMModelData map(DenseVector[] value) throws Exception {
            if (null==membershipMartix){
                membershipMartix = getRuntimeContext().getBroadcastVariable(broadcastKey);
            }
            return new FCMModelData(value, membershipMartix.toArray(new Tuple2[0]));
        }
    }

    private static class CalMembershipMartix extends RichMapFunction<DenseVector, Tuple2<DenseVector,DenseVector>>{
        private DenseVector[] centroids;
        private final String broadcastKey;
        private final DistanceMeasure distanceMeasure;//距离计算方案
        private final Double fuzzinessCoefficient;//模糊系数

        private CalMembershipMartix(String broadcastKey, DistanceMeasure distanceMeasure, Double fuzzinessCoefficient) {
            this.broadcastKey = broadcastKey;
            this.distanceMeasure = distanceMeasure;
            this.fuzzinessCoefficient = fuzzinessCoefficient;
        }


        @Override
        public Tuple2<DenseVector,DenseVector> map(DenseVector value) throws Exception {
            if (null==centroids){
                centroids = (DenseVector[])getRuntimeContext().getBroadcastVariable(broadcastKey).get(0);
            }
            DenseVector dv = updateMembershipVector(value, centroids, distanceMeasure, fuzzinessCoefficient);
            return Tuple2.of(value,dv);
        }
    }


    private static class FCMIterationBody  implements IterationBody{
        private final int maxIterationNum;//迭代次数
        private final DistanceMeasure distanceMeasure;//距离计算方案
        private final Double fuzzinessCoefficient;//模糊系数
        private final Double convergenceTolerance;//收敛阈值

        public FCMIterationBody(int maxIterationNum, DistanceMeasure distanceMeasure,Double fuzzinessCoefficient,Double convergenceTolerance) {
            this.maxIterationNum = maxIterationNum;
            this.distanceMeasure = distanceMeasure;
            this.fuzzinessCoefficient = fuzzinessCoefficient;
            this.convergenceTolerance = convergenceTolerance;
        }

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<DenseVector[]> centroids = variableStreams.get(0);
            DataStream<Tuple2<DenseVector,DenseVector>> pointsAndMembership = dataStreams.get(0);

            //每一个并行任务计算出一部分结果
            DataStream<Tuple3<Double[], DenseVector[],Double>> centroidIdAndPoints =
                    //根据并行度进行广播，每个task都会得到一份centroids，eg：并行度为3，总数据条数6 则每个并行任务均分到2条数据，并都获得相同的广播centroids
                    pointsAndMembership.connect(centroids.broadcast())
                            .transform(
                                    "CentroidsUpdateAccumulator",
                                    new TupleTypeInfo<>(
                                            ObjectArrayTypeInfo.getInfoFor(Types.DOUBLE),
                                            ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE),
                                            Types.DOUBLE),
                                    new FCM.CentroidsUpdateAccumulator(distanceMeasure,fuzzinessCoefficient));
//            centroidIdAndPoints.map(r->{
//                System.out.println(Arrays.toString(r.f0));
//                System.out.println(Arrays.toString(r.f1));
//                System.out.println(r.f2);
//                return r;
//            }, new TupleTypeInfo<>(
//                    ObjectArrayTypeInfo.getInfoFor(Types.DOUBLE),
//                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE),
//                    Types.DOUBLE));
            setManagedMemoryWeight(centroidIdAndPoints, 100);
            int parallelism = centroidIdAndPoints.getParallelism();

            //归约 计算 新旧 隶属矩阵 差值
            SingleOutputStreamOperator<Double> maxTolData = centroidIdAndPoints
                    .countWindowAll(parallelism)
                    .reduce(new ReduceFunction<Tuple3<Double[], DenseVector[], Double>>() {
                        @Override
                        public Tuple3<Double[], DenseVector[], Double> reduce(Tuple3<Double[], DenseVector[], Double> value1, Tuple3<Double[], DenseVector[], Double> value2) throws Exception {
                            if (value1.f2 > value2.f2) {
                                return value1;
                            } else {
                                return value2;
                            }
                        }
                    })
                    .flatMap(new FlatMapFunction<Tuple3<Double[], DenseVector[], Double>, Double>() {
                        @Override
                        public void flatMap(Tuple3<Double[], DenseVector[], Double> value, Collector<Double> out) throws Exception {
                            out.collect(value.f2);
                        }
                    });

//            归约 计算 新的聚类中心
            SingleOutputStreamOperator<DenseVector[]> newCentroids = centroidIdAndPoints
                    //使用 计数窗口 会使 所有数据 进入一个任务 即并行度为1
                    //当并行度为3时，会产生3条 并行处理 的数据 此时设置计数窗口为并行度，即将产生的三条数据进行归约处理，最终产生一条数据
                    .countWindowAll(parallelism)
                    .reduce(new CentroidsUpdateReducer())
                    .flatMap(new FlatMapFunction<Tuple3<Double[], DenseVector[],Double>, DenseVector[]>() {
                        @Override
                        public void flatMap(Tuple3<Double[], DenseVector[],Double> value, Collector<DenseVector[]> out) throws Exception {
                            DenseVector[] newCentroids = new DenseVector[value.f1.length];
                            for (int i = 0; i < value.f1.length; i++) {
                                newCentroids[i] = new DenseVector(value.f1[i].size());
                                for (int j = 0; j < value.f1[i].size(); j++) {
                                    newCentroids[i].set(j, value.f1[i].get(j) / value.f0[i]);
                                }
                            }
                            out.collect(newCentroids);
                        }
                    },ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE))
                    .setParallelism(1);
//            newCentroids.map(r->{
//                System.out.println("newCen = "+Arrays.toString(r));
//                return r;
//            },ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE));

            SingleOutputStreamOperator<Integer> terminationCriteriaUDF = pointsAndMembership
                    .connect(maxTolData.broadcast())
                    .transform(
                            "TerminationCriteriaUDF",
                            Types.INT,
                            new TerminationCriteriaUDF(convergenceTolerance, maxIterationNum));

            SingleOutputStreamOperator<DenseVector[]> finalCentroids =
                    newCentroids
                            .flatMap(new ForwardInputsOfLastRound<>());

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids),
                    DataStreamList.of(finalCentroids),
                    terminationCriteriaUDF);
        }
    }

    //自定义实现迭代停止条件
    public static class TerminationCriteriaUDF  extends AbstractStreamOperator<Integer>
            implements  TwoInputStreamOperator<
            Tuple2<DenseVector,DenseVector>, Double, Integer>
            ,IterationListener<Integer>{

        private final double tolerance;
        private final Integer maxIter;
        private ListState<Double> maxTol;

        public TerminationCriteriaUDF(double tolerance, Integer maxIter) {
            this.tolerance = tolerance;
            this.maxIter = maxIter;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            maxTol = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("maxTol", Types.DOUBLE));
        }

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<Integer> collector) throws Exception {
            //获得最大差值
            Double maxTolerance =
                    Objects.requireNonNull(
                            OperatorStateUtils.getUniqueElement(maxTol, "maxTol")
                                    .orElse(null));
            if (i < 1){
                collector.collect(0);
            }else{
                if (i+1<maxIter && maxTolerance > tolerance){
                    collector.collect(0);
                }
            }
            maxTol.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Integer> collector) throws Exception {

        }

        @Override
        public void processElement1(StreamRecord<Tuple2<DenseVector, DenseVector>> element) throws Exception {

        }

        @Override
        public void processElement2(StreamRecord<Double> element) throws Exception {
            Preconditions.checkState(!maxTol.get().iterator().hasNext());
            maxTol.add(element.getValue());
        }
    }

    //将 每一个并行任务 产生的结果 进行 合并
    private static class CentroidsUpdateReducer
            implements ReduceFunction<Tuple3<Double[], DenseVector[],Double>>{
        @Override
        /**
         *
         * Tuple3<Double[], DenseVector[],Double> f0:每一列的和 作分母 ,f1 样本的 每一个特征对 聚类列 产生的贡献 f2: 隶属矩阵差值
         */
        public Tuple3<Double[], DenseVector[],Double> reduce(Tuple3<Double[], DenseVector[],Double> t1, Tuple3<Double[], DenseVector[],Double> t2) throws Exception {
            for (int i = 0; i < t2.f0.length; i++) {
                t2.f0[i] += t1.f0[i];
                //y += a * x ,向量加法，将t1.f1[i] 加到 t2.f1[i] 上
                BLAS.axpy(1.0, t1.f1[i], t2.f1[i]);
            }
            return Tuple3.of(t2.f0,t2.f1,t2.f2);
        }
    }

    private static class CentroidsUpdateAccumulator
            extends AbstractStreamOperator<Tuple3<Double[],DenseVector[],Double>>
            implements TwoInputStreamOperator<
            Tuple2<DenseVector,DenseVector>, DenseVector[], Tuple3<Double[],DenseVector[],Double>>,
            IterationListener<Tuple3<Double[],DenseVector[],Double>>{

        private final Double m;
        private final DistanceMeasure distanceMeasure;
        private ListState<DenseVector[]> centroids;
        //java8仅支持申请一个ListStateWithCache 两个会报 分配内存不足
        private ListStateWithCache<Tuple2<DenseVector,DenseVector>> pointsAndMembership;
        private List<Tuple2<DenseVector,DenseVector>> tmp2;

        private CentroidsUpdateAccumulator(DistanceMeasure distanceMeasure,Double m) {
            this.distanceMeasure = distanceMeasure;
            this.m = m;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE);

            centroids =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("centroids", type));

            DenseVectorSerializer dvs = new DenseVectorSerializer();
            TypeSerializer<?>[] typeSerializers = {dvs,dvs};
            Tuple2<DenseVector,DenseVector> tuple2 = new Tuple2<>();
            pointsAndMembership =
                    new ListStateWithCache<Tuple2<DenseVector,DenseVector>>(
                            new TupleSerializer<Tuple2<DenseVector,DenseVector>>((Class<Tuple2<DenseVector, DenseVector>>) tuple2.getClass(), typeSerializers),
                            getContainingTask(),
                            getRuntimeContext(),
                            context,
                            config.getOperatorID());
//            用于 暂时存储 更新后的 隶属度 ，中间转储
            tmp2 = new ArrayList<>();
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            pointsAndMembership.snapshotState(context);
        }

        @Override
        public void onEpochWatermarkIncremented(
                int i,//当前迭代次数，每一轮迭代结束，i+1
                Context context,
                Collector<Tuple3<Double[], DenseVector[],Double>> collector) throws Exception {
//            System.out.println(i);
            //获得聚类中心
            DenseVector[] centroidValues =
                    Objects.requireNonNull(
                            OperatorStateUtils.getUniqueElement(centroids, "centroids")
                                    .orElse(null));
            //根据epoch 更新 该任务所属分区的 隶属矩阵 并 计算 新旧 隶属矩阵差值
            double tol = -1;
            if (i>0){//第一轮 结束 获得新的聚类中心后，更新隶属矩阵
                for (Tuple2<DenseVector, DenseVector> dt2 : pointsAndMembership.get()) {
                    //更新 隶属度
                    //计算数据点 距离 每一个聚类中心的 欧氏距离
                    DenseVector point = dt2.f0;
                    DenseVector newMembershipVector = updateMembershipVector(point, centroidValues, distanceMeasure, m);
                    //计算 新旧 隶属 矩阵 差值
                    DenseVector oldMemberVector = dt2.f1;
                    //计算差值
                    for (int j = 0; j < newMembershipVector.size(); j++) {
                        double tt = Math.abs(newMembershipVector.get(j) - oldMemberVector.get(j));
                        if (tt > tol) {
                            tol = tt;
                        }
                    }
                    tmp2.add(Tuple2.of(point,newMembershipVector));
                }
            }

            //计算新的聚类中心
            //记录 每一列的 m次幂 求和
            //分母
            Double[] centroidSum = new Double[centroidValues.length];
            Arrays.fill(centroidSum,0.0);
            //分子
            DenseVector[] pointContributions = new DenseVector[centroidValues.length];
            for (int k = 0; k < centroidValues.length; k++) {
                pointContributions[k] = new DenseVector(centroidValues[k].size());
            }

            //TODO:先更新聚类中心，根据新的聚类中心更新隶属矩阵
            //1.计算隶属度的m次幂
            if (i>0) {
                cal(centroidSum, pointContributions, tmp2);
            }else{
                cal(centroidSum, pointContributions, pointsAndMembership);
            }

            output.collect(new StreamRecord<>(new Tuple3<>(centroidSum, pointContributions, tol)));

            //更新缓存
            if (i>0){
                pointsAndMembership.clear();
                for (Tuple2<DenseVector, DenseVector> dt2 : tmp2) {
                    pointsAndMembership.add(dt2);
                }
                tmp2.clear();
            }
            centroids.clear();
        }

        private void cal(
                Double[] centroidSum,
                DenseVector[] pointContributions,
                ListStateWithCache<Tuple2<DenseVector, DenseVector>> listState) throws Exception {
            listState.get().forEach(dt2->{
                DenseVector membershipVector = dt2.f1;
                for (int k = 0; k < membershipVector.size(); k++) {
                    double tt = Math.pow(membershipVector.get(k), m);
                    centroidSum[k] += tt;
                    for (int j = 0; j < dt2.f0.size(); j++) {
                        pointContributions[k].set(j, pointContributions[k].get(j) + tt * dt2.f0.get(j));
                    }
                }
            });
        }
        private void cal(
                Double[] centroidSum,
                DenseVector[] pointContributions,
                List<Tuple2<DenseVector, DenseVector>> list) throws Exception {
            for (Tuple2<DenseVector, DenseVector> dt2 : list) {
                DenseVector membershipVector = dt2.f1;
                for (int k = 0; k < membershipVector.size(); k++) {
                    double tt = Math.pow(membershipVector.get(k), m);
                    centroidSum[k] += tt;
                    for (int j = 0; j < dt2.f0.size(); j++) {
                        pointContributions[k].set(j, pointContributions[k].get(j) + tt * dt2.f0.get(j));
                    }
                }
            }
        }


        @Override
        public void onIterationTerminated(
                Context context,
                Collector<Tuple3<Double[], DenseVector[],Double>> collector) throws Exception {

        }

        @Override
        public void processElement1(StreamRecord<Tuple2<DenseVector, DenseVector>> element) throws Exception {
            pointsAndMembership.add(element.getValue());
        }

        @Override
        public void processElement2(StreamRecord<DenseVector[]> element) throws Exception {
            Preconditions.checkState(!centroids.get().iterator().hasNext());
            centroids.add(element.getValue());
        }
    }

    public static DenseVector updateMembershipVector(DenseVector point,DenseVector[] centroidValues,DistanceMeasure distanceMeasure,double m) {
        //计算数据点 距离 每一个聚类中心的 欧氏距离
        //转为具有 范数的 向量
        VectorWithNorm vwn = new VectorWithNorm(point);
        //记录 距离
        Double[] dis = new Double[centroidValues.length];
        for (int k = 0; k < centroidValues.length; k++) {
            VectorWithNorm cent = new VectorWithNorm(centroidValues[k]);
            dis[k] = distanceMeasure.distance(vwn, cent);
            //避免0除
            if (0 == dis[k]) {
                dis[k] = 1e-10;
            }
        }
        //得到新的 样本 隶属度
        DenseVector newMembershipVector = new DenseVector(dis.length);
        for (int k = 0; k < centroidValues.length; k++) {
            double sum = 0.0;
            for (int j = 0; j < centroidValues.length; j++) {
                Double dd = dis[k] / dis[j];
                double tt = 2.0 / (m - 1);
                sum = sum + Math.pow(dd, tt);
            }
            newMembershipVector.set(k, 1.0 / sum);
        }
        return newMembershipVector;
    }

    private DenseVector calMembershipVector(int len){
        //使用狄克雷分布进行初始化
        // 创建一个随机数生成器
        UniformRandomProvider rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        // 创建狄克雷分布的采样器
        DirichletSampler sampler = DirichletSampler.symmetric(rng,len,1.0);
        double[] sample = sampler.sample();
        return new DenseVector(sample);
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static FCM load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

}

package cn.swust.algorithms.canopy;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.*;
import org.apache.flink.iteration.datacache.nonkeyed.ListStateWithCache;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.distance.DistanceMeasure;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;


import java.io.IOException;
import java.util.*;

import static org.apache.flink.ml.clustering.kmeans.KMeans.selectRandomCentroids;
import static org.apache.flink.ml.common.datastream.DataStreamUtils.setManagedMemoryWeight;

public class Canopy implements CanopyParams<Canopy>, AlgoOperator<Canopy> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public Canopy() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        //检查T1和T2的大小
        if(getT1()<=getT2()){
            throw new IllegalArgumentException("T1 must be greater than T2");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> ((Vector) row.getField(getFeaturesCol())).toDense());

        //随机选取一个样本作为初始中心点
        DataStream<DenseVector[]> initCentroids = selectRandomCentroids(points, 1, getSeed());

/*        initCentroids.map(r->{
            System.out.println(Arrays.toString(r));
            return r;
        });*/

        //构建迭代
        IterationConfig config =
                IterationConfig.newBuilder()
                        .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                        .build();

        IterationBody body =
                new CanopyIterationBody(
                        getSeed(), getT1(),getT2(), getMaxIter(), DistanceMeasure.getInstance(getDistanceMeasure()));

        DataStreamList resultDatas = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(initCentroids),
                ReplayableDataStreamList.notReplay(points),
                config,
                body);

        //构建输出列
        TypeInformation[] types = {TypeInformation.of(DenseVector.class), TypeInformation.of(DenseVector[].class)};
        String[] names = {"centroids", getPredictionCol()};
        RowTypeInfo outputTypeInfo = new RowTypeInfo(types,names);

        //全部的canopy聚类
        DataStream<DenseVector[]> finalResult = resultDatas.get(0);
        SingleOutputStreamOperator<Row> convertResult = finalResult.map(r -> {
            Row row = new Row(2);
            //聚类中心
            row.setField(0, r[0]);
            //canopy结果
            row.setField(1, r);
            return row;
        }, outputTypeInfo);

        return new Table[] {tEnv.fromDataStream(convertResult)};
        /*finalResult.map(r->{
            System.out.println("canopy = "+Arrays.toString(r));
            return r;
        });*/
    }

    private static class CanopyIterationBody implements IterationBody{
        private final Long seed;
        private final Double t1;
        private final Double t2;
        private final int maxIterationNum;
        private final DistanceMeasure distanceMeasure;

        private CanopyIterationBody(Long seed, Double t1, Double t2, int maxIterationNum, DistanceMeasure distanceMeasure) {
            this.seed = seed;
            this.t1 = t1;
            this.t2 = t2;
            this.maxIterationNum = maxIterationNum;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<DenseVector[]> centroids = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

/*            centroids.map(r->{
                System.out.println("ss "+Arrays.toString(r));
                return r;
            },TypeInformation.of(DenseVector[].class));*/

            // Tuple2： f0:弱聚类数组（canopy结果）  f1：每一个分区中选取的下一轮聚类中心
            DataStream<Tuple2<DenseVector[], DenseVector[]>> centroidIdAndPoints =
                    //根据并行度进行广播，每个task都会得到一份centroids，eg：并行度为3，总数据条数6 则每个并行任务均分到2条数据，并都获得相同的广播centroids
                    points.connect(centroids.broadcast())
                            .transform(
                                    "CentroidsUpdateAccumulator",
                                    new TupleTypeInfo<>(
                                            ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE),
                                            ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE)),
                                    new CentroidsUpdateAccumulator(seed, t1, t2, distanceMeasure));

            /*centroidIdAndPoints.map(r->{
                System.out.println("centriods = "+r.f0[0]);
                System.out.println("f0 = "+Arrays.toString(r.f0));
                System.out.println("f1 = "+Arrays.toString(r.f1));
                return r;
            }, Types.TUPLE(TypeInformation.of(DenseVector[].class),TypeInformation.of(DenseVector[].class)));*/

            setManagedMemoryWeight(centroidIdAndPoints, 100);
            int parallelism = centroidIdAndPoints.getParallelism();
//            System.out.println(parallelism);
            //将每一个task的聚类结果进行归约处理，最终产生一条数据
            SingleOutputStreamOperator<Tuple2<DenseVector[], DenseVector[]>> reduceData = centroidIdAndPoints
                    //当并行度为3时，会产生3条 并行处理 的数据 此时设置计数窗口为并行度，即将产生的三条数据进行归约处理，最终产生一条数据
                    .countWindowAll(parallelism)
                    .reduce(new ReduceFunction<Tuple2<DenseVector[], DenseVector[]>>() {
                        @Override
                        public Tuple2<DenseVector[], DenseVector[]> reduce(Tuple2<DenseVector[], DenseVector[]> value1, Tuple2<DenseVector[], DenseVector[]> value2) throws Exception {
                            //合并聚类结果
                            ArrayList<Integer> hashcode = new ArrayList<>();
                            ArrayList<DenseVector> dvs = new ArrayList<>();
                            for (DenseVector dv : value1.f0) {
                                if(!hashcode.contains(dv.hashCode())){
                                    hashcode.add(dv.hashCode());
                                    dvs.add(dv);
                                }
                            }
                            for (DenseVector dv : value2.f0) {
                                if(!hashcode.contains(dv.hashCode())){
                                    hashcode.add(dv.hashCode());
                                    dvs.add(dv);
                                }
                            }
                            DenseVector[] arr1 = dvs.toArray(new DenseVector[0]);
                            hashcode.clear();
                            dvs.clear();
                            //保留一条下一轮的聚类中心
                            //判断是否到达迭代终点
                            DenseVector endPoint = new DenseVector(1);
                            endPoint.set(0, -1);
                            if (value1.f1[0].hashCode() != endPoint.hashCode()){
                                if (value2.f1[0].hashCode() != endPoint.hashCode()){
                                    return new Tuple2<>(arr1, value2.f1);
                                }else{
                                    return new Tuple2<>(arr1, value1.f1);
                                }
                            }else{
                                if (value2.f1[0].hashCode() != endPoint.hashCode()){
                                    return new Tuple2<>(arr1, value2.f1);
                                }else{
                                    return new Tuple2<>(arr1, new DenseVector[]{endPoint});
                                }
                            }
                            /*DenseVector[] del1 = value1.f1;
                            DenseVector[] del2 = value2.f1;
                            HashSet<DenseVector> set2 = new HashSet<>();
                            set2.addAll(Arrays.asList(del1));
                            set2.addAll(Arrays.asList(del2));
                            DenseVector[] arr2 = new ArrayList<DenseVector>(set2).toArray(new DenseVector[0]);*/

                        }
                    });

           /* reduceData.map(r->{
                System.out.println("centriods = "+r.f0[0]);
                System.out.println("point = "+Arrays.toString(r.f0));
                System.out.println("nextCe = "+Arrays.toString(r.f1));
                System.out.println();
                return r;
            },Types.TUPLE(TypeInformation.of(DenseVector[].class),TypeInformation.of(DenseVector[].class)));*/

            //分别过滤出聚类结果 和 下一轮的聚类的中心
            //过滤出每一轮产生的canopy结果
            SingleOutputStreamOperator<DenseVector[]> perResults = reduceData.map(r -> r.f0, TypeInformation.of(DenseVector[].class));
            /*perResults.map(r->{
                System.out.println("perResult = "+Arrays.toString(r));
                return r;
            },TypeInformation.of(DenseVector[].class));
*/
            /*//删除待删样本
            SingleOutputStreamOperator<DenseVector[]> deleteData = reduceData.map(r -> r.f1, TypeInformation.of(DenseVector[].class));

            //将待删样本广播出去,执行删除
            SingleOutputStreamOperator<DenseVector> newPoints = points
                    .connect(deleteData.broadcast())
                    .transform(
                            "DeletePoints",
                            TypeInformation.of(DenseVector.class),
                            new DeletePoints());*/
            //过滤出下一轮的聚类中心
            SingleOutputStreamOperator<DenseVector[]> nextCentroids =
                    reduceData
                            .map(r -> r.f1, TypeInformation.of(DenseVector[].class))
                            .setParallelism(1);

            //构造结束条件
            DenseVector endPoint = new DenseVector(1);
            endPoint.set(0, -1);
            SingleOutputStreamOperator<Integer> terminationCriteriaUDF = points
                    .connect(nextCentroids.broadcast())
                    .transform(
                            "TerminationCriteriaUDF",
                            Types.INT,
                            new TerminationCriteriaUDF(endPoint, maxIterationNum));

            return new IterationBodyResult(
                    DataStreamList.of(nextCentroids),
                    DataStreamList.of(perResults),
                    terminationCriteriaUDF);
        }
    }

    public static class TerminationCriteriaUDF  extends AbstractStreamOperator<Integer>
            implements  TwoInputStreamOperator<
            DenseVector,DenseVector[], Integer>
            ,IterationListener<Integer>{
        private final DenseVector endPoint;
        private final Integer maxIter;
        private ListState<DenseVector> nextCentriods;

        public TerminationCriteriaUDF(DenseVector endPoint, Integer maxIter) {
            this.endPoint = endPoint;
            this.maxIter = maxIter;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            nextCentriods =
                    context
                            .getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("nextCentriods",TypeInformation.of(DenseVector.class)));
        }

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<Integer> collector) throws Exception {
            //获得新的聚类中心
            DenseVector cen =
                    Objects.requireNonNull(
                            OperatorStateUtils.getUniqueElement(nextCentriods, "nextCentriods")
                                    .orElse(null));
            //未到达迭代终点
            System.out.println("i "+i);
            if (i+1<maxIter && endPoint.hashCode() != cen.hashCode()){
                collector.collect(0);
            }
            nextCentriods.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Integer> collector) throws Exception {

        }

        @Override
        public void processElement1(StreamRecord<DenseVector> element) throws Exception {

        }

        @Override
        public void processElement2(StreamRecord<DenseVector[]> element) throws Exception {
            Preconditions.checkState(!nextCentriods.get().iterator().hasNext());
            nextCentriods.add(element.getValue()[0]);
        }
    }

    private static class CentroidsUpdateAccumulator
            extends AbstractStreamOperator<Tuple2<DenseVector[], DenseVector[]>>
            implements TwoInputStreamOperator<
            DenseVector, DenseVector[], Tuple2<DenseVector[], DenseVector[]>>,
            IterationListener<Tuple2<DenseVector[], DenseVector[]>> {

        private final Long seed;
        private final Double t1;
        private final Double t2;
        private final DistanceMeasure distanceMeasure;
        //每一轮的聚类中心
        private ListState<DenseVector[]> centroids;
        //存放全部样本
        private ListStateWithCache<DenseVector> points;
        //中间转储
        private List<DenseVector> tmp;

        public CentroidsUpdateAccumulator(Long seed, Double t1, Double t2, DistanceMeasure distanceMeasure) {
            super();
            this.seed = seed;
            this.t1 = t1;
            this.t2 = t2;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE);

            centroids =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("centroids", type));

            points =
                    new ListStateWithCache<DenseVector>(
                            new DenseVectorSerializer(),
                            getContainingTask(),
                            getRuntimeContext(),
                            context,
                            config.getOperatorID());
            tmp = new ArrayList<>();
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            points.snapshotState(context);
        }

        //具体逻辑
        @Override
        public void onEpochWatermarkIncremented(
                int i,
                Context context,
                Collector<Tuple2<DenseVector[], DenseVector[]>> collector) throws Exception {
            //获取聚类中心
            DenseVector[] centroidValues =
                    Objects.requireNonNull(
                            OperatorStateUtils.getUniqueElement(centroids, "centroids")
                                    .orElse(null));

            //遍历样本 计算欧式距离
            //加入到聚类中心
            List<DenseVector> belongs = new ArrayList<>();
            List<DenseVector> deleteList = new ArrayList<>();
            for (DenseVector dv : points.get()) {
                VectorWithNorm vwn = new VectorWithNorm(dv);
                //聚类中心只有1个
                VectorWithNorm cent = new VectorWithNorm(centroidValues[0]);
                //计算dis
                double dis = distanceMeasure.distance(cent, vwn);
//                System.out.println("vwm = "+dv+" cent ="+ centroidValues[0]+" =dis "+ dis);
                //跳过聚类中心本身
                //误差< 1e-6认为是相同
                if (dis <= 1e-6)continue;
                /**
                 * 如果某个点到中心点的距离小于T1，则将该点划入当前聚类中心
                 * 如果某个点到中心点的距离小于T2，则将该点从数据集中删除（即从后续的聚类过程中移除）。
                 * T1>T2
                 */
                if (dis < t1){
                    belongs.add(dv);
                    if (dis >= t2){
                        //继续参与下一轮的聚类中心的选取
                        tmp.add(dv);
                    }else{
                        //待删项
                        deleteList.add(dv);
                    }
                }else{
                    //继续参与下一轮的聚类中心的选取
                    tmp.add(dv);
                }
            }

            //合并 canopy结果
            List<DenseVector> results = new ArrayList<>();
            results.add(centroidValues[0]);
//            System.out.println("tmp = "+tmp.size());
//            deleteList.add(centroidValues[0]);
            deleteList.clear();
            for (int j = 1; j <= belongs.size(); j++) {
                results.add(belongs.get(j-1));
            }

            //以每一个分区样本中的第一个点作为下一轮的聚类中心
            DenseVector[] nextCentroids = new DenseVector[1];
            if (tmp.size()>0){
                nextCentroids[0] = tmp.get(0);
            }else{
                //没有就设为终止条件
                DenseVector terminalVector = new DenseVector(1);
                terminalVector.set(0, -1);
                nextCentroids[0] = terminalVector;
            }
            //发送到下游
            output.collect(
                    new StreamRecord<>(
                            Tuple2.of(
                                    results.toArray(new DenseVector[0]),
                                    nextCentroids)));

            centroids.clear();
            //更新参与下一轮聚类 每一个 分区的 样本
            points.clear();
            points.addAll(tmp);
            tmp.clear();
        }

        @Override
        public void onIterationTerminated(
                Context context,
                Collector<Tuple2<DenseVector[], DenseVector[]>> collector) throws Exception {
            centroids.clear();
            points.clear();
            tmp.clear();
        }

        @Override
        public void processElement1(StreamRecord<DenseVector> streamRecord) throws Exception {
            points.add(streamRecord.getValue());
        }

        @Override
        public void processElement2(StreamRecord<DenseVector[]> streamRecord) throws Exception {
            Preconditions.checkState(!centroids.get().iterator().hasNext());
            centroids.add(streamRecord.getValue());
        }
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static Canopy load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

}


package cn.swust.algorithms.ahp;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;


public class AHP implements AlgoOperator<AHP>,AHPParams<AHP> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public AHP() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }
    //定义平均一致性指标
    private static final double RI[] = {0, 0, 0.58, 0.89, 1.12, 1.26, 1.36, 1.41, 1.46, 1.49, 1.52, 1.54, 1.56, 1.58, 1.59};

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        //检查判断矩阵是否合法
        checkJugleMartix(getJudgmentMatrix(),getInputCols());
        //计算判断矩阵的权重向量
        DenseVector weightVector = calWeightVector(getJudgmentMatrix(), getInputCols().length);
        //进行一致性检验
        checkConsistency(weightVector,getJudgmentMatrix());
        //指标类型非空
        Preconditions.checkNotNull(getIndicatorType(), "indicatorType must be set");
        //检查指标类型是否合法
        checkIndicatorType(getIndicatorType());

        RowTypeInfo inputRowTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());

        DataStream<Row> rowDataStream = tEnv.toDataStream(inputs[0]);
        //由于没有主观给出判断矩阵，此处采用熵值法计算每一行数据对于该列的权重

        //1.计算每一列的最大值和最小值
        SingleOutputStreamOperator<List<Tuple3<Integer, Double, Double>>> dealFirstData = rowDataStream.transform(
                "CalPerColMaxValueAndMinValue",
                Types.LIST(Types.TUPLE(Types.INT, Types.DOUBLE, Types.DOUBLE)),
                new CalPerColMaxValueAndMinValue(getIndicatorType()));

        //1.1 合并结果
        //进行结果合并
        int parallelism = dealFirstData.getParallelism();
        DataStream<List<Tuple3<Integer, Double, Double>>> dealFirstDataFinal =
                dealFirstData
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
//        dealFirstDataFinal.print();
        //1.2 归一化
        final String bcPerColMaxAndMin= "bcPerColMaxAndMin";
        DataStream<Tuple2<Row,Row>> dealData2 = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(rowDataStream),
                Collections.singletonMap(bcPerColMaxAndMin, dealFirstDataFinal),
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new Standardization(bcPerColMaxAndMin, getIndicatorType()),
                            Types.TUPLE(inputRowTypeInfo,inputRowTypeInfo));
                });
//        dealData2.print();
        //2.计算 Pij
        //2.1 计算每一列的总和
        SingleOutputStreamOperator<Row> dealDataTmp = dealData2.map(r -> r.f1,inputRowTypeInfo);
        DataStream<Row> perColSumData =
                DataStreamUtils.reduce(dealDataTmp, new CalPerColSum(), inputRowTypeInfo);
//        perColSumData.print();
        //2.2 计算Pij
        final String perColSum= "perColSum";
        DataStream<Row> dealDataPweight = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(dealDataTmp),
                Collections.singletonMap(perColSum, perColSumData),
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new CalWeight(perColSum),
                            inputRowTypeInfo);
                });
//        dealDataPweight.print();
        //3. 计算每一列的熵值
        //3.1 计算每一列的Pij和
        SingleOutputStreamOperator<Tuple2<Row,Integer>> perPweightData =
                dealDataPweight
                        .transform(
                                "CalPerPweight",
                                Types.TUPLE(
                                        inputRowTypeInfo,
                                        Types.INT),
                                new CalPerPweight());
//        perPweightData.print();
        //3.2 合并数据
        parallelism = perPweightData.getParallelism();
        DataStream<Tuple2<Row,Integer>> perPweightSum =
                perPweightData
                        .countWindowAll(parallelism)
                        .reduce(
                                (ReduceFunction<Tuple2<Row,Integer>>) (value1, value2) -> {
                                    Row r = new Row(value1.f0.getArity());
                                    for (int i = 0; i < value1.f0.getArity(); i++) {
                                        r.setField(i, (Double)value1.f0.getField(i) + (Double) value2.f0.getField(i));
                                    }
                                    return Tuple2.of(r, value1.f1 + value2.f1);
                                });
//        perPweightSum.print();
        //3.3 计算熵值冗余度
        SingleOutputStreamOperator<Row> entropyRedundancyData = perPweightSum.map(r -> {
            Double k = 1.0 / Math.log(r.f1);
            Row row = new Row(r.f0.getArity());
            for (int i = 0; i < r.f0.getArity(); i++) {
                row.setField(i, 1-(-k * (Double) r.f0.getField(i)));
            }
            return row;
        }, inputRowTypeInfo);
//        entropyRedundancyData.print();
        //4. 计算权重
        SingleOutputStreamOperator<Row> perColWeight = entropyRedundancyData.map(r -> {
            Row row = new Row(r.getArity());
            double sum = 0;
            for (int i = 0; i < r.getArity(); i++) {
                sum += (Double) r.getField(i);
            }
            for (int i = 0; i < r.getArity(); i++) {
                row.setField(i, (Double) r.getField(i) / sum);
            }
            return row;
        },inputRowTypeInfo);
//        perColWeight.print();
        //5. 计算每一条数据对于每一列应占比重
        final String bcPerColWeight= "bcPerColWeight";
        DataStream<Tuple2<Row,Row>> delaData3 = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(dealData2),
                Collections.singletonMap(bcPerColWeight, perColWeight),
                dataStreams -> {
                    DataStream input = dataStreams.get(0);
                    return input.map(
                            new CalScore(bcPerColWeight),
                            Types.TUPLE(inputRowTypeInfo,inputRowTypeInfo));
                });
//        delaData3.print();
        //6. 计算每一条数据的得分
        SingleOutputStreamOperator<Tuple2<Row, Double>> finalData = delaData3.map(r -> {
            Double score = 0.0;
            for (int i = 0; i < weightVector.size(); i++) {
                score += (Double) r.f1.getField(i) * weightVector.get(i);
            }
            return Tuple2.of(r.f0, score);
        }, Types.TUPLE(inputRowTypeInfo, Types.DOUBLE));
//        finalData.print();
        return new Table[]{convert2Table(finalData,tEnv,inputRowTypeInfo)};
    }

    private Table convert2Table(
            DataStream<Tuple2<Row, Double>> inputDataStream,
            StreamTableEnvironment tEnv,
            RowTypeInfo inputRowTypeInfo){
        //1.构建输出列
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputRowTypeInfo.getFieldTypes(), Types.DOUBLE),
                        ArrayUtils.addAll(inputRowTypeInfo.getFieldNames(), getOutputCol()));
        //2.构建输出行
        SingleOutputStreamOperator<Row> dataStream = inputDataStream.map(r -> {
            Row row = new Row(outputTypeInfo.getArity());
            for (int i = 0; i < r.f0.getArity(); i++) {
                row.setField(i, r.f0.getField(i));
            }
            row.setField(row.getArity() - 1, r.f1);
            return row;
        }, outputTypeInfo);
        return tEnv.fromDataStream(dataStream);
    }

    private static class CalScore extends RichMapFunction<Tuple2<Row,Row>, Tuple2<Row,Row>>{
        private final String bcPerColWeight;
        private Row weight;

        private CalScore(String bcPerColWeight) {
            this.bcPerColWeight = bcPerColWeight;
        }

        @Override
        public Tuple2<Row,Row> map(Tuple2<Row,Row> value) throws Exception {
            if (weight == null){
                weight = (Row) getRuntimeContext().getBroadcastVariable(bcPerColWeight).get(0);
            }
            Row row = new Row(weight.getArity());
            for (int i = 0; i < weight.getArity(); i++) {
                row.setField(i, (Double) value.f1.getField(i) * (Double) weight.getField(i));
            }
            return Tuple2.of(value.f0,row);
        }
    }


    /**
     * 计算每一列的总和，并统计数据条数
     */
    private static class CalPerPweight
            extends AbstractStreamOperator<Tuple2<Row,Integer>>
            implements OneInputStreamOperator<Row, Tuple2<Row,Integer>>,
            BoundedOneInput{
        private Integer count = 0;
        private List<Double> perPweight = new ArrayList<>();
        private ListState<List<Double>> perPweightState;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            perPweightState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "perPweightState",
                                            Types.LIST(Types.DOUBLE)));

            OperatorStateUtils.getUniqueElement(perPweightState, "perPweightState")
                    .ifPresent(x -> perPweight = x);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            perPweightState.update(Collections.singletonList(perPweight));
        }

        @Override
        public void endInput() throws Exception {
            Row row = new Row(perPweight.size());
            for (int i = 0; i < perPweight.size(); i++) {
                row.setField(i, perPweight.get(i));
            }
            output.collect(new StreamRecord<>(Tuple2.of(row,count)));
            perPweightState.clear();
        }

        @Override
        public void processElement(StreamRecord<Row> element) throws Exception {
            count++;
            Row value = element.getValue();
            if (perPweight.size() < value.getArity()){
                //第一次
                for (int i = 0; i < value.getArity(); i++) {
                    Double t = (Double) value.getField(i);
                    if (t == 0){
                        perPweight.add(0.0);
                    }else{
                        perPweight.add(t * Math.log(t));
                    }
                }
            }else{
                for (int i = 0; i < value.getArity(); i++) {
                    Double t = (Double) value.getField(i);
                    if (t != 0.0){
                        perPweight.set(i, perPweight.get(i) + t * Math.log(t));
                    }
                }
            }

        }


    }


    private static class CalWeight extends RichMapFunction<Row, Row>{
        private final String perColSum;
        private Row perColSumRow;

        private CalWeight(String perColSum) {
            this.perColSum = perColSum;
        }

        @Override
        public Row map(Row value) throws Exception {
            if (perColSumRow == null){
                perColSumRow = (Row)getRuntimeContext().getBroadcastVariable(perColSum).get(0);
            }
            Row r = new Row(value.getArity());
            for (int i = 0; i < value.getArity(); i++) {
                r.setField(i, (Double) value.getField(i) / (Double) perColSumRow.getField(i));
            }
            return r;
        }
    }

    private static class CalPerColSum implements ReduceFunction<Row> {
        @Override
        public Row reduce(Row value1, Row value2) throws Exception {
            Row r = new Row(value1.getArity());
            for (int i = 0; i < value1.getArity(); i++) {
                r.setField(i, (Double)value1.getField(i) + (Double) value2.getField(i));
            }
            return r;
        }
    }

    // tuple2: f0:原始数据 f1:标准化数据
    private static class Standardization extends RichMapFunction<Row,Tuple2<Row,Row>>{
        private final String bcPerColMaxAndMin;
        private final Integer[] indicatorTypes;
        private Map<Integer, Tuple2<Double,Double>> map;

        private Standardization(String bcPerColMaxAndMin, Integer[] indicatorTypes) {
            this.bcPerColMaxAndMin = bcPerColMaxAndMin;
            this.indicatorTypes = indicatorTypes;
        }

        @Override
        public Tuple2<Row,Row> map(Row value) throws Exception {
            if (map == null){
                List<Tuple3<Integer, Double, Double>> perColMaxAndMin =
                        (List<Tuple3<Integer, Double, Double>>)getRuntimeContext().getBroadcastVariable(bcPerColMaxAndMin).get(0);
                map = new HashMap<>();
                for (Tuple3<Integer, Double, Double> tp3 : perColMaxAndMin) {
                    map.put(tp3.f0,new Tuple2<>(tp3.f1,tp3.f2));
                }
            }
            Row r = new Row(value.getArity());
            for (int i = 0; i < indicatorTypes.length; i++) {
                double t = (Double) value.getField(i);
                double maxValue = map.get(i).f1;
                double minValue = map.get(i).f0;
                if (indicatorTypes[i] == 1) {
                    r.setField(i, (t - minValue) / (maxValue - minValue));
                }else{
                    r.setField(i, (maxValue - t) / (maxValue - minValue));
                }
            }
            return Tuple2.of(value,r);
        }
    }

    /**
     * 计算每一列的最大值和最小值
     * tuple3: f0:index f1:minValue f2:maxValue
     */
    private static class CalPerColMaxValueAndMinValue
            extends AbstractStreamOperator<List<Tuple3<Integer, Double, Double>>>
            implements OneInputStreamOperator<Row, List<Tuple3<Integer, Double, Double>>>,
            BoundedOneInput {
        private Map<Integer,Tuple3<Integer, Double, Double>> cntMap = new HashMap<>();
        private ListState<Map<Integer,Tuple3<Integer, Double, Double>>> cntMapState;

        private final Integer[] indicatorTypes;

        private CalPerColMaxValueAndMinValue(Integer[] indicatorTypes) {
            this.indicatorTypes = indicatorTypes;
        }

        @Override
        public void endInput() throws Exception {
            output.collect(new StreamRecord<>(new ArrayList<>(cntMap.values())));
            cntMapState.clear();
        }

        @Override
        public void processElement(StreamRecord<Row> element) throws Exception {
            Row value = element.getValue();
            for (int i = 0; i < indicatorTypes.length; i++) {
                if (cntMap.containsKey(i)) {
                    //已经存在,取出值
                    Tuple3<Integer, Double, Double> tuple3 = cntMap.get(i);
                    //更新值
                    tuple3.f2 = Math.max(tuple3.f2, (Double) value.getField(i));
                    tuple3.f1 = Math.min(tuple3.f1, (Double) value.getField(i));
                    cntMap.put(i,tuple3);
                }else{
                    //不存在
                    cntMap.put(i,new Tuple3<>(i,(Double) value.getField(i),(Double) value.getField(i)));
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


    private void checkIndicatorType(Integer[] indicatorType) {
        Preconditions.checkArgument(
            indicatorType.length == getInputCols().length,
            "indicatorType length must be equal to inputCols length");
        for (Integer t : indicatorType) {
            Preconditions.checkArgument(
                    t == 0 || t == 1,
                    "indicatorType must be 0 or 1");
        }

    }

    private void checkConsistency(DenseVector weightVector, Double[][] judgmentMatrix) {
        //计算A*W矩阵
        int m = weightVector.size();
        Double[][] aw = new Double[m][m];
        double total = 0;
        for (int i = 0; i < m; i++) {
            double sum = 0;
            for (int j = 0; j < m; j++) {
                aw[i][j] = judgmentMatrix[i][j] * weightVector.get(j);
                sum += aw[i][j];
            }
            total += sum / weightVector.get(i);
        }
        //计算最大特征值
        double maxEigenValue = total / m;
        //计算CI
        double ci = (maxEigenValue - m) / (m - 1);
        //计算CR
        double cr = ci / RI[m];
        Preconditions.checkArgument(
                cr < 0.1,
                "CR is too large, please check your judgment matrix");
    }

    //判断矩阵 矩阵维度
    private DenseVector calWeightVector(Double[][] judgmentMatrix,int m) {
        //几何平均法求权重
        DenseVector dv1 = applyGeometricAverage(judgmentMatrix, m);
        //算术平均法
        DenseVector dv2 = applyArithmeticAverage(judgmentMatrix, m);
        //计算最终权重
        DenseVector dv = new DenseVector(m);
        for (int i = 0; i < m; i++) {
            dv.set(i, (dv1.get(i) + dv2.get(i)) / 2);
        }
        return dv;
    }

    private DenseVector applyArithmeticAverage(Double[][] judgmentMatrix, int m) {
        DenseVector dv = new DenseVector(m);
        Double [] tmp = new Double[m];
        double total = 0;
        for (int i = 0; i < m; i++) {
            double sum = 1;
            for (int j = 0; j < m; j++) {
                sum += judgmentMatrix[i][j];
            }
            tmp[i]= sum / m;
            total += tmp[i];
        }
        //归一化
        for (int i = 0; i < m; i++) {
            dv.set(i, tmp[i] / total);
        }
        return dv;
    }

    private DenseVector applyGeometricAverage(Double[][] judgmentMatrix, int m) {
        DenseVector dv = new DenseVector(m);
        Double [] tmp = new Double[m];
        double total = 0;
        for (int i = 0; i < m; i++) {
            double sum = 1;
            for (int j = 0; j < m; j++) {
                sum *= judgmentMatrix[i][j];
            }
            tmp[i]=Math.pow(sum, 1.0 / m);
            total += tmp[i];
        }
        //归一化
        for (int i = 0; i < m; i++) {
            dv.set(i, tmp[i] / total);
        }
        return dv;
    }

    private void checkJugleMartix(Double[][] judgmentMatrix, String[] inputCols) {
        Preconditions.checkArgument(
                inputCols.length <= RI.length,
                "The number of input columns must be less than or equal to the length of RI (15)");
        Preconditions.checkNotNull(judgmentMatrix,"The judgment matrix cannot be empty");
        Preconditions.checkArgument(
                judgmentMatrix[0].length == inputCols.length,
                "The columns of the judgment matrix must be of the same length as the input columns");
    }


    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static AHP load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
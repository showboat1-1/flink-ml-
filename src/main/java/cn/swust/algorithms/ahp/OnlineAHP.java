package cn.swust.algorithms.ahp;

import cn.swust.algorithms.topsis.OnlineTopsis;
import cn.swust.algorithms.topsis.TopsisModelData;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.window.Windows;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnlineAHP implements OnlineAHPParams<OnlineAHP>, AlgoOperator<OnlineAHP> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    public OnlineAHP() {
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
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputRowTypeInfo.getFieldTypes(), Types.DOUBLE),
                        ArrayUtils.addAll(inputRowTypeInfo.getFieldNames(), getOutputCol()));

        //获取窗口处理策略
        Windows windows = getWindows();
        //利用窗口 计算每一个窗口时间内的 每一条数据的得分 即 计算一段时间内 每一条数据的得分
        //进行窗口聚合，计算均值
        SingleOutputStreamOperator<Row> finalData = DataStreamUtils.windowAllAndProcess(
                tEnv.toDataStream(inputs[0]),
                windows,
                new CalScoreWithPeriod<>(weightVector,getIndicatorType(), getInputCols()),
                outputTypeInfo);
//        finalData.print();

        return new Table[]{tEnv.fromDataStream(finalData)};
    }

    /**
     * 因为并没有给出 每一条数据间的判断矩阵
     * 采用 熵值法计算每一条数据 相对于于其他数据的权重
     * @param <W>
     */
    private static class CalScoreWithPeriod<W extends Window>
            extends ProcessAllWindowFunction<Row, Row, W> {
        private DenseVector weightVector;
        private Integer[] indicatorType;
        private String[] inputCols;

        public CalScoreWithPeriod(DenseVector weightVector, Integer[] indicatorType, String[] inputCols) {
            this.weightVector = weightVector;
            this.indicatorType = indicatorType;
            this.inputCols = inputCols;
        }

        @Override
        public void process(
                ProcessAllWindowFunction<Row, Row, W>.Context context,
                Iterable<Row> elements,
                Collector<Row> out) throws Exception {
            List<Row> lt = new ArrayList<>();
            //记录当前窗口中的数据
            List<DenseVector> data = new ArrayList<>();
            //记录每一列的最大最小值 f0:max f1:min
            List<Tuple2<Double,Double>> maxAndMin = new ArrayList<>();
            for (Row element : elements) {
                DenseVector dv = new DenseVector(inputCols.length);
                for (int i = 0; i < inputCols.length; i++) {
                    dv.set(i, (Double) element.getField(i+1));
                    if(maxAndMin.size() < inputCols.length){
                        maxAndMin.add(new Tuple2<>((Double) element.getField(i+1), (Double) element.getField(i+1)));
                    }else{
                        Tuple2<Double, Double> tp = maxAndMin.get(i);
                        Double t = (Double) element.getField(i + 1);
                        maxAndMin.set(i,new Tuple2<>(Math.max(tp.f0, t), Math.min(tp.f1, t)));
                    }
                }
                data.add(dv);
                lt.add(element);
            }
            //1.归一化
            List<DenseVector> normalizationList = new ArrayList<>();
            List<Double> perColSum = new ArrayList<>();
            for (DenseVector dv : data) {
                DenseVector t = new DenseVector(dv.size());
                for (int i = 0; i < inputCols.length; i++) {
                    if (indicatorType[i] == 1) {
                        t.set(i, (dv.get(i) - maxAndMin.get(i).f1) / (maxAndMin.get(i).f0 - maxAndMin.get(i).f1));
                    }else{
                        t.set(i, (maxAndMin.get(i).f0 - dv.get(i)) / (maxAndMin.get(i).f0 - maxAndMin.get(i).f1));
                    }
                    if(perColSum.size() < inputCols.length){
                        perColSum.add(t.get(i));
                    }else{
                        perColSum.set(i, perColSum.get(i) + t.get(i));
                    }
                }
                normalizationList.add(t);
            }
            //2.计算ej
            DenseVector eVector = new DenseVector(inputCols.length);
            for (DenseVector dv : normalizationList) {
                DenseVector t = new DenseVector(dv.size());
                for (int i = 0; i < inputCols.length; i++) {
                    double p = dv.get(i) / perColSum.get(i);
                    t.set(i, p == 0.0 ? 0.0 : p * Math.log(p));
                }
                //y += a * x .
                BLAS.axpy(1,t, eVector);
            }
            //3.计算dj
            DenseVector dVector = new DenseVector(inputCols.length);
            Double k = 1.0 / (Math.log(inputCols.length));
            Double total = 0.0;
            for (int i = 0; i < eVector.size(); i++) {
                dVector.set(i,1 - (-k*eVector.get(i)));
                total += dVector.get(i);
            }
            //4. 计算weight
            DenseVector weight = new DenseVector(inputCols.length);
            for (int i = 0; i < dVector.size(); i++) {
                weight.set(i, dVector.get(i) / total);
            }
            //5. 计算每一条数据最终得分
            for (int i = 0; i < normalizationList.size(); i++) {
                Double score = 0.0;
                for (int j = 0; j < inputCols.length; j++) {
                    score += normalizationList.get(i).get(j) * weightVector.get(j) * weight.get(j);
                }
                //构建输出列
                Row row = rowAppend(lt.get(i), score);
                out.collect(row);
            }
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

    private void checkJugleMartix(Double[][] judgmentMatrix, String[] inputCols) {
        Preconditions.checkArgument(
                inputCols.length <= RI.length,
                "The number of input columns must be less than or equal to the length of RI (15)");
        Preconditions.checkNotNull(judgmentMatrix,"The judgment matrix cannot be empty");
        Preconditions.checkArgument(
                judgmentMatrix[0].length == inputCols.length,
                "The columns of the judgment matrix must be of the same length as the input columns");
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

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineAHP load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}

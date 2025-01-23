package cn.swust.algorithms.featureGeneration;

import cn.swust.algorithms.featureGeneration.time.TimeFeatureExtractor2;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutoFeatureTransformer implements AlgoOperator<AutoFeatureTransformer> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public AutoFeatureTransformer() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "输入表只能有一个");

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        Table inputTable = inputs[0];


        //获取输入列类型
        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        TypeInformation<?>[] fieldTypes = inputTypeInfo.getFieldTypes();
        String[] fieldNames = inputTypeInfo.getFieldNames();

        Table tmp = null;
        OneHotEncoder estimator;
        for (int i = 0; i < fieldTypes.length; i++) {
            Class<?> typeClass = fieldTypes[i].getTypeClass();
            if (typeClass.equals(String.class)) {
                applyTimeFeatureExtractor(tEnv, inputTable, fieldNames[i]);
            }else if (typeClass.equals(Double.class)) {
               tmp = applyMeanImputer(tEnv, inputTable,i);
            }else if (typeClass.equals(Integer.class)){
                //调用独热编码
                System.out.println("one-hot-encoder");
                estimator = new OneHotEncoder().setInputCols("feature").setOutputCols("output");
                OneHotEncoderModel model = estimator.fit(tmp);
                Table[] transform = model.transform(tmp);
                //结果打印
                tEnv.toDataStream(transform[0]).print();
            }
        }

//        // 获取输入表的 TableSchema.
//        TableSchema tableSchema = inputTable.getSchema();
//        String[] columnNames = tableSchema.getFieldNames();  // 获取列名
//
////         处理每一列的数据类型
//        for (String column : columnNames) {
//            DataType columnType = tableSchema.getFieldDataType(column).orElseThrow(() -> new IllegalArgumentException("未能获取列数据类型"));
//            System.out.println(column);
//            if (columnType.equals(DataTypes.STRING())) {
//                // 如果是日期字符串类型，使用 TimeFeatureExtractor2 算子
////                applyTimeFeatureExtractor(tEnv, inputTable, column);
//            } else if (columnType.equals(DataTypes.INT()) || columnType.equals(DataTypes.DOUBLE())) {
//                // 如果是数值类型列，检查是否有缺失值
////                inputTable = applyMeanImputer(tEnv, inputTable, column);
//            }
//        }

        // 打印转换后的表，检查处理是否生效
//        inputTable.execute().print();

        // 返回转换后的表
//        return new Table[]{inputTable};
        return new Table[]{tmp};
    }

    private boolean matchDateTime(String date){
        // 定义正则表达式
        final String DATE_PATTERN = "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])$";
        final Pattern pattern = Pattern.compile(DATE_PATTERN);
        Matcher matcher = pattern.matcher(date);
        return matcher.matches();
    }

    private void applyTimeFeatureExtractor(StreamTableEnvironment tEnv, Table inputTable,String column) {
        // 使用 TimeFeatureExtractor2 算子提取时间特征
        TimeFeatureExtractor2 timeFeatureExtractor = new TimeFeatureExtractor2();
        timeFeatureExtractor.setInputCol(column);
        timeFeatureExtractor.setOutputCols("year", "month", "day");

        // 获取转换后的表
        Table[] transformedTable = timeFeatureExtractor.transform(inputTable);
        inputTable = transformedTable[0];  // 提取时间特征后更新 inputTable

        // 打印输出，以确保提取了时间特征
        tEnv.createTemporaryView("tt",inputTable);
        tEnv.sqlQuery("SELECT * FROM tt").execute().print();
    }


    private Table applyMeanImputer(StreamTableEnvironment tEnv, Table inputTable,int index) {
        // 给输入表创建临时视图
//        String tableName = "input_view";
//        tEnv.createTemporaryView(tableName, inputTable);  // 注册临时视图
//        String query = String.format("SELECT `%s` FROM %s", column, tableName);
//        Table tmpTable = tEnv.sqlQuery(query);
        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputTable.getResolvedSchema());
        //构建输出列
        RowTypeInfo outputTypeInfo = new RowTypeInfo(
                ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.DOUBLE),
                ArrayUtils.addAll(inputTypeInfo.getFieldNames(), "newValue")
        );
        DataStream<Row> rowData = tEnv.toDataStream(inputTable);
        //处理缺失值
        SingleOutputStreamOperator<Row> t1 = rowData.map(r -> {
            if (r.getField(index) == null) {
                r.setField(index, 0.0);
                return r;
            } else {
                return r;
            }
        },Types.ROW(inputTypeInfo.getFieldTypes()));

        //类型转换
        SingleOutputStreamOperator<Tuple2<Row, Integer>> t2 = t1.map(r -> {
            return Tuple2.of(r, 1);
        },Types.TUPLE(Types.ROW(inputTypeInfo.getFieldTypes()), Types.INT));

        //计算总和
        DataStream<Tuple2<Row, Integer>> totalData = DataStreamUtils.reduce(t2, new ReduceFunction<Tuple2<Row, Integer>>() {
            @Override
            public Tuple2<Row, Integer> reduce(Tuple2<Row, Integer> value1, Tuple2<Row, Integer> value2) throws Exception {
                value1.f0.setField(index,(Double)value1.f0.getField(index)+(Double)value2.f0.getField(index));
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        //计算均值
        final String broadCastKey = "broadcastKey1";
        DataStream<Row> finalData = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(t2),
                Collections.singletonMap(broadCastKey, totalData),
                inputList -> {
                    DataStream input = inputList.get(0);
                    return input
                            .map(
                                    new CalAvg(index, broadCastKey),
                                    outputTypeInfo);
                });
        //将结果转换为表
        return tEnv.fromDataStream(finalData);
    }

    private class CalAvg extends RichMapFunction<Tuple2<Row, Integer>, Row> {
        private final Integer index;
        private Double avg;
        private final String broadcastKey;

        private CalAvg(Integer index, String broadcastKey) {
            this.index = index;
            this.broadcastKey = broadcastKey;
        }

        @Override
        public Row map(Tuple2<Row, Integer> value) throws Exception {
            if (avg == null) {
                Tuple2<Row, Integer> tt =
                        (Tuple2<Row, Integer>) getRuntimeContext().getBroadcastVariable(broadcastKey).get(0);
                avg = (Double) tt.f0.getField(index)/tt.f1;
            }
            Row r = new Row(value.f0.getArity() + 1);
            if ((Double) value.f0.getField(index) == 0.0){
                for (int i = 0; i < value.f0.getArity(); i++) {
                    r.setField(i,value.f0.getField(i));
                }
                r.setField(value.f0.getArity(),avg);
            }else{
                for (int i = 0; i < value.f0.getArity(); i++) {
                    r.setField(i,value.f0.getField(i));
                }
                r.setField(value.f0.getArity(),value.f0.getField(index));
            }
            return r;
        }
    }


    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static AutoFeatureTransformer load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}

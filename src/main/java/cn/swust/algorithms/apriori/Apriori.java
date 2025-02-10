package cn.swust.algorithms.apriori;

import cn.hutool.core.collection.CollectionUtil;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

public class Apriori implements AprioriParams<Apriori>, AlgoOperator<Apriori> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public Apriori() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkNotNull(getItemSeparator(), "itemSeparator must not be null");
        Preconditions.checkArgument(getInputCols().length==1, "inputCols must be one");


        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        //生成数据记录流,仅关心记录条数
        SingleOutputStreamOperator<Tuple2<List<String>, Integer>> tmpMapData = tEnv
                .toDataStream(inputs[0])
                .map(row -> Tuple2.of(Arrays.asList((String) row.getField(getInputCols()[0])), 1),
                        Types.TUPLE(Types.LIST(Types.STRING), Types.INT));

        DataStream<Tuple2<List<String>, Integer>> recordData = DataStreamUtils.reduce(
                tmpMapData,
                (ReduceFunction<Tuple2<List<String>, Integer>>)
                        (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1),
                Types.TUPLE(Types.LIST(Types.STRING), Types.INT));
//        recordData.print();


        //将每一条数据的组合情况求出
        KeyedStream<Tuple2<List<String>, Integer>, Integer> keyedData = tEnv
                .toDataStream(inputs[0])
                .flatMap(new CalCombinationConditions(), Types.TUPLE(Types.LIST(Types.STRING), Types.INT))
                .keyBy(r -> Objects.hash(r.f0));

        //求解每一种情况出现的次数
        DataStream<Tuple2<List<String>, Integer>> reduceData = DataStreamUtils
                .reduce(
                        keyedData,
                        (ReduceFunction<Tuple2<List<String>, Integer>>)
                                (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1),
                        Types.TUPLE(Types.LIST(Types.STRING), Types.INT));
//        reduceData.print();

        //将 记录 广播到每个运算符中
        final String broadcastRecordKey = "broadcastRecordKey";
        DataStream<Tuple5<List<String>, Integer, Double, Double, Double>> supportData = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(reduceData),
                Collections.singletonMap(broadcastRecordKey, recordData),
                inputList -> {
                    DataStream input = inputList.get(0);
                    return input.map(
                            new CalSupport(broadcastRecordKey),
                            Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE));
                }
        );
//        supportData.print();

        //利用7元组保存频 繁项集、出现次数、前缀、后缀、支持度、置信度、提升度
        SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> tmp =
                supportData
                        .map(
                                r -> Tuple7.of(r.f0, r.f1, r.f2, r.f3, r.f4, new ArrayList<>(),new ArrayList<>()),
                                Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));

        //过滤出满足条件的流
        SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> replayData = tmp.flatMap(
                (FlatMapFunction<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>, Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>>)
                        (value, out) -> {
                            if(value.f0.size() == 1){
                                out.collect(Tuple7.of(value.f0, value.f1, value.f2, value.f2, 1.0,value.f5,value.f6));
                            }else{
                                out.collect(value);
                            }
                        },Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)))
                .filter(row -> row.f2 >= getMinSupport());

        //过滤出可变流
        SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> varData =
                replayData
                        .filter(row -> row.f0.size() == 1);

        //迭代求解 每轮创建新的运算符
        DataStreamList outputDataList1 = Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(varData),
                ReplayableDataStreamList.replay(replayData),
                new IterationConfig(IterationConfig.OperatorLifeCycle.PER_ROUND),//每一轮都会重新创建运算符
                (variableStreams, dataStreams) -> {
                    //把可变流广播到每一个并行任务上
                    final String broadcastVarKey = "broadcastVarKey";
                    DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> variable = variableStreams.get(0);
                    DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> replayed = dataStreams.get(0);

                    //计算置信度和提升度
                    DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> tmpData = BroadcastUtils.withBroadcastStream(
                            Collections.singletonList(replayed),
                            Collections.singletonMap(broadcastVarKey, variable),
                            inputList -> {
                                DataStream input = inputList.get(0);
                                return input.flatMap(
                                        new CalConfidenceAndLift(broadcastVarKey),
                                        Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));
                            }
                    );
                    //过滤出反馈流用于下回合的迭代
                    SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> feedback = tmpData.filter(r -> r.f4 != 0.0);

                    //利用map消除addTail报错
                    SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> newData =
                            feedback
                                    .map(
                                            r -> {
//                                                System.out.println(r);
                                                return r;
                                            },
                                            Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));

                    //构建自定义迭代终止条件，当反馈流数据条数为0时终止
                    /*SingleOutputStreamOperator<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> terminalData = tmpData.filter(r -> r.f4 == 0.0);
                    terminalData.map(r->{
                        System.out.println(r);
                        return r;
                    }, Types.TUPLE(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));
                    final String broadcastTerKey = "broadcastTerKey";
                    DataStream<Integer> terminalStream = BroadcastUtils.withBroadcastStream(
                            Collections.singletonList(newData),
                            Collections.singletonMap(broadcastTerKey, terminalData),
                            inputList -> {
                                DataStream input = inputList.get(0);
                                return input.flatMap(
                                        new TerminalUDF(broadcastVarKey, getMaxIter()));
                            }
                    );*/
                    SingleOutputStreamOperator<Integer> terminalStream = feedback.flatMap(new TerminateOnMaxIter<>(getMaxIter()));

                    return new IterationBodyResult(
                            DataStreamList.of(newData),//feedStream
                            DataStreamList.of(newData),//outputStream
                            terminalStream);//terminateStream 默认迭代20轮
                });//返回一个只读列表

        DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> outputData1 = outputDataList1.get(0);
//        outputData1.print();

        KeyedStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>, Integer> kk =
                outputData1.keyBy(r -> Objects.hash(r.f0,r.f1,r.f2,r.f3,r.f4,r.f5,r.f6));

        DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> tt =
                DataStreamUtils
                        .reduce(
                                kk,
                                (ReduceFunction<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>>)
                                        (value1, value2) -> value2);


        //流转表
        return new Table[]{convertToTable(tEnv, tt)};
    }

    private class TerminalUDF
            extends RichFlatMapFunction<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>, Integer>
            implements IterationListener<Integer>{
        private List<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> perRecords;
        private final String broadcastTerKey;
        private final int maxIter;

        private TerminalUDF(String broadcastTerKey, int maxIter) {
            this.broadcastTerKey = broadcastTerKey;
            this.maxIter = maxIter;
        }

        @Override
        public void flatMap(Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>> value, Collector<Integer> out) throws Exception {
            if (perRecords == null) {
                try {
                    System.out.println("perRecords is null");
                    perRecords = getRuntimeContext().getBroadcastVariable(broadcastTerKey);
                }catch (NullPointerException e){
                    System.out.println("terminalData is null");
                    perRecords = null;
                }
            }
        }

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<Integer> collector) throws Exception {
            System.out.println("i "+ i);
            if (i + 1 < maxIter && perRecords != null) {
                collector.collect(0);
            }

        }

        @Override
        public void onIterationTerminated(Context context, Collector<Integer> collector) throws Exception {

        }
    }

    private Table convertToTable(StreamTableEnvironment tEnv, DataStream<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> inputDataStream){
        //过滤出满足条件的数据
        // 7个字段 保存 频繁项集、出现次数、前缀、后缀、支持度、置信度、提升度
        SingleOutputStreamOperator<Row> outputData = inputDataStream.filter(r->r.f3>=getMinConfidence()&&r.f4>=getLift()).map(r -> {
            int len = r.getArity();
            Row row = new Row(len);
            for (int i = 0; i < len; i++) {
                row.setField(i, r.getField(i));
            }
            return row;
        }).returns(Types.ROW(Types.LIST(Types.STRING), Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));

        return tEnv
                .fromDataStream(outputData)
                .as("itemSet","count"," support", "confidence", "lift", "prefix", "suffix");
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static Apriori load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }


    private class CalConfidenceAndLift
            extends RichFlatMapFunction<
            Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>,
            Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> {

        private final String broadcastKey;
        private Map<List<String>, Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> map;

        public CalConfidenceAndLift(String broadcastKey) {
            this.broadcastKey = broadcastKey;
        }

        @Override
        public void flatMap(Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>> value, Collector<Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>>> out) throws Exception {
            if (map == null) {
                List<Tuple7<List<String>, Integer, Double, Double, Double, List<String>,List<String>>> tmp1 = getRuntimeContext().getBroadcastVariable(broadcastKey);
                map = new HashMap<>();
                for (Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>> record : tmp1) {
                    map.put(record.f0, record);
                }
                tmp1.clear();
            }
//            System.out.println(value);
            //避免重复计算
            if(value.f4!=0.0) {
                out.collect(value);
                return;
            }

            //将数据分为前缀和后缀
            List<String> items = value.f0;
            int len = items.size()-1;
            //计算每一条流的组合情况 eg:ABC --> 计算C(3,2) --> 计算C(items.len,items.len-1)
            //构建字符数组
            String[] strs = items.toArray(new String[0]);
            //计算获得组合数
            while(len >= 1){
                String[][] nchs = nchoosek(strs, len);
                //构建前缀和后缀
                for (String[] nch : nchs) {
                    List<String> frontList= Arrays.asList(nch);
                    //计算后缀,求解差集
                    List<String> backList = CollectionUtil.subtractToList(items, frontList);
                    //TODO:修改String为List
                    //计算置信度 AB: A->B 即 AB出现的次数 除以 B出现的次数
                    Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>> cnt = map.get(backList);
                    if (cnt != null) {
                        double confidence = value.f1.doubleValue() / cnt.f1.doubleValue();
                        //计算提升度 AB: A->B 即 confidence(A->B) / support(A)
                        //判断前缀是否在map中
                        if (map.containsKey(frontList)) {
                            double frontSupport = map.get(frontList).f2;
                            double lift = confidence / frontSupport;
//                    System.out.println(Tuple5.of(value.f0,value.f1,value.f2,confidence,lift));
                            List<String> tmpStrName = new ArrayList<>(frontList);
                            tmpStrName.addAll(backList);
                            Tuple7<List<String>, Integer, Double, Double, Double, List<String>, List<String>> tmp2 =
                                    Tuple7.of(tmpStrName, value.f1, value.f2, confidence, lift,frontList, backList);
                            out.collect(tmp2);
                        }
                    }
                }
                len--;
            }
        }
    }

    private class CalSupport extends RichMapFunction<Tuple2<List<String>, Integer>, Tuple5<List<String>, Integer, Double, Double, Double>> {
        private Tuple2<List<String>, Integer> record;
        private final String broadcastKey;

        public CalSupport(String broadcastKey) {
            this.broadcastKey = broadcastKey;
        }

        @Override
        public Tuple5<List<String>, Integer, Double, Double, Double> map(Tuple2<List<String>, Integer> value) throws Exception {
            if (record == null) {
                record = (Tuple2<List<String>, Integer>) getRuntimeContext().getBroadcastVariable(broadcastKey).get(0);
            }
//            System.out.println(Tuple5.of(value.f0, value.f1, value.f1.doubleValue() / record.f1, 0.0, 0.0));
            return Tuple5.of(value.f0, value.f1, value.f1.doubleValue() / record.f1, 0.0, 0.0);
        }
    }

    private class CalCombinationConditions implements FlatMapFunction<Row, Tuple2<List<String>, Integer>> {
        @Override
        public void flatMap(Row value, Collector<Tuple2<List<String>, Integer>> out) throws Exception {
            String line = (String) value.getField(getInputCols()[0]);
            //按照 给定 分隔符 进行 分割
            String[] strs = line.split(getItemSeparator());
            int len = strs.length;
//            System.out.println(len);
            for (int i = 1; i <= len; i++) {
                String[][] nch = nchoosek(strs, i);
                for (String[] strings : nch) {
                    out.collect(Tuple2.of(Arrays.asList(strings), 1));
                }
            }
        }

//        private String[] matchWord(String str){
//            List<String> content = new ArrayList<>();
//            Pattern pattern = Pattern.compile("[\\u4e00-\\u9fa5_a-zA-Z0-9]+");
//            Matcher matcher = pattern.matcher(str);
//            while (matcher.find()) {
//                content.add(matcher.group());
//            }
//            return content.toArray(new String[0]);
//        }
    }

    //求解组合数情况
    private String[][] nchoosek(String[] array, int k) {
        int n = array.length;
        if (k == 0) {
            return new String[1][0];
        }
        return nchoosek0(array, n, k);
    }

    private String[][] nchoosek0(String[] array, int n, int k) {
        String[][] comb = null;
        if (n == k) {
            comb = new String[1][n];
            for (int i = 0; i < n; i++) {
                comb[0][i] = array[i];
            }
            return comb;
        }
        if (k == 1) {
            comb = new String[n][1];
            for (int i = 0; i < n; i++) {
                comb[i][0] = array[i];
            }
            return comb;
        }
        for (int i = 0, limit = n - k + 1; i < limit; i++) {
            String[][] next = nchoosek0(Arrays.copyOfRange(array, i + 1, n), n - i - 1, k - 1); // Get all possible values for the next one
            int combRowLen = comb == null ? 0 : comb.length;
            int totalRowLen = next.length + combRowLen;
            int totalColLen = next[0].length + 1;
            String[][] tempComb = new String[totalRowLen][totalColLen];
            if (comb != null) { // TempComb capacity expansion comb
                for (int j = 0; j < combRowLen; j++) {
                    tempComb[j] = Arrays.copyOf(comb[j], totalColLen);
                }
            }
            String value = array[i];
            for (int row = combRowLen; row < totalRowLen; row++) {
                tempComb[row][0] = value; // The value completes the current one
                for (int col = 1; col < totalColLen; col++) { // Copy the next one.
                    tempComb[row][col] = next[row - combRowLen][col - 1];
                }
            }
            comb = tempComb;
        }
        return comb;
    }
}
/**
 * 方案一 使用 全轮 存在的运算符
 */
//        //进行迭代求解 每轮不创建新的运算符
//        DataStreamList outputDataList = Iterations.iterateBoundedStreamsUntilTermination(
//                DataStreamList.of(varData),
//                ReplayableDataStreamList.replay(replayData),
//                IterationConfig.newBuilder().build(),//默认运算符只创建一次，在整个迭代过程中将不再创建
//                (variableStreams, dataStreams) -> {
//                    //把可变流广播到每一个并行任务上
//                    final String broadcastVarKey = "broadcastVarKey";
//                    DataStream<Tuple7<String, Integer, Double, Double, Double, String, String>> variable = variableStreams.get(0);
//                    DataStream<Tuple7<String, Integer, Double, Double, Double, String, String>> replayed = dataStreams.get(0);
//
//                    //设置迭代轮数 默认20轮
//                    DataStream<Integer> terminateStream =
//                            variable
//                                    .flatMap(
//                                            new TerminateOnMaxIter(5));
//
//                    //查看每一轮可变流的改变
//                    variable.map(r -> {
//                        System.out.println(r);
//                        return r;
//                    }, Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE));
//
//                    //计算置信度和提升度
//                    DataStream<Tuple7<String, Integer, Double, Double, Double, String, String>> tmpData = BroadcastUtils.withBroadcastStream(
//                            Collections.singletonList(replayed),
//                            Collections.singletonMap(broadcastVarKey, variable),
//                            inputList -> {
//                                DataStream input = inputList.get(0);
//                                return input.flatMap(
//                                        new CalConfidenceAndLift(broadcastVarKey),
//                                        Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING, Types.STRING));
//                            }
//                    );
//                    //利用map消除addTail报错
//                    SingleOutputStreamOperator<Tuple7<String, Integer, Double, Double, Double, String, String>> newData =
//                            tmpData
//                                    .map(
//                                            r -> {
////                                                System.out.println(r);
//                                                return r;
//                                            },
//                                            Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING, Types.STRING));
//
//
//                    //仅将最后一轮迭代的结果返回输出
//                    DataStream<Tuple7<String, Integer, Double, Double, Double, String, String>> finalData =
//                            tmpData
//                                    .flatMap(
//                                            new ForwardInputsOfLastRound<>());
//
//                    finalData.map(r->{
//                        System.out.println(r);
//                        return r;
//                    },Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING, Types.STRING));
////                    System.out.println(tmpData.getTransformation().getInputs().size());
////                    System.out.println(tmpData.getTransformation() instanceof PhysicalTransformation);
////                    System.out.println(newData.getTransformation().getInputs().size());
////                    System.out.println(newData.getTransformation() instanceof PhysicalTransformation);
//
//                    return new IterationBodyResult(
//                            DataStreamList.of(newData),//feedStream
//                            DataStreamList.of(finalData),//outputStream
//                            terminateStream);//terminateStream
//                });//返回一个只读列表

//        DataStream<Tuple7<String, Integer, Double, Double, Double, String, String>> outputData = outputDataList.get(0);
//        outputData.print();

//        SingleOutputStreamOperator<Tuple7<String, Integer, Double, Double, Double, String, String>> endData = outputData.connect(varData).process(new CoProcessFunction<Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>>() {
//            @Override
//            public void processElement1(Tuple7<String, Integer, Double, Double, Double, String, String> value, CoProcessFunction<Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>>.Context ctx, Collector<Tuple7<String, Integer, Double, Double, Double, String, String>> out) throws Exception {
//                out.collect(value);
//            }
//
//            @Override
//            public void processElement2(Tuple7<String, Integer, Double, Double, Double, String, String> value, CoProcessFunction<Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>, Tuple7<String, Integer, Double, Double, Double, String, String>>.Context ctx, Collector<Tuple7<String, Integer, Double, Double, Double, String, String>> out) throws Exception {
//                out.collect(value);
//            }
//        });
//
//        endData.print();
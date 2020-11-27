package com.ym123.App;

import com.ym123.bean.PvCount;
import com.ym123.bean.UserBehavior1;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 网站的页面浏览量
 *
 * @author ymstart
 * @create 2020-11-26 14:46
 */
public class PageView2 {
    public static void main(String[] args) throws Exception {
        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior1> map = readTextFile.map(new MapFunction<String, UserBehavior1>() {
            @Override
            public UserBehavior1 map(String value) throws Exception {
                String[] words = value.split(",");
                return new UserBehavior1(
                        Long.parseLong(words[0]),
                        Long.parseLong(words[1]),
                        Integer.valueOf(words[2]),
                        words[3],
                        Long.parseLong(words[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior1>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior1 element) {
                return element.getTimestamp() * 1000;
            }
        });

        SingleOutputStreamOperator<PvCount> aggregate = map.filter(date -> "pv".equals(date.getBehavior())).map(new MapFunction<UserBehavior1, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior1 value) throws Exception {
                Random random = new Random();
                return new Tuple2<>("pv_" + random.nextInt(4), 1);
            }
        }).keyBy(0).timeWindow(Time.hours(1)).aggregate(new countPV(), new windowPV());

        aggregate.keyBy("windowEnd").process(new pvSum())


        env.execute();
*/
    }

    public static class countPV implements AggregateFunction<Tuple2<String, Integer>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class windowPV implements WindowFunction<Long, PvCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            String field = tuple.getField(0);
            Long pvCOUNT = input.iterator().next();
            long windowEnd = window.getEnd();
            out.collect(new PvCount(field, windowEnd, pvCOUNT));
        }
    }

    public static class pvSum extends KeyedProcessFunction<Long, PvCount, String> {

        //状态
        private ListState<PvCount> listState = null;

        @Override
        public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PvCount>("pv_count", PvCount.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取状态信息
            Iterable<PvCount> pvCounts = listState.get();

            Long count = 0L;

            while (pvCounts.iterator().hasNext()) {
                count += 1;
            }

            out.collect("pv:" + count);

            listState.clear();
        }
    }
}

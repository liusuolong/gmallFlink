package com.ym123.App;

import com.ym123.bean.ItemCount;
import com.ym123.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 实时热门商品统计
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 *
 * @author ymstart
 * @create 2020-11-25 18:04
 */
public class HotItemApp {
    public static void main(String[] args) throws Exception {
        //1.设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件
        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.csv");

        //3.转换为样例类 并设置watermark为追加方式
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.map(new MapFunction<String, UserBehavior>() {
            public UserBehavior map(String value) throws Exception {
                String[] words = value.split(",");
                return new UserBehavior(
                        Long.parseLong(words[0]),
                        Long.parseLong(words[1]),
                        Integer.valueOf(words[2]),
                        words[3],
                        Long.parseLong(words[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000;
            }
        });

//        SingleOutputStreamOperator<ItemCount> aggregate = userBehaviorDS
//                .filter(date -> "pv".equals(date.getBehavior()))
//                .keyBy(date -> date.getItemId())
//                .timeWindow(Time.hours(1), Time.minutes(5))
//                .aggregate(new itemCount(), new windowResult());

        //过滤出页面点击数据behavior = "pv"
        SingleOutputStreamOperator<UserBehavior> filterPvDS = userBehaviorDS.filter(new FilterFunction<UserBehavior>() {
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });
        //filterPvDS.print("pv");

        //按商品itemId进行分组
        KeyedStream<UserBehavior, Long> keyByItemId = filterPvDS.keyBy(date -> date.getItemId());
        //keyByItemId.print();

        //开窗
        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWindowDS = keyByItemId.timeWindow(Time.hours(1), Time.minutes(5));

        //new itemCount()定义聚合规则
        //new WindowResult() 定义输出数据类型
        SingleOutputStreamOperator<ItemCount> aggregate = userBehaviorWindowDS.aggregate(new itemCount(), new windowResult());
        //aggregate.print();

        //按windowEnd分组,取每个商品的top
        SingleOutputStreamOperator<String> topN = aggregate
                .keyBy("windowEnd").process(new topN(5));

        topN.print();

        env.execute();


    }

    public static class itemCount implements AggregateFunction<UserBehavior, Long, Long> {
        public Long createAccumulator() {
            return 0L;
        }

        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        public Long getResult(Long accumulator) {
            return accumulator;
        }

        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class windowResult implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            //窗口结束时间
            long windowEnd = window.getEnd();
            //计数
            Long count = input.iterator().next();
            out.collect(new ItemCount(itemId, windowEnd, count));
        }
    }

    public static class topN extends KeyedProcessFunction<Tuple, ItemCount, String> {

        private Integer topn;

        public topN() {
        }

        public topN(Integer topn) {
            this.topn = topn;
        }

        //保存状态
        //定义ListStage 存放相同window下相同key的数据
        private ListState<ItemCount> listStage;

        @Override
        public void open(Configuration parameters) throws Exception {
            listStage = getRuntimeContext()
                    .getListState(new ListStateDescriptor<ItemCount>("top_n", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据放入集合
            listStage.add(value);
            //定义一个定时器
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            Iterator<ItemCount> iterator = listStage.get().iterator();
            //将iterator转换为List
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);
            //排序
            //使用比较器正序排
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            //数据最终输出
            StringBuilder sb = new StringBuilder();

            sb.append("===========================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1L)).append("\n");
            //取topN
            for (int i = 0; i < Math.min(topn, itemCounts.size()); i++) {
                ItemCount itemCount = itemCounts.get(i);
                sb.append("top:").append(i + 1);
                sb.append("itemId:").append(itemCount.getItemId());
                sb.append("商品热度:").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("===========================\n");

            //清空状态
            listStage.clear();

            Thread.sleep(2000);

            out.collect(sb.toString());
        }
    }
}

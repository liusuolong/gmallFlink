package com.ym123.App;

import com.ym123.bean.ItemCount;
import com.ym123.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

/**
 * @author ymstart
 * @create 2020-11-26 9:35
 */
public class HotItemApp_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop002:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("hot", new SimpleStringSchema(), properties));

        //kafkaSource.print();

        SingleOutputStreamOperator<UserBehavior> map = kafkaSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] words = value.split(",");
                return new UserBehavior(
                        Long.parseLong(words[0]),
                        Long.parseLong(words[1]),
                        Integer.valueOf(words[2]),
                        words[3],
                        Long.parseLong(words[4]));
            }
        });
        //map.print();

        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });
        //filter.print();

        KeyedStream<UserBehavior, Long> keyByItemDS = filter.keyBy(date -> date.getItemId());

        //keyByItemDS.print();

        WindowedStream<UserBehavior, Long, TimeWindow> timeWindowDS = keyByItemDS.timeWindow(Time.hours(1), Time.seconds(3));

        //将同一个窗口中相同的key做聚合
        /**
         * ?????????????????
         *读不出kafka数据
         *aggregate.print();
         */
        SingleOutputStreamOperator<ItemCount> aggregate = timeWindowDS.aggregate(new countItem(), new windowCount());

        SingleOutputStreamOperator<String> topN = aggregate.keyBy("windowEnd").process(new TOPN());

        topN.print();

        env.execute();
    }

    public static class countItem implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    public static class windowCount implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            //获取窗口结束时间
            long windowEnd = window.getEnd();
            Long itemCount = input.iterator().next();
            out.collect(new ItemCount(itemId, windowEnd, itemCount));
        }
    }

    public static class TOPN extends KeyedProcessFunction<Tuple, ItemCount, String> {

        private Integer topn;

        public TOPN() {
        }

        public TOPN(Integer topn) {
            this.topn = topn;
        }

        //定义状态
        private ListState<ItemCount> listState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("top-n", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据放入集合中
            listState.add(value);
            //定义一个定时器
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //从状态中取出数据,转换为String
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);
            //排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if(o1.getCount() > o2.getCount()){
                        return -1;
                    }else if (o1.getCount() < o2.getCount()){
                        return 1;
                    }else{
                        return 0;
                    }
                }
            });
            StringBuilder sb = new StringBuilder();

            //取TOPN
            sb.append("窗口结束时间:").append(new Timestamp(timestamp-1L));
            for (int i = 0; i < Math.min(topn, itemCounts.size()); i++) {
                ItemCount itemCount = itemCounts.get(i);
                sb.append("top_").append(i+1);
                sb.append("itemId:").append(itemCount.getItemId());
                sb.append("count:").append(itemCount.getCount());
            }
            sb.append("=========================\n");

            //清空状态
            listState.clear();

            Thread.sleep(2000);

            out.collect(sb.toString());
        }
    }
}

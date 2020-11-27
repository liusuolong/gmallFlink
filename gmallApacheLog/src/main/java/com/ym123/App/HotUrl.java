package com.ym123.App;

import com.ym123.bean.ApacheLogCount;
import com.ym123.bean.ApacheLogEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * 热门页面浏览数
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 *
 * @author ymstart
 * @create 2020-11-26 11:21
 */
public class HotUrl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //DataStreamSource<String> readTextFile = env.readTextFile("input/apache.log");
        DataStreamSource<String> readTextFile = env.socketTextStream("hadoop002", 2222);

        SingleOutputStreamOperator<ApacheLogEvent> apacheLogDS = readTextFile.map(new MapFunction<String, ApacheLogEvent>() {
            public ApacheLogEvent map(String value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                String[] words = value.split(" ");
                long time = sdf.parse(words[3]).getTime();
                return new ApacheLogEvent(words[0], words[1], time, words[5], words[6]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getEventTime();
            }
        });

        SingleOutputStreamOperator<ApacheLogEvent> filter = apacheLogDS.filter(new FilterFunction<ApacheLogEvent>() {
            public boolean filter(ApacheLogEvent value) throws Exception {
                return "GET".equals(value.getMethod());
            }
        });

        KeyedStream<ApacheLogEvent, String> keyByURL = filter.keyBy(date -> date.getUrl());

        WindowedStream<ApacheLogEvent, String, TimeWindow> timeWindow =
                keyByURL.timeWindow(Time.minutes(10), Time.seconds(5))
                        .allowedLateness(Time.minutes(1))
                        .sideOutputLateData(new OutputTag<ApacheLogEvent>("sideOut"){});

        SingleOutputStreamOperator<ApacheLogCount> aggregate = timeWindow.aggregate(new countURL(), new windowCount());

        SingleOutputStreamOperator<String> topNUrl = aggregate.keyBy(date -> date.getWindowEnd()).process(new topNURL(5));

        topNUrl.print();
        //侧输出流
        aggregate.getSideOutput(new OutputTag<ApacheLogEvent>("sideOut"){}).print("sideOut");
        env.execute();
    }

    public static class countURL implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    public static class windowCount implements WindowFunction<Long, ApacheLogCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<ApacheLogCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long urlCount = input.iterator().next();
            out.collect(new ApacheLogCount(url, windowEnd, urlCount));
        }
    }

    public static class topNURL extends KeyedProcessFunction<Long, ApacheLogCount, String> {

        private Integer topN;

        public topNURL() {
        }

        public topNURL(Integer topN) {
            this.topN = topN;
        }

        //定义状态
        //Map可以去重
        private MapState<String,ApacheLogCount> mapState = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,ApacheLogCount>("url-topN",String.class,ApacheLogCount.class));
        }

        @Override
        public void processElement(ApacheLogCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据放入集合
            mapState.put(value.getUrl(),value);
            //设置一个定时器,处理状态中的定时器
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
            //设置一个定时器,处理清空状态
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 600000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp == ctx.getCurrentKey() + 600000L){
                //清空状态
                mapState.clear();
                return;
            }

            //读取数据状态并转换为String
            Iterator<Map.Entry<String, ApacheLogCount>> iterator = mapState.entries().iterator();
            ArrayList<Map.Entry<String, ApacheLogCount>> entries = Lists.newArrayList(iterator);
            //排序
            entries.sort(new Comparator<Map.Entry<String, ApacheLogCount>>() {
                @Override
                public int compare(Map.Entry<String, ApacheLogCount> o1, Map.Entry<String, ApacheLogCount> o2) {
                    if (o1.getValue().getUrlCount() > o2.getValue().getUrlCount()){
                        return -1;
                    }else if (o1.getValue().getUrlCount() < o2.getValue().getUrlCount()){
                        return 1;
                    }else {
                        return 0;
                    }
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("=============================\n");
            sb.append("窗口关闭时间：").append(new Timestamp(timestamp -1L)).append("\n");
            //取topN
            for (int i = 0; i < Math.min(topN, entries.size()); i++) {
                Map.Entry<String, ApacheLogCount> entry = entries.get(i);
                sb.append("topN").append(i+1);
                sb.append("url:").append(entry.getValue().getUrl());
                sb.append("页面热度：").append(entry.getValue().getUrlCount());
                sb.append("\n");
            }
            sb.append("=============================\n");

            Thread.sleep(2000);

            out.collect(sb.toString());

        }
    }
}

package com.ym123.App;

import com.ym123.bean.UserBehavior1;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**网站的页面浏览量
 * 存在数据倾斜
 * @author ymstart
 * @create 2020-11-26 14:46
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> pv =
                //先过滤出pv数据
                map.filter(new FilterFunction<UserBehavior1>() {
            @Override
            public boolean filter(UserBehavior1 value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
            //按商品id分组
        }).keyBy(date -> date.getItemId()).map(new MapFunction<UserBehavior1, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior1 value) throws Exception {
                return new Tuple2<>("pv", 1);
            }
        }).keyBy(0).timeWindow(Time.hours(1)).sum(1);

        pv.print();

        env.execute();

    }
}

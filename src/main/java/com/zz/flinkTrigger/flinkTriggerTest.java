package com.zz.flinkTrigger;

import cn.hutool.core.io.FileUtil;
import com.zz.flinkTest.flinkSlide;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
trigger 测试
滚动窗口，20s
然后是trigger内部技术，10个元素输出一次。
没有keyby的时候开窗是 windowall

https://www.jianshu.com/p/df363876e942
*/
public class flinkTriggerTest {

    public static void main(String[] args) throws Exception {
// set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("group.id", "test");*/

        AllWindowedStream<Integer, TimeWindow> stream = env
                .addSource(new sourceTrigger())
                .map(new String2Integer())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
//                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(2)))
                .trigger(CustomProcessingTimeTrigger.create());

        stream.sum(0).print();

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;
        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }
        @Override
        public void open(Configuration parameters) throws Exception {
        }
    }

    public static class sourceTrigger extends RichSourceFunction<String> {
        private boolean isrun=true;
        private List<String> sourceList;

        @Override
        public void open(Configuration parameters) throws Exception {
            File file = new File("src/main/java/com/zz/flinkTrigger/flinkTrigger.txt");
            sourceList = FileUtil.readUtf8Lines(file);
        }
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isrun) {
                for (String s : sourceList) {
                    TimeUnit.MILLISECONDS.sleep(100);
                    ctx.collect(s);
                }
            }
        }

        @Override
        public void cancel() {
            isrun=false;
        }
    }

}
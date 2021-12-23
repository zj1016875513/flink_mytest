package com.zz.flinkTest;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class wordCountTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.addSource(new source1(),"random source")
            .partitionCustom(getPartitioner(),getKeySelector())
                .keyBy(x->x)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        String key = ctx.getCurrentKey();
                        out.collect("key= "+key+"  value= "+value);
                    }
                })
            .print("main");

//        env.addSource(new source1(),"random source")
//                .keyBy(x->x)
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(2)))
//                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
//                        Date start = new Date(context.window().getStart());
//                        Date end = new Date(context.window().getEnd());
//                        ArrayList<String> list = new ArrayList<>();
//                        for (String ele : elements) {
//                            list.add(ele);
//                        }
//                        String outStr="window= ["+start+"->"+end+") list_size="+list.size()+" key= "+key;
//                        out.collect(outStr);
//                    }
//                })
//                .print("main");

        env.execute(wordCountTest.class.getSimpleName());
    }

    private static Partitioner<String> getPartitioner() {
        return new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
//                System.out.println(key.hashCode() % numPartitions);
                return Math.abs(key.hashCode()%numPartitions);
            }
        };
    }

    private static KeySelector<String, String> getKeySelector() {
        return new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        };
    }

    public static class source1 extends RichSourceFunction<String>{
         private List<String> list;
         private boolean isrun;
        @Override
        public void open(Configuration parameters) throws Exception {
            list = FileUtils
                    .readLines(new File("src/main/java/com/zz/flink/wordCount/words.txt"), StandardCharsets.UTF_8);
        }

        @Override
        public void run(SourceContext<String> ctx) throws InterruptedException {
            isrun = true;
            while (isrun) {
                for (String ele : list) {
                    ctx.collect(ele);
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        }

        @Override
        public void cancel() {
            isrun=false;
        }
    }
}

package com.zz.flinkTest;

import cn.hutool.core.io.FileUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class flinkSlide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<String> wms1 = WatermarkStrategy.
                <String>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[2]);
                    }
                });



        env.addSource(new source1())
            .keyBy(x->x.split(",")[0])
            .window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(2)))
            .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                MapState<String, Integer> mapstate;
                @Override
                public void open(Configuration parameters) throws Exception {

                    mapstate = getRuntimeContext()
                            .getMapState(new MapStateDescriptor<String, Integer>("mapstate", String.class, Integer.class));
                }

                @Override
                public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                    ArrayList<String> list = new ArrayList<>();
                    for (String element : elements) {
                        String data = element.split(",")[1];
                        list.add(data);
                    }
                    Collections.sort(list, new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            int num1 = Integer.parseInt(o1.split("-")[1]);
                            int num2 = Integer.parseInt(o2.split("-")[1]);
                            return num2-num1;//1-2为增序 2-1为倒序
                        }
                    });
                    ArrayList<String> top3List = new ArrayList<>();
                    for (int i = 0; i < 3; i++) {
                        String res = null;
                        try {
                            res = list.get(i);
                        } catch (Exception e) {
                            continue;
                        }
                        if (res!=null) {
                            top3List.add(res);
                        }
                    }
                    Collections.reverse(list);

                    TimeWindow w = context.window();
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                    long start = w.getStart();
                    long end = w.getEnd();
                    String startPt = sdf.format(start);
                    String endPt = sdf.format(end);
                    int num = list.size();

                    mapstate.put(startPt+"->"+endPt+";key="+s,num);
                    for (Map.Entry<String, Integer> entry : mapstate.entries()) {
                        String key = entry.getKey();
                        Integer value = entry.getValue();
                        String mapOut = "key= "+key+" -> "+"value= "+value;
                        System.out.println(mapOut);
                    }
                    out.collect("w_start= "+startPt+"; "
                                    +"w_end= "+endPt+"; "
                                    +"key= "+s+"; "
                                    +"count= "+list.size()+"; "
                                    +"top3List= "+top3List+"; "
                                    +"value= "+list
                    );
                }
            }).print();

        env.execute("test1");

    }

    public static class source1 extends RichSourceFunction<String> {
        private boolean isrun=true;
        private List<String> sourceList;

        @Override
        public void open(Configuration parameters) throws Exception {
            File file = new File("src/main/java/com/zz/flinkTest/flinkTest1.txt");
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

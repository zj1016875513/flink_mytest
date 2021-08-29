package com.zz.flink.single_pass;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class Flink_File_Sink {
    public static StreamingFileSink<String> getFileSink(){
        DefaultRollingPolicy<String, String> rollPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(TimeUnit.SECONDS.toSeconds(10))/*每隔多长时间生成一个文件*/
                .withInactivityInterval(TimeUnit.SECONDS.toSeconds(10))/*默认60秒,未写入数据处于不活跃状态超时会滚动新文件*/
                .withMaxPartSize(128 * 1024 * 1024)/*设置每个文件的最大大小 ,默认是128M*/
                .build();
        /*输出文件的前、后缀配置*/
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("news")
                .withPartSuffix(".txt")
                .build();

        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                /*forRowFormat指定文件的跟目录与文件写入编码方式，这里使用SimpleStringEncoder 以UTF-8字符串编码方式写入文件*/
                .forRowFormat(new Path("file:///D:/IDEAworkspace/flink_mytest/output/fileSinkDir"), new SimpleStringEncoder<String>("UTF-8"))
                /*这里是采用默认的分桶策略DateTimeBucketAssigner，它基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH*/
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /*设置上面指定的滚动策略*/
                .withRollingPolicy(rollPolicy)
                /*桶检查间隔，这里设置为1s*/
                .withBucketCheckInterval(10)
                /*指定输出文件的前、后缀*/
                .withOutputFileConfig(config)
                .build();

        return streamingFileSink;
    }
}

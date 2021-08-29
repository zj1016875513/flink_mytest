package com.zz.flink.single_pass;

import cn.hutool.core.io.FileUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class flink_file {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        env.setStateBackend(new FsStateBackend("file:///D:/IDEAworkspace/flink_mytest/output/checkpoint"));
//        env.setStateBackend(new FsStateBackend("file:///output/checkpoint"));
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SingleOutputStreamOperator<String> stream = env.addSource(new file_source())
                .filter(x -> x.split("\t").length == 4)//是否还需要过滤url相同的？
                .map(new file_map());

        stream.print();
//        stream.addSink(Flink_File_Sink.getFileSink());
        stream.addSink(new Hbase_sink());


        try {
            env.execute(flink_file.class.getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

package com.zz.flink.single_pass;

import cn.hutool.core.io.FileUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

public  class file_source extends RichSourceFunction<String> {
    boolean isrun=true;
    List<String> newsList;
    @Override
    public void open(Configuration parameters) throws Exception {
        File file_in = new File("data/news_test_list.txt");
        newsList = FileUtil.readUtf8Lines(file_in);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isrun) {
            for (String s : newsList) {
                ctx.collect(s);
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
    }

    @Override
    public void cancel() {
        isrun=false;
    }
}

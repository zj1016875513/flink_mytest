package com.zz.flink.single_pass;

import com.alibaba.fastjson.JSON;
import com.zz.flink.bean.News;
import com.zz.flink.bean.NewsTopic;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

public class file_map implements MapFunction<String,String>, CheckpointedFunction {
    ListState<NewsTopic> listState;
    ArrayList<NewsTopic> list=new ArrayList<>();
    @Override
    public String map(String value) throws Exception {
        String[] split = value.split("\t");
        String url = split[0];
        String lt = split[1];
        String rt = split[2];
        String pt = split[3];
        String uuid = UUID.nameUUIDFromBytes(rt.getBytes()).toString();
        String intoTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(System.currentTimeMillis()));
        String topicIndex = "";
        News news ;
        NewsTopic newsTopic = null;
        if (list.isEmpty()) {
            topicIndex =uuid;
            news = new News(url, lt, rt, pt, uuid, topicIndex, intoTime);
            ArrayList<News> newsList = new ArrayList<>();
            newsList.add(news);
             newsTopic = new NewsTopic(topicIndex, newsList);
            list.add(newsTopic);
        }else {
            Tuple2<String, Double> res = getMaxsimilaryAndIndex(list, rt);
            if (res.f1>=0.6){
                topicIndex=res.f0;
                news = new News(url, lt, rt, pt, uuid, topicIndex, intoTime);
                for (NewsTopic newsTopic1 : list) {
                    if (news.getTopicindex().equals(newsTopic1.getTopicIndex())) {
                        newsTopic1.getNewsList().add(news);
                        newsTopic=newsTopic1;
                        break;
                    }
                }
            }else {
                topicIndex=uuid;
                news = new News(url, lt, rt, pt, uuid, topicIndex, intoTime);
                ArrayList<News> newsList = new ArrayList<>();
                newsList.add(news);
                 newsTopic = new NewsTopic(topicIndex, newsList);
                list.add(newsTopic);
            }
        }

       return JSON.toJSONString(newsTopic);

    }

    public Tuple2<String,Double> getMaxsimilaryAndIndex(ArrayList<NewsTopic> list, String rt){
        String topicIndex = "";
        News cur_news;
        double Maxsimilarity=0.0;
        for (NewsTopic newsTopic : list) {
            ArrayList<News> newsList = newsTopic.getNewsList();
            for (News news : newsList) {
                cur_news = news;
                String listRT = news.getRt();
                double similarity = Cosine.getSimilarity(rt, listRT);
                if (Maxsimilarity<similarity){
                    Maxsimilarity=similarity;
                    topicIndex=news.getTopicindex();
                }
            }
        }
        return new Tuple2<>(topicIndex,Maxsimilarity);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.update(list);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<NewsTopic> listStateDescriptor = new ListStateDescriptor<>("listState", NewsTopic.class);
         listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        for (NewsTopic ele : listState.get()) {
            list.add(ele);
        }
    }
}

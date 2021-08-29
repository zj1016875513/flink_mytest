package com.zz.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewsTopic {
    private String topicIndex;
    private String first_update;
    private String end_update;
    private ArrayList<News> newsList;
}

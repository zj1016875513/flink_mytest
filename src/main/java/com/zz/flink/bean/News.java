package com.zz.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class News {
    private String url;
    private String lt;
    private String rt;
    private String pt;
    private String uuid;
    private String Topicindex;
    private String intoTime;
}

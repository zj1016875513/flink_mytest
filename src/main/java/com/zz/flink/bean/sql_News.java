package com.zz.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class sql_News {
    private String url;
    private String lt;
    private String rt;
    private long time;
}

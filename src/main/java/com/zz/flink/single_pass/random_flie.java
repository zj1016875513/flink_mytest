package com.zz.flink.single_pass;


import cn.hutool.core.io.FileUtil;
import jline.internal.TestAccessible;
import org.junit.Test;
import org.mortbay.util.UrlEncoded;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class random_flie {
    public static void main(String[] args) {

    }
    @Test
    //创造原始数据
    public void test1(){
        File file = new File("data/rt.txt");
        File file_out = new File("data/news_test_list.txt");
        List<String> rtList = FileUtil.readUtf8Lines(file);
        ArrayList<String> newsStrs = new ArrayList<>();
        for (int i = 0; i < rtList.size(); i++) {
            String url="www.abc"+i+".com";
            String pt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(new Date(System.currentTimeMillis() - 100000000 + i * 5000L));
            String rt =rtList.get(i);
//            String encode = UrlEncoded.encode(rt);
            StringBuffer sb = new StringBuffer();
            sb.append(url).append("\t")
              .append("zh-cn").append("\t")
              .append(rt).append("\t")
              .append(pt).append("\t");
            newsStrs.add(sb.toString());
        }
        FileUtil.writeLines(newsStrs,file_out,"UTF-8");
    }

    @Test
    public void test2(){
        String myText="www.test.com";
        String resultStr = UUID.nameUUIDFromBytes((myText).getBytes()).toString();
        System.out.println(resultStr);
    }

}

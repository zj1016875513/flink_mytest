package com.zz.flink.single_pass;


import cn.hutool.core.io.FileUtil;
import jline.internal.TestAccessible;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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

    @Test
    public void test3(){}

    @Test
    public void put() throws Exception{

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop162:2181,hadoop163:2181,hadoop164:2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
//        admin = connection.getAdmin();
        //获取table对象
        String rowkey = "536738c6-fb6c-3f4f-81b4-0afee7facd65".replaceAll("-", "");
//        System.out.println(rowkey);

        //创建put对象，并赋rk值
        List<Put> putsList = new ArrayList<>();
        Put put = new Put(rowkey.getBytes());
        // 添加值：f1->列族, order->属性名 如age， 第三个->属性值 如25
        put.addColumn("info".getBytes(), "url".getBytes(), "www.abc202.com".getBytes());
        put.addColumn("info".getBytes(), "lt".getBytes(), "zh-cn".getBytes());
        put.addColumn("info".getBytes(), "rt".getBytes(), "重庆10岁盲童的出行路".getBytes());
        put.addColumn("info".getBytes(), "pt".getBytes(), "2021-08-28 12:41:05".getBytes());
        put.addColumn("info".getBytes(), "uuid".getBytes(), "536738c6-fb6c-3f4f-81b4-0afee7facd65".getBytes());
        put.addColumn("info".getBytes(), "Topicindex".getBytes(), "536738c6-fb6c-3f4f-81b4-0afee7facd65".getBytes());
        put.addColumn("info".getBytes(), "intoTime".getBytes(), "2021-08-29 23:11:19".getBytes());
        putsList.add(put);
        //插入数据
        Table newsTabel = connection.getTable(TableName.valueOf("flink:newssink"));
        newsTabel.put(putsList);
        //关闭
        newsTabel.close();
        connection.close();
    }

}

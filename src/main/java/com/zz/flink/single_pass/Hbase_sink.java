package com.zz.flink.single_pass;

import com.alibaba.fastjson.JSON;
import com.zz.flink.bean.News;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.List;

public class Hbase_sink extends RichSinkFunction<String> {
    private transient Integer maxSize = 1;
    private transient Long delayTime = 5000L;

    public Hbase_sink() {
    }

    public Hbase_sink(Integer maxSize, Long delayTime) {
        this.maxSize = maxSize;
        this.delayTime = delayTime;
    }

    private transient Connection connection;
    private transient Long lastInvokeTime;
    private transient List<Put> putsList = new ArrayList<>();
//    private transient List<Put> putsList = new ArrayList<>(maxSize);

    // 创建连接
    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);

        // 获取全局配置文件，并转为ParameterTool
//        ParameterTool params =
//                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        //创建一个Hbase的连接
        connection = HBaseUtil.getConnection("hadoop162:2181,hadoop163:2181,hadoop164:2181");
//        connection = HBaseUtil.getConnection(
//                "hadoop162:2181,hadoop163:2181,hadoop164:2181",
//                params.getInt("hbase.zookeeper.property.clientPort", 2181)
//        );

        // 获取系统当前时间
        lastInvokeTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        News news = JSON.parseObject(value, News.class);
        String url = news.getUrl();
        String lt = news.getLt();
        String rt = news.getRt();
        String pt = news.getPt();
        String uuid = news.getUuid();
        String Topicindex = news.getTopicindex();
        String intoTime = news.getIntoTime();
        String rowkey = uuid.replaceAll("-", "");

        //创建put对象，并赋rk值
        Put put = new Put(rowkey.getBytes());

        // 添加值：f1->列族, order->属性名 如age， 第三个->属性值 如25
        put.addColumn("info".getBytes(), "url".getBytes(), url.getBytes());
        put.addColumn("info".getBytes(), "lt".getBytes(), lt.getBytes());
        put.addColumn("info".getBytes(), "rt".getBytes(), rt.getBytes());
        put.addColumn("info".getBytes(), "pt".getBytes(), pt.getBytes());
        put.addColumn("info".getBytes(), "uuid".getBytes(), uuid.getBytes());
        put.addColumn("info".getBytes(), "Topicindex".getBytes(), Topicindex.getBytes());
        put.addColumn("info".getBytes(), "intoTime".getBytes(), intoTime.getBytes());

        Table newssink = connection.getTable(TableName.valueOf("flink:newssink"));
        newssink.put(put);
        newssink.close();

//        putsList.add(put);// 添加put对象到list集合
//        //使用ProcessingTime
//        long currentTime = System.currentTimeMillis();
//        //开始批次提交数据
//        if (putsList.size() == maxSize || currentTime - lastInvokeTime >= delayTime) {
//            //获取一个Hbase表
//            Table table = connection.getTable(TableName.valueOf("flink:newssink"));
//            table.put(putsList);//批次提交
//            putsList.clear();
//            lastInvokeTime = currentTime;
//            table.close();
//        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

}
class HBaseUtil {
    /**
     * @param zkQuorum zookeeper地址，多个要用逗号分隔
//     * @param port     zookeeper端口号
     * @return connection
     */
    public static Connection getConnection(String zkQuorum) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",zkQuorum);

//        (String zkQuorum, int port)
//        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", zkQuorum);
//            conf.set("hbase.zookeeper.property.clientPort", port + "");

        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }
}

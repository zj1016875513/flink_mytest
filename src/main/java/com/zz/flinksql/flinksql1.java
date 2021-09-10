package com.zz.flinksql;

import com.zz.flink.bean.News;
import com.zz.flink.bean.sql_News;
import com.zz.flink.single_pass.file_source;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class flinksql1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<sql_News> ds =
                env.addSource(new file_source()).map(new MapFunction<String, sql_News>() {
                    @Override
                    public sql_News map(String value) throws Exception {
                        String[] split = value.split("\t");
                        long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[3]).getTime();
                        return new sql_News(split[0], split[1], split[2],time);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<sql_News>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<sql_News>() {
                                    @Override
                                    public long extractTimestamp(sql_News element, long recordTimestamp) {
                                        return element.getTime();
                                    }
                                }));


        Table table = tenv.fromDataStream(ds,$("url"),$("lt"),$("rt"), $("time").rowtime());
        tenv.sqlQuery("select url,lt,rt from "+table+" group by url,lt,rt").execute().print();//算是在流中去重了

//        tenv.executeSql("select url,lt,rt," +
//                "hop_start(t, interval '5' second, interval '20' second) w_start" +
//                "hop_end(time,interval '5' minute,interval '20' minute) w_end," +
//                " from "+table+" ");
//        table.window(Slide.over(lit(5).millis()).every(lit(2).millis()).on($("time")).as("w"))
//                .groupBy($("url"),$("lt"),$("rt"),$("w"))
//                .select($("url"),$("lt"),$("rt"),$("w").start(),$("w").end())
//                .execute()
//                .print();
//        Table table1 = tenv.sqlQuery("select * from table");
//        table.select($("url"),$("lt"),$("rt")).execute().print();

//        tenv.toAppendStream(table1, Row.class).print();
//        ds.print();
//        env.execute("abc");
    }
}

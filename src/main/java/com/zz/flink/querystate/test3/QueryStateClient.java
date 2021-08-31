package com.zz.flink.querystate.test3;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import java.util.concurrent.CompletableFuture;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/5 1:53 下午
 */
public class QueryStateClient {
    public static void main(String[] args) throws Exception {
        QueryableStateClient client =
                new QueryableStateClient("hadoop162",9069);

        ValueStateDescriptor<Tuple2<String,Long>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Long>>
                        ("keycount",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));

        CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                client.getKvState(JobID.fromHexString("4b8e63a3c0689aa34356c470346b8e1b"),
                        "query-name",
                        "a",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        descriptor);

        System.out.println(resultFuture.get().value());

    }
}

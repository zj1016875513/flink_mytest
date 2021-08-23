package com.zz.flink.querystate.test2;

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
                new QueryableStateClient("hadoop162",9096);

        ValueStateDescriptor<Tuple2<String,Long>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Long>>
                        ("keycount",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));

        CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                client.getKvState(JobID.fromHexString("99b6106e5a830bfd177ab380ce9c0110"),
                        "query-name",
                        "a",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        descriptor);

//        resultFuture.thenAccept(response -> {
//            try {
//                Long count = response.get();
//                // now we could do something with the value
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//
//        resultFuture.get(5, TimeUnit.SECONDS);

        System.out.println(resultFuture.get().value());

    }
}

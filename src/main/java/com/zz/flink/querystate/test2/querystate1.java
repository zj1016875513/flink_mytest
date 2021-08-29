package com.zz.flink.querystate.test2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class querystate1 {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        QueryableStateClient client = new QueryableStateClient(
                "hadoop162", // taskmanager的地址
                9069);// 默认是9069端口，可以在flink-conf.yaml文件中配置

        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<String,Long>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Long>>
                        ("keycount",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));

        while (true) {

            CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                    client.getKvState(JobID.fromHexString("99b6106e5a830bfd177ab380ce9c0110"),
                            "query-name",
                            "a",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            descriptor);
            // now handle the returned value
            resultFuture.thenAccept(response -> {
                try {
                    Tuple2<String, Long> res = response.value();
                    System.out.println("abc");
                    System.out.println("res: " + res);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


            Thread.sleep(100);
        }
    }
}
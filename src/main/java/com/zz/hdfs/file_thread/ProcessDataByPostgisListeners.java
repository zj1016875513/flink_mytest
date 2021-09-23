package com.zz.hdfs.file_thread;

import java.util.List;

/*
 * 很多网上资料没有这个实现类
 * 手动加了个，其实就是初始化父类传个encode
 */
public class ProcessDataByPostgisListeners extends ReaderFileListener{
    public ProcessDataByPostgisListeners(String encode) {
        super.setEncode(encode);
    }

    @Override
    public void output(List<String> stringList) throws Exception {
        // 这个方法记得写 要不nio没有输出
    }
}



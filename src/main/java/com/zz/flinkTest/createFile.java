package com.zz.flinkTest;

import cn.hutool.core.io.FileUtil;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

public class createFile {
    @Test
    public void createfile1(){
        String[] arr={"a","b","c","d","e","f","g","h","i","j"};
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            list.add(arr[new Random().nextInt(arr.length)]+","
                    +arr[new Random().nextInt(arr.length)]+"-"
                    +new Random().nextInt(500));
        }
        File file = new File("src/main/java/com/zz/flinkTest/flinkTest1.txt");
        FileUtil.writeUtf8Lines(list,file);
    }

    @Test
    public void createfile2(){
        String[] arr={"a","b","c","d","e","f","g","h","i","j"};
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            list.add(new Random().nextInt(500)+"");
        }
        File file = new File("src/main/java/com/zz/flinkTrigger/flinkTrigger.txt");
        FileUtil.writeUtf8Lines(list,file);
    }
}

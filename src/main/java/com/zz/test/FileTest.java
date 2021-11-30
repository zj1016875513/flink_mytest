package com.zz.test;

import org.apache.commons.io.FileUtils;
import org.apache.flink.util.MathUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class FileTest {
    @Test
    public void File() throws IOException {
        File file = new File("data/rt.txt");
        List<String> list = FileUtils.readLines(file, "UTF-8");
        for (String s : list) {
            System.out.println(s);
        }
    }

    @Test
    public void t1() throws IOException {
        List<String> list = FileUtils
                .readLines(new File("src/main/java/com/zz/flink/wordCount/words.txt"), StandardCharsets.UTF_8);
        for (String s : list) {

            System.out.println(s);
            int hashCode = s.hashCode();
            System.out.println("hashCode= "+hashCode);
            int murmurHash = MathUtils.murmurHash(hashCode);
            System.out.println("murmurHash= "+murmurHash);
            int keyGroupId = murmurHash % 128;
            System.out.println("keyGroupId = "+keyGroupId);
            int i = keyGroupId * 5 / 128;
            System.out.println(i);
            System.out.println();
        }
    }
}

package com.zz.test;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
}

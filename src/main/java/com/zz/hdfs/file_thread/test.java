package com.zz.hdfs.file_thread;

import org.junit.Test;

import java.io.*;
import java.util.Arrays;

public class test {
    @Test
    public void test0() throws IOException {
        File file = new File("E:/database_test/kfdata/A.csv");
        FileInputStream fis = new FileInputStream(file);
        int available = fis.available();
        byte[] bytes = new byte[7];
//        int read_length = fis.read(bytes);
        int read_length = fis.read(bytes,0,7);
        for (byte ele : bytes) {
            System.out.println((char) ele);
        }

        String s = new String(bytes);
        System.out.println(s);
        System.out.println(read_length);
        System.out.println(available);
    }

    @Test
    public void test1() throws IOException {
        File file = new File("E:/database_test/kfdata/A.csv");
        FileReader fr = new FileReader(file);

        BufferedReader br = new BufferedReader(fr);
//        System.out.println(br.readLine());
        char[] c = new char[100];
        br.read(c,0,10);
        String s1 = new String(c);
        System.out.println(s1);
    }

    @Test
    public void test2() throws IOException {
        String filePath="E:/database_test/kfdata/A.csv";
        RandomAccessFile raf=null;
        File file=null;
        try {
            file=new File(filePath);
            raf=new RandomAccessFile(file,"r");
            // 获取 RandomAccessFile对象文件指针的位置，初始位置为0
//            System.out.println(raf.length());
            System.out.println("输入内容："+raf.getFilePointer());
            //移动文件记录指针的位置
            raf.seek(3);
            System.out.println("输入内容："+raf.getFilePointer());

            byte[] b=new byte[1024];
            int hasRead=0;
            raf.read(b);
//            String s = new String(b);
//            System.out.println(s);
            //循环读取文件
/*            while((hasRead=raf.read(b))>0){
                //输出文件读取的内容
                System.out.print(new String(b,0,hasRead));
            }*/
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            raf.close();
        }
    }
}

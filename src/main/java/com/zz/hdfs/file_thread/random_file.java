package com.zz.hdfs.file_thread;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;

public class random_file {
    public static void main(String[] args) {
        String path="randomtest.txt";
        int seekPointer=20;
        randomRed(path,seekPointer);//读取的方法
        randomWrite(path);//追加写的方法
        //insert(path, 23, "\nlucene是一个优秀的全文检索库");

    }
    public static void randomRed(String path,int pointe){
        try{

            RandomAccessFile raf=new RandomAccessFile(path, "r");
            //获取RandomAccessFile对象文件指针的位置，初始位置是0
            System.out.println("RandomAccessFile文件指针的初始位置:"+raf.getFilePointer());
            raf.seek(pointe);//移动文件指针位置
            byte[]  buff=new byte[1024];
            //用于保存实际读取的字节数
            int hasRead=0;
            //循环读取
            while((hasRead=raf.read(buff))>0){
                //打印读取的内容,并将字节转为字符串输入
                System.out.println(new String(buff,0,hasRead));

            }

        }catch(Exception e){
            e.printStackTrace();
        }

    }
    public static void randomWrite(String path){
        try{
            /**以读写的方式建立一个RandomAccessFile对象**/
            RandomAccessFile raf=new RandomAccessFile(path, "rw");

            //将记录指针移动到文件最后
            raf.seek(raf.length());
            raf.write("我是追加的 \r\n".getBytes());

        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 实现向指定位置
     * 插入数据
     * @param fileName 文件名
     * @param points 指针位置
     * @param insertContent 插入内容
     * **/
    public static void insert(String fileName,long points,String insertContent){
        try{
            File tmp= File.createTempFile("tmp", null);
            tmp.deleteOnExit();//在JVM退出时删除

            RandomAccessFile raf=new RandomAccessFile(fileName, "rw");
            //创建一个临时文件夹来保存插入点后的数据
            FileOutputStream tmpOut=new FileOutputStream(tmp);
            FileInputStream tmpIn=new FileInputStream(tmp);
            raf.seek(points);
            /**将插入点后的内容读入临时文件夹**/

            byte [] buff=new byte[1024];
            //用于保存临时读取的字节数
            int hasRead=0;
            //循环读取插入点后的内容
            while((hasRead=raf.read(buff))>0){
                // 将读取的数据写入临时文件中
                tmpOut.write(buff, 0, hasRead);
            }

            //插入需要指定添加的数据
            raf.seek(points);//返回原来的插入处
            //追加需要追加的内容
            raf.write(insertContent.getBytes());
            //最后追加临时文件中的内容
            while((hasRead=tmpIn.read(buff))>0){
                raf.write(buff,0,hasRead);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}

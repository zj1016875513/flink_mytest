package com.zz.hdfs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class HdfsJavaAPI {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        HdfsJavaAPI o = new HdfsJavaAPI();
        //1.创建
        o.mkdirToHdfs();
        //2.上传
        o.uploadFile();
        //3.下载
        o.downloadFile();
        //4.删除
        o.deleteFile();
        //5.重命名
        o.renameFile();
        //6.查看文件的描述信息
        o.testlistFiles();
        //7.IO流上传
        o.ioPutFile();
        //8.IO流下载
        o.ioGetFile();
        //9.合并小文件
        o.mergeSmallFiles();
    }

    /**
     * 获取HDFS文件系统
     * @return
     * @throws IOException
     */
    public FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop162:8020");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdirToHdfs() throws IOException {
        FileSystem fileSystem = getFileSystem();

        boolean b = fileSystem.mkdirs(new Path("/javaApi/test2"));//若目录已经存在，则创建失败，返回false
        if (b){
            System.out.println("/javaApi/test1创建成功！");
        }else {
            System.out.println("/javaApi/test1可能已存在，创建失败！");
        }
        fileSystem.close();
    }

    /**
     * 文件上传
     * @throws IOException
     */
    @Test
    public void uploadFile() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyFromLocalFile(
                new Path("file:///C:/Users/JACK/Desktop/food.txt"),
                new Path("hdfs://hadoop162:8020/javaApi/test1"));//hdfs路径也可以直接写成/xsluo/dir1
        fileSystem.close();
    }

    /**
     * 文件下载
     * @throws IOException
     */
    public void downloadFile() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/xsluo/a.txt"),new Path("file:///F:\\testDatas\\a2.txt"));
        fileSystem.close();
    }

    /**
     * 文件删除
     * @throws IOException
     */
    public void deleteFile() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.delete(new Path("hdfs://node01:8020/xsluo/b.txt"),true);
        fileSystem.close();
    }

    /**
     * 文件重命名
     * @throws IOException
     */
    public void renameFile() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.rename(new Path("/xsluo/a.txt"),new Path("/xsluo/a_new.txt"));
        fileSystem.close();
    }

    /**
     * 文件相关信息查看
     * @throws IOException
     */
    public void testlistFiles() throws IOException {
        FileSystem fileSystem = getFileSystem();
        //获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/xsluo"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            //输出详情
            System.out.println(status.getPath().getName());//文件名称
            System.out.println(status.getLen());//长度
            System.out.println(status.getPermission());//权限
            System.out.println(status.getOwner());//所属用户
            System.out.println(status.getGroup());//分组
            System.out.println(status.getModificationTime());//修改时间
            //获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                //获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        fileSystem.close();
    }

    //https://www.cnblogs.com/qlqwjy/p/7275874.html
    @Test
    public void listToFile() throws IOException {
        String aa ="\"Angelababy和橙子见面时哭了\\n\" +\n" +
                "                \"国务院调查组进驻郑州\\n\" +\n" +
                "                \"王靖雯我不想比了我好害怕\\n\" +\n" +
                "                \"陈玘念怎么嫁给刘诗雯弹幕\\n\" +\n" +
                "                \"孟佳看披荆斩棘的哥哥电视突然掉了\\n\" +\n" +
                "                \"女乒刘国梁在和不在的区别\\n\" +\n" +
                "                \"张国伟即将重新开始跳高生涯\\n\" +\n" +
                "                \"广西女护士杀害男医生案二审维持死刑\\n\" +\n" +
                "                \"国务院公布调查河南暴雨举报电话\\n\" +\n" +
                "                \"房产中介有望被取消\\n\" +\n" +
                "                \"陈思铭方彬涵拥吻\\n\" +\n" +
                "                \"Bobby结婚\\n\" +\n" +
                "                \"南方的朋友请回避一下\\n\" +\n" +
                "                \"深圳不足超早产儿平安出院\\n\" +\n" +
                "                \"千万不要把百合花放在卧室\\n\" +\n" +
                "                \"福布斯中国名人榜\\n\" +\n" +
                "                \"心动的信号\\n\" +\n" +
                "                \"方陈式cp官宣\\n\" +\n" +
                "                \"广东惠州报告H例\\n\" +\n" +
                "                \"腾讯阿里美团快手蒸发亿\\n\" +\n" +
                "                \"赵露思的干饭勺比雪梨还大\\n\" +\n" +
                "                \"刘聪是赵文卓的关门弟子吧\\n\" +\n" +
                "                \"吴宣仪在台上剪头发\\n\" +\n" +
                "                \"麦穗告白大雄被拒\\n\" +\n" +
                "                \"洪成成告白马子佳被拒\\n\" +\n" +
                "                \"布瑞吉情商\\n\" +\n" +
                "                \"湖北的健康码叫什么\\n\" +\n" +
                "                \"中餐厅\\n\" +\n" +
                "                \"换乘恋爱\\n\" +\n" +
                "                \"景德镇陶瓷大学保安当众打死流浪狗\\n\" +\n" +
                "                \"陈意涵晒劈叉照\\n\" +\n" +
                "                \"邓凯慰罗悦嘉在一起\\n\" +\n" +
                "                \"夏研Rap\\n\" +\n" +
                "                \"王皓 训练的时候抱得动樊振东\\n\" +\n" +
                "                \"被马琳笑死\\n\" +\n" +
                "                \"追尾骂人女司机法拉利是租的\\n\" +\n" +
                "                \"男子戴猪八戒面具开车\\n\" +\n" +
                "                \"周生辰时宜重逢抱\\n\" +\n" +
                "                \"管泽元六杀\\n\" +\n" +
                "                \"华春莹说民主不应是可口可乐\\n\" +\n" +
                "                \"千万别来广东的大学恋爱\\n\" +\n" +
                "                \"阿富汗首位女省长已被塔利班逮捕\\n\" +\n" +
                "                \"家庭里的无效沟通\\n\" +\n" +
                "                \"女子戴手链拔插头爆燃熏黑手掌\\n\" +\n" +
                "                \"数万网友为洗衣机柯基应援发声\\n\" +\n" +
                "                \"错误的医学常识危害有多大\\n\" +\n" +
                "                \"虎牙主播当众被打骨折\\n\" +\n" +
                "                \"上海新增新冠确诊\"";
        ArrayList<String> list = new ArrayList<>();
        list.add("12345");
        list.add("678910");
        list.add("abcde");
        StringBuilder sb = new StringBuilder(aa);
        StringBuilder append = sb.append(sb)
                .append(sb).append(sb).append(sb).append(sb).append(sb).append(sb).append(sb).append(sb).append(sb);

        list.add(append.toString());
        for (String s : list) {
            System.out.println(s);
            System.out.println(s.length());
        }
        FileSystem fileSystem = getFileSystem();

        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://hadoop162:8020/javaApi/test2/123.txt"));
//        new BufferedWriter(fos);
        for (String s : list) {
            byte[] data = s.getBytes();
            int length = data.length;
            fos.write(data,0,length);
            fos.write("\r\n".getBytes());

        }
        fos.flush();
        fos.close();
    }

    /**
     * IO流操作hdfs文件：上传
     * @throws IOException
     */
    @Test
    public void ioPutFile() throws IOException {

        // 1 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 2 创建输入流；路径前不需要加file:///，否则报错
        FileInputStream fis = new FileInputStream(new File("C:/Users/JACK/Desktop/food.txt"));
        // 3 创建输出流
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://hadoop162:8020/javaApi/test2"));
        // 4 流对拷 org.apache.commons.io.IOUtils
        IOUtils.copy(fis,fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fileSystem.close();
    }

    /**
     * IO流操作hdfs文件：下载
     * @throws IOException
     */
    public void ioGetFile() throws IOException {
        // 1 获取文件系统
        FileSystem fileSystem = getFileSystem();
        // 2 创建输入流
        FSDataInputStream fis = fileSystem.open(new Path("/xsluo/a_new.txt"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("F:\\testDatas\\a_new.txt"));
        // 4 流对拷
        IOUtils.copy(fis,fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fileSystem.close();
    }

    /**
     * 小文件合并
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void mergeSmallFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取分布式文件系统hdfs；第三个参数指定hdfs的用户
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "hadoop");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://node01:8020/xsluo/bigfile.txt"));

        //读取所有本地小文件，写入到hdfs的大文件里面去
        //获取本地文件系统 localFileSystem
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //读取本地的小文件们
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("F:\\testDatas"));
        for (FileStatus fileStatus : fileStatuses) {
            //获取每一个本地小文件的路径
            Path path = fileStatus.getPath();
            //读取本地小文件
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);
            IOUtils.copy(fsDataInputStream,fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
        }
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
    }

}


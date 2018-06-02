package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * 通过流的方式来访问hdfs
 * @author Qin
 */
public class StreamAccessTest {
    FileSystem fs = null;

    @Before
    public void init() throws Exception {

        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop:8020"), conf, "qinzhen");
    }

    /**
     * 通过流的方式上传文件到hdfs
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {

        FSDataOutputStream outputStream = fs.create(new Path("/idea重要快捷键.txt"), true);
        FileInputStream inputStream = new FileInputStream("C:/Users/qinzhen/Desktop/idea重要快捷键.txt");

        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * 通过流的方式从hdfs下载文件
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testDownLoadFileToLocal() throws IllegalArgumentException, IOException {

        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/idea重要快捷键.txt"));

        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("D:/idea重要快捷键.txt"));

        //再将输入流中数据传输到输出流
        IOUtils.copy(in, out);
    }

    /**
     * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
     * 用于上层分布式运算框架并发处理数据
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testRandomAccess() throws IllegalArgumentException, IOException{
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/idea重要快捷键.txt"));


        //可以将流的起始偏移量进行自定义
        in.seek(22);

        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("D:/idea重要快捷键.txt"));

        IOUtils.copy(in,out);
    }

    /**
     * 显示hdfs上文件的内容
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCat() throws IllegalArgumentException, IOException{

        FSDataInputStream in = fs.open(new Path("/idea重要快捷键.txt"));

        IOUtils.copy(in, System.out);
    }
}

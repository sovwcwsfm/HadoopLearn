package net.fibonacci.hadoop.hdfs.demo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author LIAO
 * create  2020-12-06 21:11
 * HDFS的操作类
 */
public class HdfsApi {
    public static void main(String[] args) throws Exception {
        HdfsApi hdfsApi = new HdfsApi();
        //hdfsApi.getFileSystem3();
        //hdfsApi.listFiles();
        //hdfsApi.mkdirs();
        //hdfsApi.uploadFile();
        hdfsApi.downFile();
    }

    //方式一：获取FileSystem
    public void getFileSystem1() throws IOException {
        //1、创建 Configuration
        Configuration conf= new Configuration();

        //2、设置文件系统类型
        conf.set("fs.defaultFS","hdfs://hadoop0:8020");

        //3、获取指定文件系统
        FileSystem fileSystem = FileSystem.get(conf);

        //4、打印输出测试
        System.out.println(fileSystem);
    }

    /**
     * 方式二：set方式+通过newInstance
     * @throws IOException
     */
    public void getFileSystem2() throws IOException {
        //1:创建Configuration对象
        Configuration conf = new Configuration();

        //2:设置文件系统类型
        conf.set("fs.defaultFS", "hdfs://hadoop0:8020");

        //3:获取指定文件系统
        FileSystem fileSystem = FileSystem.newInstance(conf);

        //4:输出测试
        System.out.println(fileSystem);
    }

    /**
     * 方式三：new URI+get
     * @throws Exception
     */
    public void getFileSystem3() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }

    /**
     * 方式四：newInstance+get
     * @throws Exception
     */
    public void getFileSystem4() throws Exception{
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop0:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }

    //文件的遍历
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(), "root");

        //2、调用方法
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

        //3、迭代遍历
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            //4、打印输出
            System.out.println(fileStatus.getPath() + "=======" +fileStatus.getPath().getName());
        }
    }

    //创建文件夹
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(), "root");

        //2、创建文件夹
        fileSystem.mkdirs(new Path("/aa2/bb2/cc2"));

        //3、关闭FileSystem
        fileSystem.close();
    }

    //上传文件
    public void uploadFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(), "root");

        //2、上传文件
        fileSystem.copyFromLocalFile(new Path("D://input/test1.txt"),new Path("/aa2/bb2/cc2"));

        //3、关闭FileSystem
        fileSystem.close();
    }

    //下载文件
    public void downFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(), "root");

        //2、获取hdfs的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/aa2/bb2/cc2/test1.txt"));

        //3、获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream("D://test1_down.txt");

        //4、文件的拷贝
        IOUtils.copy(inputStream,outputStream);

        //5、关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

}

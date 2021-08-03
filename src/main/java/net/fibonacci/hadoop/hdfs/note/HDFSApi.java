package net.fibonacci.hadoop.hdfs.note;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2020/12/24 14:29
 * @Description: HDFS 相关操作API 实例
 */
public class HDFSApi {

    private static final String FILE_DEFAULT_NAME = "fs.defaultFS";
    private static final String FILE_VALUE_NAME = "hdfs://hadoop01:8020";
    private static final String FILE_USER = "hadoop";

    public static void main(String[] args) throws Exception {
        HDFSApi testApi = new HDFSApi();
//        testApi.getFileSystem();
//        testApi.getFileSystem2();
//        testApi.getFileSystem3();
//        testApi.getFileSystem4();
        // 遍历文件系统
//        testApi.listFile();
        // 创建文件夹
//        testApi.mkdirs();
        // 上传（复制）文件
//        testApi.uploadFile();
        // 下载文件
//        testApi.downloadFile();
        // 合并本地文件到HDFS
        testApi.mergeFile();
    }

    public void getFileSystem() throws IOException {
        // 获取 Configuration
        Configuration configuration = new Configuration();
        // 设置文件系统类型
        configuration.set(FILE_DEFAULT_NAME, FILE_VALUE_NAME);
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        System.out.println("FileSystem is : ");
        System.out.println(fileSystem);

    }


    /**
     * 方式二:set方式+通过newInstance * @throws IOException
     */
    public void getFileSystem2() throws IOException {
        //1:创建Configuration对象
        Configuration conf = new Configuration();
        //2:设置文件系统类型
        conf.set(FILE_DEFAULT_NAME, FILE_VALUE_NAME);
        //3:获取指定文件系统
        FileSystem fileSystem = FileSystem.newInstance(conf);
        //4:输出测试
        System.out.println("FileSystem is : ");
        System.out.println(fileSystem);
    }

    /**
     * 方式三:new URI+get * @throws Exception */
    public void getFileSystem3() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME),
                new Configuration());

        System.out.println("FileSystem is : ");
        System.out.println(fileSystem);
    }

    /**
     * 方式四:newInstance+get * @throws Exception
     */
    public void getFileSystem4() throws Exception{
        FileSystem fileSystem = FileSystem.newInstance(new
                URI(FILE_VALUE_NAME), new Configuration());

        System.out.println("FileSystem is : ");
        System.out.println(fileSystem);
    }


    /**
     * 遍历文件
     * @throws Exception 异常
     */
    public void listFile() throws Exception {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), "hadoop");

        // 调用方法
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        System.out.println("文件内容：");
        // 遍历文件
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            // 输出
            System.out.println("Path: " + fileStatus.getPath() + " -- Name: " + fileStatus.getPath().getName() );
        }

    }

    /**
     * 创建文件夹
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), "hadoop");
        // 创建文件流
        fileSystem.mkdirs(new Path("/CreateByApi/testDir"));

        fileSystem.close();

    }

    /**
     * 上传文件
     */
    public void uploadFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), "hadoop");
        // 复制文件
        fileSystem.copyFromLocalFile(new Path("/Users/sovwcwsfm/Desktop/wtf/list.txt"), new Path("/CreateByApi/testDir"));

        fileSystem.close();
    }

    /**
     * 下载文件 其实就是通过流把文件从一个地址复制到另外一个地址
     */
    public void downloadFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), "hadoop");
        // 获取文件流 输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/CreateByApi/testDir/list.txt"));
        // 获取保存文件流 输出流
        FileOutputStream outputStream = new FileOutputStream("/Users/sovwcwsfm/Desktop/wtf/listFromHadoop.txt");
        // 保存拷贝 将输入流复制到输出流
        IOUtils.copy(inputStream, outputStream);

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);

        fileSystem.close();

    }

    /**
     * 文件的下载方式一:使用copyToLocalFile * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void downloadFile2() throws URISyntaxException, IOException, InterruptedException { //1:获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), "hadoop"); //2:调用方法，实现文件的下载
        //注意设置false和true两个参数，否则在IDEA中直接运行报错:(null) entry in command string: null chmod 0644
        fileSystem.copyToLocalFile(false,new Path("/CreateByApi/testDir/list.txt"), new
                Path("/Users/sovwcwsfm/Desktop/wtf/listFromHadoop.txt"),true);
        //3:关闭FileSystem
        fileSystem.close();
    }

    /**
     * 小文件的合并上传
     * @throws URISyntaxException * @throws IOException
     * @throws InterruptedException */
    public void mergeFile() throws URISyntaxException, IOException,
            InterruptedException {
        //1:获取FileSystem(分布式文件系统)
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(),"hadoop");
        //2:获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test_merge.txt"));
        //3:获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //4:获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/Users/sovwcwsfm/MyDocument/BigData/TestData/"));
        //5:遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream =
                    localFileSystem.open(fileStatus.getPath());
            //6:将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        //7:关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();

    }

    /**
     * 思路 先遍历文件
     * 然后合并 然后下载
     */
    private void mergeAndDownload() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), FILE_USER);

        // 获取保存文件流 输出流
        FileOutputStream outputStream = new FileOutputStream("/Users/sovwcwsfm/MyDocument/BigData/TestData/day01/mergeDownload.txt");

        // 获取文件目录下的所有文件数据 比上面的少做一步
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/testMergeDownload/"));

        // 遍历文件
        for (FileStatus status : fileStatuses) {
            // 获取小文件输入流
            FSDataInputStream inputStream = fileSystem.open(status.getPath());

            // 将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        // 关闭大文件输出流
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();

    }
}

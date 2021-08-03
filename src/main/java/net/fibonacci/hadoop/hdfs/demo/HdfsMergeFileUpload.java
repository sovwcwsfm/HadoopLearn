package com.liao.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
/**
 * @author LIAO
 * create  2020-12-06 22:04
 * 合并上传
 */
public class HdfsMergeFileUpload {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsMergeFileUpload hdfsMergeFileUpload = new HdfsMergeFileUpload();
        hdfsMergeFileUpload.mergeFile();
    }

    /**
     * 小文件的合并上传
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(),"root");

        //2:获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test_big1206.txt"));

        //3:获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //4:获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D:\\input2"));

        //5:遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            //6:将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        //7:关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

}

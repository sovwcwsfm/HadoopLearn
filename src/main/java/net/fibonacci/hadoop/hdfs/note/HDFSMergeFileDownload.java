package net.fibonacci.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 上传合并文件
 * @Auther: sovwcwsfm
 * @Date: 2020/12/24 21:14
 * @Description:
 */
public class HDFSMergeFileDownload {

    private static final String FILE_VALUE_NAME = "hdfs://hadoop01:8020";
    private static final String FILE_USER = "hadoop";

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HDFSMergeFileDownload download = new HDFSMergeFileDownload();
//        download.mergeAndDownload();
        download.mergeAndDownload2();
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

        // 获取文件目录下的所有文件
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/testMergeDownload/"), true);

        // 获取一个本地文件系统（用于获取客户端的文件用的） 这里不应该用这个
//        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        // 遍历文件
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();

            // DEBUG
            System.out.println(fileStatus.getPath());

            // 用这个会获取不到 因为这玩意取的是本地的。。。 哎西
//            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());


            // 获取小文件输入流
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());

            // 将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        // 关闭大文件输出流
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();

    }


    /**
     * 思路 先遍历文件
     * 然后合并 然后下载
     */
    private void mergeAndDownload2() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(FILE_VALUE_NAME), new Configuration(), FILE_USER);

        // 获取保存文件流 输出流 这里可以用 LocalFileSystem
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

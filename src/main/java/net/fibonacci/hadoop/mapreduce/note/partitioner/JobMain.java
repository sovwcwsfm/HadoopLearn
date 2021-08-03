package net.fibonacci.hadoop.mapreduce.note.partitioner;

import net.fibonacci.hadoop.mapreduce.note.wordcount.WordMapper;
import net.fibonacci.hadoop.mapreduce.note.wordcount.WordReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2020/12/31 21:14
 * @Description: workCount 调用入口 将Map 和 Reduce 串联起来 提供程序的入口
 */
public class JobMain {

    /**
     * 提供程序入口
     * 利用job 管理程序运行参数
     * 指定Mapper
     * 指定Reducer
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建Job
        Job job = Job.getInstance(new Configuration(), "wordCount_Partitioner");

        // 设置Job
            // 设置文件路径 找到数据源
                // 设置读取文件的方式
        job.setInputFormatClass(TextInputFormat.class);
                // 设置文件路径
        TextInputFormat.addInputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/wordCount.txt"));
//        TextInputFormat.addInputPath(job, new Path("hdfs://hadoop01:8020/wcinput/wordcount.txt"));
            // 设置Map 及Map 的k v
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
            // 设置Reduce 及Reduce 的k v
        job.setReducerClass(WordReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
            // 设置Partitioner
        job.setPartitionerClass(MyPartitioner.class);
            // 设置NumReduceTask 各数 不然Partitioner 不会生效 如果 Partitioner只给定了01 但是这里设置了3 那 第三个文件会生成 但是是没有数据的
        job.setNumReduceTasks(2);

            // 设置输出路径 保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/wordCount_partitioner"));
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:8020/wcoutput2")); // 这里是一个文件夹


        // 执行
        boolean isFinish = job.waitForCompletion(true);

        System.out.println(isFinish);
    }

}

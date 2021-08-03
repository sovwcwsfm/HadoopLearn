package net.fibonacci.hadoop.mapreduce.flow.sort;

import net.fibonacci.hadoop.mapreduce.note.sort.MySortBean;
import net.fibonacci.hadoop.mapreduce.note.sort.SortMapper;
import net.fibonacci.hadoop.mapreduce.note.sort.SortReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2020/12/31 21:14
 * @Description: workCount 调用入口 将Map 和 Reduce 串联起来 提供程序的入口
 */
public class FlowSortJobMain {

    /**
     * 提供程序入口
     * 利用job 管理程序运行参数
     * 指定Mapper
     * 指定Reducer
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建Job
        Job job = Job.getInstance(new Configuration(), "sort_test");

        // 设置Job
            // 设置文件路径 找到数据源
                // 设置读取文件的方式
        job.setInputFormatClass(TextInputFormat.class);
                // 设置文件路径
        TextInputFormat.addInputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flowsort"));
//        TextInputFormat.addInputPath(job, new Path("hdfs://hadoop01:8020/wcinput/wordcount.txt"));
            // 设置Map 及Map 的k v
        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
            // 设置Reduce 及Reduce 的k v
        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

            // 设置输出路径 保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flow_sort")); // 这里是一个文件夹
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:8020/wcoutput2")); // 这里是一个文件夹


        // 执行
        boolean isFinish = job.waitForCompletion(true);

        System.out.println(isFinish);
    }

}

package net.fibonacci.hadoop.mapreduce.flow.partitioner;

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
 * @Date: 2021/1/1 21:15
 * @Description:
 */
public class FlowPartitionerJobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 创建Job
        Job job = Job.getInstance(new Configuration(), "flow_partitioner");
        // 设置源文件路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flow.log"));

        // 设置Map 及Map 的k v
        job.setMapperClass(FlowPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(4);

        // 设置Reduce 及Reduce 的k v
        job.setReducerClass(FlowPartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowPartitionerBean.class);

        // 设置输出路径 保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flow_partitioner")); // 这里是一个文件夹
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:8020/wcoutput2")); // 这里是一个文件夹


        // 执行
        boolean isFinish = job.waitForCompletion(true);

        System.out.println(isFinish);

    }

}

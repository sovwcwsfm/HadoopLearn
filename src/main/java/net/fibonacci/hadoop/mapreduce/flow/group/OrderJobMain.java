package net.fibonacci.hadoop.mapreduce.flow.group;

import net.fibonacci.hadoop.mapreduce.flow.partitioner.FlowPartitioner;
import net.fibonacci.hadoop.mapreduce.flow.partitioner.FlowPartitionerBean;
import net.fibonacci.hadoop.mapreduce.flow.partitioner.FlowPartitionerMapper;
import net.fibonacci.hadoop.mapreduce.flow.partitioner.FlowPartitionerReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class OrderJobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 创建Job
        Job job = Job.getInstance(new Configuration(), "order");
        // 设置源文件路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/orders.txt"));

        // 设置Map 及Map 的k v
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setGroupingComparatorClass(OrderGroup.class);
        job.setNumReduceTasks(3);

        // 设置Reduce 及Reduce 的k v
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);

        // 设置输出路径 保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/order")); // 这里是一个文件夹
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:8020/wcoutput2")); // 这里是一个文件夹


        // 执行
        boolean isFinish = job.waitForCompletion(true);

        System.out.println(isFinish);

    }

}

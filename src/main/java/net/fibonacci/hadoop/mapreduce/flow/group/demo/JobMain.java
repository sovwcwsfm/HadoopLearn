package net.fibonacci.hadoop.mapreduce.flow.group.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author LIAO
 * @create 2020-07-29 21:00
 * 求订单最大值的主类
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mygroup");

        //二、配置Job信息
        //1、设置输入信息
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/orders.txt"));

        //2、设置mapper
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        //3 4 5 6  shuffle
        //分区设置
        job.setPartitionerClass(OrderPartition.class);

        //分组设置
        job.setGroupingComparatorClass(OrderGroup.class);

        //7、设置Reducer
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //job.setNumReduceTasks(3);

        //8、设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/order"));

        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}

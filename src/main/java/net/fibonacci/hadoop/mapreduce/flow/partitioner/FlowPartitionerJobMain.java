package net.fibonacci.hadoop.mapreduce.flow.sum;

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
public class FlowSumJobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 创建Job
        Job job = Job.getInstance(new Configuration(), "flow_sum");
        // 设置源文件路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flow.log"));

        // 设置Map 及Map 的k v
        job.setMapperClass(FlowSumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Reduce 及Reduce 的k v
        job.setReducerClass(FlowSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置输出路径 保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/data/flow_sum")); // 这里是一个文件夹
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:8020/wcoutput2")); // 这里是一个文件夹


        // 执行
        boolean isFinish = job.waitForCompletion(true);

        System.out.println(isFinish);

    }

}

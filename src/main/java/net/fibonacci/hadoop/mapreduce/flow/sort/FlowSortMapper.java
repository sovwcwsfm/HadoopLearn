package net.fibonacci.hadoop.mapreduce.flow.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 21:55
 * @Description: 配置输出 key-> FlowSortBean value -> null
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 分割获取 手机号 upFlow
        String[] contents = value.toString().split("\t");

        String mobile = contents[0];
        int upFlow = Integer.parseInt(contents[1]);

        FlowSortBean flowData = new FlowSortBean();

        flowData.setMobile(mobile);
        flowData.setUpFlow(upFlow);

        context.write(flowData, NullWritable.get());
    }
}

package net.fibonacci.hadoop.mapreduce.flow.sum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 21:01
 * @Description:    统计每个手机号的流量
 * 输出key 手机号
 * 输出结果 流量
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 分割数据
        String[] contents = value.toString().split("\t");

        // 手机号
        String mobile = contents[1];

        // 将流量信息存放到bean中
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(contents[6]));
        flowBean.setDownFlow(Integer.parseInt(contents[7]));
        flowBean.setUpCountFlow(Integer.parseInt(contents[8]));
        flowBean.setDownCountFlow(Integer.parseInt(contents[9]));


        // 写入 key -> 手机号 value -> 流量数据
        context.write(new Text(mobile), flowBean);

    }
}

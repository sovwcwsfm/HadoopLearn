package net.fibonacci.hadoop.mapreduce.flow.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 21:10
 * @Description: 将手机号的流量数据汇总
 * 输入手机号 流量数据
 * 输出手机号 统计后的流量数据
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 结果flowBean
        FlowBean sumData = new FlowBean();
        for (FlowBean data: values) {
            // 累加 4个数据
            sumData.doAdd(data);
        }

        // 写出
        context.write(key, sumData);
    }
}

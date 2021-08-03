package net.fibonacci.hadoop.mapreduce.flow.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/2 20:44
 * @Description:
 * 输出 Top 1
 */
public class OrderReducer extends Reducer<OrderBean, Text, Text, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // 因为values 是排序后的内容 所以第一个就是 top1了 其他的可以不管
            context.write(value, NullWritable.get());
            break;
        }
    }
}

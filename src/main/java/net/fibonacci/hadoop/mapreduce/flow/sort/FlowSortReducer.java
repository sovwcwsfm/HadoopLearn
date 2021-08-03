package net.fibonacci.hadoop.mapreduce.flow.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 22:01
 * @Description: 这里没啥逻辑 只是做个输出了
 */
public class FlowSortReducer extends Reducer<FlowSortBean, NullWritable, FlowSortBean, NullWritable> {
    @Override
    protected void reduce(FlowSortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 就输出
        context.write(key, NullWritable.get());
    }
}

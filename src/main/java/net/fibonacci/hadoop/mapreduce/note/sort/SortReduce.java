package net.fibonacci.hadoop.mapreduce.note.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2020/12/31 21:08
 * @Description:
 *
 * <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class SortReduce extends Reducer<MySortBean, NullWritable, MySortBean, NullWritable> {

    /**
     * 合并数据
     * @param key               map 输出的key 进来的key都是一样的
     * @param values            map 输出的value 这里代表单词的个数
     * @param context           上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(MySortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 这里咋进来的 咋 输出
        context.write(key, NullWritable.get());
    }
}

package net.fibonacci.hadoop.mapreduce.note.wordcount;

import org.apache.hadoop.io.LongWritable;
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
public class WordReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 合并数据
     * @param key               map 输出的key 进来的key都是一样的
     * @param values            map 输出的value 这里代表单词的个数
     * @param context           上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // key 出现的次数
        long keyCount = 0;
        // 遍历当前key 下面的所有key 次数 然后统计求和
        for (LongWritable count : values) {
            keyCount += count.get();
        }

        // 输出
        context.write(key, new LongWritable(keyCount));
    }
}

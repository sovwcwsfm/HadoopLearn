package net.fibonacci.hadoop.mapreduce.note.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2020/12/31 20:40
 * @Description: 单词计数Map
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN: 框架读取到的数据集的key类型 对应的key是当前行数据对于本文开头的偏移量
 *     VALUEIN: 框架读取到的数据集的value类型 对应的key是读取到的当前一行数据
 *     KEYOUT: 自定义业务逻辑返回的key的类型
 *     VALUEOUT: 自定义业务逻辑返回的value的类型
 *
 *     为什么 用 LongWritable 不用 Long 因为Long是JDK中的类型，序列化效率较低
 *     hadoop 自己创建了一些的数据类型来处理 高效序列化 为啥要序列化 本地存储网络传输都需要序列化
 *
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private LongWritable one = new LongWritable(1);

    /**
     * 数据处理
     * @param key           一行文本对应文本开头的偏移量
     * @param value         一行文本数据集
     * @param context       上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 单词切分
        String[] worlds = value.toString().split(" ");

        // 计数 生成 <hello 1> 的key value
        for (String world : worlds) {
            // 输出 写入到上下文中
            context.write(new Text(world), one);
        }
    }
}

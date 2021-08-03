package net.fibonacci.hadoop.mapreduce.note.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class SortMapper extends Mapper<LongWritable, Text, MySortBean, NullWritable> {

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
        // 生成 MySortBean 的key
//        MySortBean data = new MySortBean();
//        data.setWord(worlds[0]);
//        data.setCount(Integer.parseInt(worlds[1]));
        // 输出 写入到上下文中  如果这么写但是 MySortBean 没有重载 MySortBean() 会有问题 应该是走了反射 反射创建不出对象了
        context.write(new MySortBean(worlds[0], Integer.parseInt(worlds[1])), NullWritable.get());
    }
}

package net.fibonacci.hadoop.mapreduce.note.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 18:51
 * @Description: 自定义分区
 *
 * Partitioner<KEY, VALUE>
 *     KEY      输入数据KEY 数据类型    这里指单词  数据类型
 *     VALUE    输入数据VALUE 数据类型  这里指单词数量  数据类型
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        // 自定义的分区逻辑
        // 这里通过key的长度来区分 >= 5的放在一个结果文件 < 5 的放在一个结果文件
        if (text.getLength() >= 5) {
            return 0;
        }else {
            return 1;
        }

    }
}

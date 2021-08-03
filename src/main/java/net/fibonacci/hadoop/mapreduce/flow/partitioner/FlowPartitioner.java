package net.fibonacci.hadoop.mapreduce.flow.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 22:23
 * @Description: 分区
 */
public class FlowPartitioner extends Partitioner<Text, FlowPartitionerBean> {
    @Override
    public int getPartition(Text text, FlowPartitionerBean flowPartitionerBean, int numPartitions) {
        if (text.toString().startsWith("135")) {
            return 0;
        }else if (text.toString().startsWith("136")) {
            return 1;
        }else if (text.toString().startsWith("137")) {
            return 2;
        }
        return 3;
    }
}

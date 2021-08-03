package net.fibonacci.hadoop.mapreduce.flow.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/2 20:17
 * @Description:
 * 数据分区 订单分开
 */
public class OrderPartitioner extends Partitioner<OrderBean, Text> {

    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        // 通过订单号的hash 值 和 当前的ReduceTask数量 取模
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

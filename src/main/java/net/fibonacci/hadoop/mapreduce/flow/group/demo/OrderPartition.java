package com.naixue.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author LIAO
 * @create 2020-07-29 20:42
 * Partitioner<KEY, VALUE>
 *     KEY:k2
 *     VALUE:v2
 */
public class OrderPartition extends Partitioner<OrderBean,Text> {

    /**
     *
     * @param orderBean k2
     * @param text v2
     * @param numPartitions ReduceTask的个数
     * @return  返回的是分区的编号：比如说：ReduceTask的个数3个，返回的编号是 0 1 2
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        //参考源码  return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

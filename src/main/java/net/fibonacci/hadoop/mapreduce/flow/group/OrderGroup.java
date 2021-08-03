package net.fibonacci.hadoop.mapreduce.flow.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/2 20:24
 * @Description: 将订单分组
 *
 * * 实现分组有固定的步骤：
 * 		 * 1、继承WritableComparator
 * 		 * 2、调用父类的构造器
 * 		 * 3、指定分组的规则，重写一个方法
 */
public class OrderGroup extends WritableComparator {

    public OrderGroup() {
        // 指定分组的使用的bean
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 指定分组规则
        OrderBean orderA = (OrderBean) a;
        OrderBean orderB = (OrderBean) b;


        return orderA.getOrderId().compareTo(orderB.getOrderId());
    }
}

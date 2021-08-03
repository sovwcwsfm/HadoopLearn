package net.fibonacci.hadoop.mapreduce.flow.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/2 20:05
 * @Description:
 *  这里主要的功能就是把数据抽出来 放到自己的封装的OrderBean 中
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] orderInfos = value.toString().split("\t");

        // 数据封装到OrderBean 中
        OrderBean order = new OrderBean();
//        order.setOrderId("123456");
        order.setOrderId(orderInfos[0]);
//        order.setPrice(122.3);
        order.setPrice(Double.parseDouble(orderInfos[2]));

        context.write(order, value);
    }
}

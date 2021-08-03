package net.fibonacci.hadoop.mapreduce.flow.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/2 19:49
 * @Description: 订单数据类型
 * 排序按最大金额倒排
 */
public class OrderBean implements WritableComparable<OrderBean> {
    // 订单编号
    private String orderId;
    // 价格
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", price=" + price +
                '}';
    }

    @Override
    public int compareTo(OrderBean o) {
        int res = orderId.compareTo(o.getOrderId());
        if (0 == res) {
            return o.price.compareTo(price);
        }
        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        price = dataInput.readDouble();
    }
}

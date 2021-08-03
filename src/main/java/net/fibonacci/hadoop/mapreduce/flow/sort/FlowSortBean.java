package net.fibonacci.hadoop.mapreduce.flow.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 21:43
 * @Description: 按upFlow 倒排
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String mobile;
    private int upFlow;

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    @Override
    public String toString() {
        return "FlowSortBean{" +
                "mobile='" + mobile + '\'' +
                ", upFlow=" + upFlow +
                '}';
    }

    @Override
    public int compareTo(FlowSortBean o) {
        // 倒序大的在前
        return o.upFlow - this.upFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mobile);
        dataOutput.writeInt(upFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mobile = dataInput.readUTF();
        upFlow = dataInput.readInt();
    }
}

package net.fibonacci.hadoop.mapreduce.flow.sum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 20:54
 * @Description: 流量求和实体类
 */
public class FlowBean implements Writable {

    private int upFlow;             // 上行数据包
    private int downFlow;           // 下行数据包
    private int upCountFlow;        // 上行流量
    private int downCountFlow;      // 下行流量

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(int upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public int getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(int downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", upCountFlow=" + upCountFlow +
                ", downCountFlow=" + downCountFlow +
                '}';
    }

    public void doAdd(FlowBean data) {
        this.upFlow += data.getUpFlow();
        this.downFlow += data.getDownFlow();
        this.upCountFlow += data.getUpCountFlow();
        this.downCountFlow += data.getDownCountFlow();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readInt();
        downFlow = dataInput.readInt();
        upCountFlow = dataInput.readInt();
        downCountFlow = dataInput.readInt();
    }
}

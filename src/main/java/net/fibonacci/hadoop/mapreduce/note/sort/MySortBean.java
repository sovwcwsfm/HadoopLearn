package net.fibonacci.hadoop.mapreduce.note.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/1/1 19:30
 * @Description:
 */
public class MySortBean implements WritableComparable<MySortBean> {

    private String word;
    private int count;

    // TODO 如果是通过重载构造函数创建的类型 必须要有默认的构造器
    public MySortBean(String word, int count) {
        this.word = word;
        this.count = count;
    }

    // 这个必须要有
    public MySortBean() {

    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        // 这里 的内容 就是最终写到文件里面的内容
        return "MySortBean{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

    /**
     * 比较器
     * @param o
     * @return
     */
    @Override
    public int compareTo(MySortBean o) {
        // 第一列按照字典顺序进行排列，第一列相同的时候, 第二列按照升序进行排列

        // 判断第一列
        int firstResult = word.compareTo(o.getWord());

        // 第一列相同
        if (0 == firstResult) {
            // 判断第二列
            return count - o.count;
        }

        return firstResult;
    }

    /**
     * 实现序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(count);
    }

    /**
     * 实现反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.count = dataInput.readInt();
    }
}

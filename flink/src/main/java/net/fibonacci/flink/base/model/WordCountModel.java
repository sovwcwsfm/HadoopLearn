package net.fibonacci.flink.base.model;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/19 20:19
 * @Description: 单词计数模型
 */
public class WordCountModel {
    private String word;
    private int count;
    private long eventTime;

    public WordCountModel() {
    }

    public WordCountModel(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "WordCountModel{" +
                "count=" + count +
                ", word=" + word +
                '}';
    }
}

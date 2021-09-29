package net.fibonacci.flink.window.project.hot.model;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 17:12
 * @Description: 统计结果
 */
public class ResultBean implements Comparable<ResultBean>{
    private long count;
    private String path;
    private long windowTime;

    public ResultBean(long count, String path, long windowTime) {
        this.count = count;
        this.path = path;
        this.windowTime = windowTime;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(long windowTime) {
        this.windowTime = windowTime;
    }

    @Override
    public String toString() {
        return "ResultBean{" +
                "count=" + count +
                ", path='" + path + '\'' +
                ", windowTime=" + windowTime +
                '}';
    }

    @Override
    public int compareTo(ResultBean resultBean) {
        return Long.compare(resultBean.count, this.count);
    }
}

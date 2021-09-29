package net.fibonacci.flink.window.project.hot.model;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 15:28
 * @Description: 日志数据模型封装
 */
public class LogBean {

    private String ip;
    private String pageUrl;
    private Long eventTime;  // 单位 s
    private String method;
    private int count = 1;

    public LogBean() {
    }

    public LogBean(String ip, String pageUrl, Long eventTime, String method) {
        this.ip = ip;
        this.pageUrl = pageUrl;
        this.eventTime = eventTime;
        this.method = method;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "ip='" + ip + '\'' +
                ", pageUrl='" + pageUrl + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", count=" + count +
                '}';
    }
}

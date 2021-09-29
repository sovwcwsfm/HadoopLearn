package net.fibonacci.flink.window.project.hot.strategy;

import net.fibonacci.flink.window.project.hot.model.LogBean;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 17:03
 * @Description: 制定EventTime字段
 */
public class TimeStampExtractor implements TimestampAssigner<LogBean> {
    @Override
    public long extractTimestamp(LogBean logBean, long l) {
        return logBean.getEventTime();
    }
}

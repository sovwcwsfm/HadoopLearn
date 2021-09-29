package net.fibonacci.flink.window.project.hot.strategy;

import net.fibonacci.flink.window.project.hot.model.LogBean;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.io.Serializable;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 16:56
 * @Description:
 */
public class PeriodicWatermarkGenerator implements WatermarkGenerator<LogBean> , Serializable {

    private long currentMaxEventTime = 0L;
    private final long maxOutOfOrder = 10L; // 最大允许的乱序时间 10 秒

    // 每个事件进入都会触发
    @Override
    public void onEvent(LogBean logBean, long l, WatermarkOutput watermarkOutput) {
        // 指定当前的EventTime 要进入到那个水位中
        currentMaxEventTime = Math.max(logBean.getEventTime(), currentMaxEventTime);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        // 设定waterMark 单位 ms
        watermarkOutput.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrder) * 1000));
    }
}

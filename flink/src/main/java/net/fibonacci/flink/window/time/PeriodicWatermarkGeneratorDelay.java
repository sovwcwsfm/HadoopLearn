package net.fibonacci.flink.window.time;

import net.fibonacci.flink.base.model.WordCountModel;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/19 17:30
 * @Description:
 */
public class PeriodicWatermarkGenerator implements WatermarkGenerator<WordCountModel> {
    @Override
    public void onEvent(WordCountModel wordCountModel, long l, WatermarkOutput watermarkOutput) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
    }
}

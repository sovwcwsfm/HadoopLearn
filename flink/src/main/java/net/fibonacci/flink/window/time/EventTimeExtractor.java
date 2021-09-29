package net.fibonacci.flink.window.time;

import net.fibonacci.flink.base.model.WordCountModel;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/19 17:27
 * @Description: 指定EventTime字段
 */
public class EventTimeExtractor implements TimestampAssigner<WordCountModel> {

    @Override
    public long extractTimestamp(WordCountModel wordCountModel, long l) {
        return wordCountModel.getEventTime();
    }
}

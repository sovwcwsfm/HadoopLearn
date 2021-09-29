package net.fibonacci.flink.window.project.hot.aggregate;

import net.fibonacci.flink.window.project.hot.model.ResultBean;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 17:11
 * @Description:
 */
public class PageWindowResult implements WindowFunction<Long, ResultBean, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, java.lang.Iterable<Long> input, Collector<ResultBean> out) throws Exception {
        // 前面是按照path分组的 所以这里的key就是path
        out.collect(new ResultBean(input.iterator().next(), key, window.getEnd()));
    }
}

package net.fibonacci.flink.window.project.hot.aggregate;

import net.fibonacci.flink.window.project.hot.model.LogBean;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 17:08
 * @Description: page求和
 */
public class PageCountAgg implements AggregateFunction<LogBean, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LogBean logBean, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

package net.fibonacci.flink.window.fun;

import net.fibonacci.flink.base.model.WordCountModel;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/18 14:51
 * @Description: 代替sum
 *
 * IN, OUT, KEY, W
 * IN：输入的数据类型
 * OUT：输出的数据类型
 * Key：key的数据类型（在Flink里面，String用Tuple表示）
 * W：Window的数据类型
 */
public class SumProcessWindowFunction extends ProcessWindowFunction<WordCountModel, WordCountModel, String, TimeWindow> {

    private final FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");

    /**
     * 窗口触发 会触发该方法
     *
     * @param key      key
     * @param context  上下文
     * @param elements 窗口内数据
     * @param out      输出
     * @throws Exception 异常
     */
    @Override
    public void process(String key, Context context, Iterable<WordCountModel> elements, Collector<WordCountModel> out) throws Exception {
        // 展示窗口时间

        System.out.println("----------------------start-------------------------");

        System.out.println("当前窗口时间: " + dataFormat.format(System.currentTimeMillis()));
        System.out.println("窗口处理时间: " + dataFormat.format(context.currentProcessingTime()));

        System.out.println("窗口开始时间: " + dataFormat.format(context.window().getStart()));
        System.out.println("窗口结束时间: " + dataFormat.format(context.window().getEnd()));

        System.out.println("----------------------end---------------------------");

        // 代替sum 求和
        int count = 0;
        for (WordCountModel word : elements) {
            // 这里因为之前已经keyBy过了 所以进来的都是同一个key 直接 ++ 就好
            count++;
        }

        out.collect(new WordCountModel(key, count));
    }
}

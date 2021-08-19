package net.fibonacci.flink.base.souce;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/19 15:57
 * @Description: 自定义source
 * 模拟：第 13 秒的时候连续发送 2 个事件，第 16 秒的时候再发送 1 个事件
 */
public class TestSource implements SourceFunction<String> {

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 控制大约在 10 秒的倍数的时间点发送事件
        String currTime = String.valueOf(System.currentTimeMillis());
        while (Integer.parseInt(currTime.substring(currTime.length() - 4)) >
                100) {
            currTime = String.valueOf(System.currentTimeMillis());
        }
        System.out.println("开始发送事件的时间：" +
                dateFormat.format(System.currentTimeMillis()));
        // 第 13 秒发送两个事件
        TimeUnit.SECONDS.sleep(13);
        ctx.collect("hadoop");
        // 产生了一个事件，但是由于网络原因，事件没有发送
        ctx.collect("hadoop");
        // 第 16 秒发送一个事件
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("hadoop");
        TimeUnit.SECONDS.sleep(300);
    }

    @Override
    public void cancel() {

    }
}

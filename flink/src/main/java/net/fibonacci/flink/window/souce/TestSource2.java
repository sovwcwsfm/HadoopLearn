package net.fibonacci.flink.window.souce;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/19 15:57
 * @Description: 自定义source
 * 模拟：正常情况下第 13 秒的时候连续发送 2 个事件，但是有一个事件确实在第13秒的
 * 发送出去了，另外一个事件因为某种原因在19秒的时候才发送出去，第 16 秒的时候再发送 1 个事件
 */
public class TestSource2 implements SourceFunction<String> {

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 控制大约在 10 秒的倍数的时间点发送事件
        long currTime = System.currentTimeMillis() / 1000;
        while (currTime % 10 != 0) {
            currTime = System.currentTimeMillis() / 1000;
        }
        System.out.println("开始发送事件的时间：" +
                dateFormat.format(System.currentTimeMillis()));
        // 第 13 秒发送两个事件
        TimeUnit.SECONDS.sleep(13);
        String eventDelay = "hadoop," + System.currentTimeMillis();
        ctx.collect(eventDelay);
        // 第 16 秒发送一个事件
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("hadoop," + System.currentTimeMillis());

        TimeUnit.SECONDS.sleep(3);
        // 产生了一个事件，但是由于网络原因，事件没有发送 到第19s才发送了 13s时候的数据
        ctx.collect(eventDelay);
        TimeUnit.SECONDS.sleep(300);
    }

    @Override
    public void cancel() {

    }
}

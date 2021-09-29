package net.fibonacci.flink.window.project.hot.map;

import net.fibonacci.flink.window.project.hot.model.LogBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 16:46
 * @Description: 日志数据转换
 * 83.149.9.123 - - 17/05/2020:10:05:03 +0000 GET /presentations/logstash-kafkamonitor-2020/images/kibana-search.png
 */
public class HotPageLogMap implements MapFunction<String, LogBean> {
    @Override
    public LogBean map(String s) throws Exception {
        String[] logInfo = s.split(" ");

        SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        long timeStamp = format.parse(logInfo[3].trim()).getTime();

        return new LogBean(logInfo[0], logInfo[6], timeStamp, logInfo[5]);
    }
}

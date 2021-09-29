package net.fibonacci.flink.window.project.hot.process;

import com.google.common.collect.Lists;
import net.fibonacci.flink.window.project.hot.model.LogBean;
import net.fibonacci.flink.window.project.hot.model.ResultBean;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 17:24
 * @Description:
 */
public class TopNHotPage2 extends KeyedProcessFunction<String, LogBean, String> {

    private int topN = 0;

    public TopNHotPage2(int topN) {
        this.topN = topN;
    }

    public int getTopN() {
        return topN;
    }

    public void setTopN(Integer topN) {
        this.topN = topN;
    }

    //key:url
    //value:count 出现的次数
    public MapState<String,Long> urlState;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        MapStateDescriptor<String,Long> descriptor =
                new MapStateDescriptor<>(
                        "average",  // 状态的名字
                        String.class, Long.class); // 状态存储的数据类型
        urlState = getRuntimeContext().getMapState(descriptor);

    }

    @Override
    public void processElement(LogBean urlView,
                               Context context,
                               Collector<String> out) throws Exception {

        urlState.put(urlView.getPageUrl(),(long)urlView.getCount());

        context.timerService().registerEventTimeTimer(urlView.getEventTime() + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ResultBean> urlViewArrayList = new ArrayList<>();

        List<String> allElementKey = Lists.newArrayList(urlState.keys());
        for(String url:allElementKey){
            urlViewArrayList.add(new ResultBean(urlState.get(url), url, new Timestamp(timestamp - 1).getTime()));
        }
        Collections.sort(urlViewArrayList);

        List<ResultBean> topN;
        if (urlViewArrayList.size() > this.topN) {
            topN = urlViewArrayList.subList(0,this.topN);
        }else {
            topN = urlViewArrayList;
        }


        for (ResultBean urlView:topN){
            System.out.println(urlView);
        }
        System.out.println("======================");


    }
}

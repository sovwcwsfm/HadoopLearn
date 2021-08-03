package net.fibonacci.flink.state.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/30 11:28
 * @Description:
 * MapState<K, V> 每个key保存一个Map集合</>
 */
public class CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    private MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 绑定状态
        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("average", Types.STRING, Types.LONG);

        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Double>> out) throws Exception {
        if (!mapState.contains("count")) {
            mapState.put("count", 0L);
            mapState.put("value", 0L);
        }

        mapState.put("count", mapState.get("count") + 1);
        mapState.put("value", mapState.get("value") + input.f1);

        if (mapState.get("count") == 3) {

            // 计算平均
            long count = mapState.get("count");
            long sum = mapState.get("value");

            double avg = (double) sum/count;

            out.collect(Tuple2.of(input.f0, avg));
        }
    }
}

package net.fibonacci.flink.state.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/30 10:16
 * @Description:  按key分组 同个key 每3次 对值取平均输出
 *                ValueState<T> 每个状态对应一个KEY 保存一个值</>
 *                value() 获取状态值
 *                update() 更新状态值
 *                clear() 清楚状态值
 */
public class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    // 保存key 对应的状态值  Tuple2 key = key出现的次数 value = key对应的value的总和
    // 关键点 同个key 会进入到同个 valueState中
    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",          // 状态的名称
                Types.TUPLE(Types.LONG, Types.LONG));       // 状态存储的数据类型 这里为啥要定义类型？不能通过泛型拿到嘛？

        // 通过当前运行上下文获取状态
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Double>> out) throws Exception {
        // 获取状态值
        Tuple2<Long, Long> currentState = countAndSum.value();

        if (null == currentState) {
            // 给定一个初始值
            currentState = Tuple2.of(0L, 0L);
        }

        // 更新数量
        currentState.f0 += 1;

        // 累加值
        currentState.f1 += input.f1;

        // 统计数据
        if (currentState.f0 == 3) {
            // 数量累计到3 计算平均 并输出
            double avg = (double) (currentState.f1 / currentState.f0);
            // 清空state中状态 重新开始计数
            countAndSum.clear();
            // 结果输出到下游
            out.collect(Tuple2.of(input.f0, avg));
        }else {
            // 未满3条 更新valueState
            countAndSum.update(currentState);
        }


    }
}

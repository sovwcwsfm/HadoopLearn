package net.fibonacci.flink.base.map;

import net.fibonacci.flink.base.model.WordCountModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/19 20:18
 * @Description: 单词计数逻辑
 */
public class WordSplitTask implements FlatMapFunction<String, WordCountModel> {
    @Override
    public void flatMap(String line, Collector<WordCountModel> out) {
        // 这里处理输入的数据 这里的原数据格式 hadoop spark hello hadoop 单行按空格切分
        String[] fields = line.split(" ");

        for (String word:
             fields) {
            // 输出到下游
            out.collect(new WordCountModel(word, 1));
        }
    }
}

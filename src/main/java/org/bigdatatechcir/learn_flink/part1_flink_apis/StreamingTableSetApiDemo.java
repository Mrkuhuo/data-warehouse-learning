package org.bigdatatechcir.learn_flink.part1_flink_apis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bigdatatechcir.learn_flink.util.WordCountData;

public class StreamingTableSetApiDemo {
    /**
     * 主函数，执行WordCount示例程序。
     * @param args 命令行参数，通过CLI库解析为程序参数。
     * @throws Exception 如果程序执行过程中遇到异常。
     */
    public static void main(String[] args) throws Exception {
        // 获取流执行环境。
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从内存中读取单词数据流。
        DataStream<String> text = env.fromElements(WordCountData.WORDS).name("in-memory-input");

        // 对文本流进行分词，然后按单词键进行聚合，计算每个单词出现的次数。
        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .name("tokenizer")
                        .keyBy(value -> value.f0)
                        .sum(1)
                        .name("counter");

        // 打印单词计数结果。
        counts.print().name("print-sink");

        // 启动作业执行。
        env.execute("WordCount");
    }

    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        /**
         * 对输入的字符串进行分词，并为每个词生成一个计数为1的元组。
         * 这是flatMap函数的实现，用于MapReduce模型中的映射阶段。
         *
         * @param value 输入的字符串，通常是从上游操作符接收的数据。
         * @param out Collector对象，用于收集和输出处理后的数据。
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将输入字符串转换为小写并按非单词字符分割，以进行分词
            String[] tokens = value.toLowerCase().split("\\W+");

            // 遍历分割后的每个词
            for (String token : tokens) {
                // 如果词的长度大于0，说明是一个有效的词
                if (token.length() > 0) {
                    // 输出每个词和计数1的元组
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}

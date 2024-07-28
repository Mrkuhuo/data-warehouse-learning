package org.bigdatatechcir.learn.flink.local;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowingDemo {
    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 获取默认的流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局作业参数
        env.getConfig().setGlobalJobParameters(params);
        // 为了简化，将作业并行度设置为1
        env.setParallelism(1);

        // 手动定义输入数据
        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
        // 向列表中添加数据
        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        input.add(new Tuple3<>("a", 10L, 1));
        input.add(new Tuple3<>("c", 11L, 1));

        // 根据预定义的输入数据创建数据生成器函数
        GeneratorFunction<Long, Tuple3<String, Long, Integer>> dataGenerator =
                index -> input.get(index.intValue());
        // 使用数据生成器创建自定义数据源
        DataGeneratorSource<Tuple3<String, Long, Integer>> generatorSource =
                new DataGeneratorSource<>(
                        dataGenerator,
                        input.size(),
                        TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}));

        // 从自定义源定义数据流
        DataStream<Tuple3<String, Long, Integer>> source =
                env.fromSource(
                        generatorSource,
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                // 指定事件时间戳是第二个字段
                                .withTimestampAssigner((event, timestamp) -> event.f1),
                        "自定义源定义数据流");

        // 按第一个字段对数据进行分组，并应用一个3毫秒间隔的会话窗口，对第三个字段执行求和聚合
        DataStream<Tuple3<String, Long, Integer>> aggregated =
                source.keyBy(value -> value.f0)
                        .window(EventTimeSessionWindows.withGap(Time.milliseconds(3)))
                        .sum(2);

        // 直接打印结果
        aggregated.print();

        // 执行流处理作业
        env.execute();
    }
}

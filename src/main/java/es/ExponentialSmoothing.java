/*
 * Copyright (c) 2024. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package es;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExponentialSmoothing {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度为 1(输出就不会出现18>)
        env.setParallelism(1);


        // 模拟数据源，可以替换为实际的数据源
        DataStream<Double> dataStream = env.fromElements(20.0, 21.0, 19.0, 18.0, 20.0);

        // 从CSV文件读取数据
        //DataStream<String> textStream = env.readTextFile("./es.csv");

        // 将字符串数据转换为Double类型
        //DataStream<Double> dataStream = textStream.map(Double::parseDouble);

        // 设置指数平滑的参数 alpha
        double alpha = 0.5;

        // 对数据流应用指数平滑函数
        DataStream<Double> smoothedStream = dataStream
                .keyBy(value -> 0) // 使用 keyBy 保证状态的正确性
                .map(new ExponentialSmoothingModel.ExponentialSmoothingFunction(alpha));

        // 打印结果
        smoothedStream.print();

        // 执行任务
        env.execute("Exponential Smoothing Job");
    }
}

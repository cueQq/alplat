package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Apriori {
    public static void main(String[] args) throws Exception {
        // 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 假设我们从文件读取事务数据
        System.out.println("Reading transaction data from file...");

        DataStream<String> transactions = env.readTextFile("./transaction_data.csv");
        // 1. 预处理数据：将事务数据转换为单个项集的列表
        DataStream<List<String>> itemsets = transactions
                .map((MapFunction<String, List<String>>) value -> {
                    String[] items = value.split(", ");
                    return Arrays.asList(items);
                })
                .returns(new TypeHint<List<String>>() {});  // 显式指定返回类型

        transactions.print();
        System.out.println("Preprocessing done.");

        // 2. 生成1项集并计算支持度
        DataStream<Tuple2<String, Integer>> frequentItemsets = itemsets
                .flatMap((FlatMapFunction<List<String>, Tuple2<String, Integer>>) (transaction, out) -> {
                    // 生成1项集
                    for (String item : transaction) {
                        out.collect(Tuple2.of(item, 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {})  // 显式指定返回类型
                .keyBy(value -> value.f0) // 按照项的名称分组
                .sum(1) // 计算每项的出现次数
                .filter(item -> item.f1 >= 2);  // 设定最小支持度为2

        // 打印频繁1项集
        frequentItemsets.print();

        // 3. 迭代生成k项集，直到无法生成新的频繁项集
        int k = 2;
        DataStream<Tuple2<String, Integer>> currentFrequentItemsets = frequentItemsets;

        while (true) {
            // 生成候选k项集（例如，2项集、3项集等）
            DataStream<Tuple2<String, Integer>> candidateKItemsets = generateCandidates(itemsets, k);

            // 计算候选k项集的支持度
            DataStream<Tuple2<String, Integer>> candidateKCounts = candidateKItemsets
                    .keyBy(value -> value.f0)
                    .sum(1);

            // 筛选出频繁k项集
            currentFrequentItemsets = candidateKCounts
                    .filter(item -> item.f1 >= 2);  // 设定最小支持度为2

            if (currentFrequentItemsets == null) {
                break;  // 如果没有频繁项集，停止迭代
            }

            // 打印当前频繁项集
            currentFrequentItemsets.print();

            k++;
        }

        env.execute("Apriori Algorithm with Flink");
    }

    // 生成候选k项集
    public static DataStream<Tuple2<String, Integer>> generateCandidates(DataStream<List<String>> itemsets, int k) {
        return itemsets
                .flatMap((FlatMapFunction<List<String>, Tuple2<String, Integer>>) (transaction, out) -> {
                    if (transaction.size() < k) return;

                    List<List<String>> combinations = combine(transaction, k);
                    for (List<String> comb : combinations) {
                        out.collect(Tuple2.of(String.join(",", comb), 1));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
    }

    // 生成指定长度k的所有组合
    public static List<List<String>> combine(List<String> items, int k) {
        List<List<String>> combinations = new ArrayList<>();
        int n = items.size();
        if (k > n) return combinations;

        // 使用递归算法生成组合
        combineHelper(items, k, 0, new ArrayList<>(), combinations);
        return combinations;
    }

    // 递归生成组合
    public static void combineHelper(List<String> items, int k, int start, List<String> current, List<List<String>> combinations) {
        if (current.size() == k) {
            combinations.add(new ArrayList<>(current));
            return;
        }

        for (int i = start; i < items.size(); i++) {
            current.add(items.get(i));
            combineHelper(items, k, i + 1, current, combinations);
            current.remove(current.size() - 1);
        }
    }
}

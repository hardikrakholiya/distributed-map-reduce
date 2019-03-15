package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.CombinerFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WCCombinerFunc implements CombinerFunc, Serializable {
    @Override
    public List<KV> combine(List<KV> word1Pairs) {
        Map<String, Integer> wordCountMap = new HashMap<>();

        for (KV word1 : word1Pairs) {
            String word = word1.getKey();
            if (!wordCountMap.containsKey(word)) {
                wordCountMap.put(word, 1);
            } else {
                wordCountMap.put(word, wordCountMap.get(word) + 1);
            }
        }

        return wordCountMap.entrySet().stream()
                .map(stringIntegerEntry -> new KV(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

}

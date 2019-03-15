package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.ReducerFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WCReducerFunc implements ReducerFunc, Serializable {
    @Override
    public List<KV> reduce(List<KV> listOfKVPair) {
        Map<String, Integer> wordCountMap = new HashMap<>();

        for (KV wordCount : listOfKVPair) {
            String word = wordCount.getKey();
            int count = (int) wordCount.getValue();

            if (!wordCountMap.containsKey(word)) {
                wordCountMap.put(word, count);
            } else {
                wordCountMap.put(word, wordCountMap.get(word) + count);
            }
        }

        return wordCountMap.entrySet().stream()
                .map(stringIntegerEntry -> new KV(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}

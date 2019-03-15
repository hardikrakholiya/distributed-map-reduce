package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.ReducerFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IIReducerFunc implements ReducerFunc, Serializable {
    @Override
    public List<KV> reduce(List<KV> listOfKVPair) {

        Map<String, Map<String, Integer>> wordDocumentCountsMap = new HashMap<>();

        for (KV kv : listOfKVPair) {
            String word = kv.getKey();
            if (!wordDocumentCountsMap.containsKey(word)) {
                wordDocumentCountsMap.put(word, new HashMap<>());
            }

            //noinspection unchecked
            ((Map<String, Integer>) kv.getValue()).forEach((doc, count) -> {
                Map<String, Integer> documentCountMap = wordDocumentCountsMap.get(word);
                if (!documentCountMap.containsKey(doc)) {
                    documentCountMap.put(doc, 0);
                }
                documentCountMap.put(doc, documentCountMap.get(doc) + count);
            });
        }

        return wordDocumentCountsMap.entrySet().stream()
                .map(kv -> new KV(kv.getKey(), kv.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}

package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.CombinerFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IICombinerFunc implements CombinerFunc, Serializable {

    @Override
    public List<KV> combine(List<KV> kvList) {

        Map<String, Map<String, Integer>> wordDocumentsMap = new HashMap<>();

        for (KV kv : kvList) {
            String word = kv.getKey();
            String doc = kv.getValue().toString();

            if (!wordDocumentsMap.containsKey(word)) {
                wordDocumentsMap.put(word, new HashMap<>());
            }

            Map<String, Integer> documentCountMap = wordDocumentsMap.get(word);
            if (!documentCountMap.containsKey(doc)) {
                documentCountMap.put(doc, 0);
            }

            documentCountMap.put(doc, documentCountMap.get(doc) + 1);
        }

        return wordDocumentsMap.entrySet().stream()
                .map(kv -> new KV(kv.getKey(), kv.getValue()))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}

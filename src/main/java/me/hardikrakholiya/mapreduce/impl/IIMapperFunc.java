package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.MapperFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IIMapperFunc implements MapperFunc, Serializable {

    @Override
    public List<KV> map(List<KV> docWordList) {
        List<KV> wordDocList = new ArrayList<>();

        for (KV kv : docWordList) {
            wordDocList.add(new KV(kv.getValue().toString(), kv.getKey()));
        }

        return wordDocList;
    }
}

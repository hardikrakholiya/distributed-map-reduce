package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.api.MapperFunc;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WCMapperFunc implements MapperFunc, Serializable {

    @Override
    public List<KV> map(List<KV> documentWordList) {
        List<KV> word1List = new ArrayList<>();

        for (KV kv : documentWordList) {
            word1List.add(new KV(kv.getValue().toString(), 1));
        }
        return word1List;
    }

}

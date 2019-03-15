package me.hardikrakholiya.mapreduce.api;

import me.hardikrakholiya.mapreduce.model.KV;

import java.util.List;

public interface ReducerFunc {
    List<KV> reduce(List<KV> listOfKVPair);
}

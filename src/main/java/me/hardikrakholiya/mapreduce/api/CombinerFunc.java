package me.hardikrakholiya.mapreduce.api;

import me.hardikrakholiya.mapreduce.model.KV;

import java.util.List;

public interface CombinerFunc {
    List<KV> combine(List<KV> kvList);
}

package me.hardikrakholiya.mapreduce.api;

import me.hardikrakholiya.mapreduce.model.KV;

import java.util.List;

public interface MapperFunc {
    List<KV> map(List<KV> kvList);
}
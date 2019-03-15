package me.hardikrakholiya.mapreduce.api;

public interface PartitioningFunc {
    int partition(Object key);
}

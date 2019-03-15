package me.hardikrakholiya.mapreduce.impl;

import me.hardikrakholiya.mapreduce.Configurations;
import me.hardikrakholiya.mapreduce.api.PartitioningFunc;

import java.io.Serializable;

public class GenericPartitioningFunc implements PartitioningFunc, Serializable {
    @Override
    public int partition(Object key) {
        return key.hashCode() % Configurations.getReducerInstances().length;
    }
}

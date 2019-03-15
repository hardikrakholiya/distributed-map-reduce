package me.hardikrakholiya.mapreduce.model;

import java.io.Serializable;

public class KV implements Serializable {
    private String key;
    private Object value;

    public KV(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{" + key + "=" + value + "}";
    }
}

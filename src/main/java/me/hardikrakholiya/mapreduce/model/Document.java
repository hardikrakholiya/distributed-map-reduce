package me.hardikrakholiya.mapreduce.model;

import java.io.Serializable;

public class Document implements Serializable {
    private String name;
    private String content;

    public Document(String name, String content) {
        this.name = name;
        this.content = content;
    }

    public String getName() {
        return name;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Document{" +
                "name='" + name + '\'' +
                '}';
    }
}

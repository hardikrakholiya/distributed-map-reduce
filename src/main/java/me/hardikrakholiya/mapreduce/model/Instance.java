package me.hardikrakholiya.mapreduce.model;

import java.io.Serializable;

public class Instance implements Serializable {
    private final String address;
    private final int port;

    public Instance(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "{" + "address='" + address + '\'' + ", port=" + port + '}';
    }
}

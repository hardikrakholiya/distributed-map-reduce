package me.hardikrakholiya.mapreduce;

import me.hardikrakholiya.mapreduce.api.ReducerFunc;
import me.hardikrakholiya.mapreduce.model.Instance;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Reducer implements Runnable {
    private final int id;
    private final String address;
    private final int port;

    Reducer(int id, Instance instance) {
        this.id = id;
        this.address = instance.getAddress();
        this.port = instance.getPort();
    }

    @Override
    public void run() {
        try (Socket masterSocket = new Socket(Configurations.getMasterInstance().getAddress(), Configurations.getMasterInstance().getPort())
        ) {
            //receive mapper function
            ObjectOutputStream masterOS = new ObjectOutputStream(masterSocket.getOutputStream());
            masterOS.writeObject(id + ":mappers");
            ObjectInputStream masterIS = new ObjectInputStream(masterSocket.getInputStream());
            Instance[] mappers = (Instance[]) masterIS.readObject();

            //get reducer function
            masterOS.writeObject(id + ":reducer_func");
            ReducerFunc reducerFunc = (ReducerFunc) masterIS.readObject();

            //get output directory
            masterOS.writeObject(id + ":output_dir");
            String outputDirectoryPath = masterIS.readObject().toString();

            //notify master's client handler to exit
            masterOS.writeObject(id + ":exit");

            List<KV> allKVList = new ArrayList<>();

            //receive data from mappers one by one
            for (Instance instance : mappers) {
                try (Socket mapperSocket = new Socket(instance.getAddress(), instance.getPort())) {
                    ObjectOutputStream mapperOS = new ObjectOutputStream(mapperSocket.getOutputStream());
                    mapperOS.writeObject(id + ":data");
                    ObjectInputStream mapperIS = new ObjectInputStream(mapperSocket.getInputStream());
                    @SuppressWarnings("unchecked")
                    List<KV> kvList = (List<KV>) mapperIS.readObject();
                    if (kvList != null) {
                        allKVList.addAll(kvList);
                    }

                    //tell mapper handler to shutdown
                    mapperOS.writeObject(id + ":exit");

                    mapperIS.close();
                    mapperOS.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            StringBuilder sb = new StringBuilder();
            for (KV kv : reducerFunc.reduce(allKVList)) {
                sb.append(kv.getKey()).append("=").append(kv.getValue()).append(System.lineSeparator());
            }

            String outputFilePath = outputDirectoryPath + File.separator + id + ".txt";

            FileOutputStream fos = new FileOutputStream(outputFilePath);
            fos.write(sb.toString().getBytes());
            fos.close();
            System.out.println("reducer_" + id + " dumped the output data into: " + outputFilePath);

            masterOS.close();
            masterIS.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

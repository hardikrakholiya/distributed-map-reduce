package me.hardikrakholiya.mapreduce;

import me.hardikrakholiya.mapreduce.api.CombinerFunc;
import me.hardikrakholiya.mapreduce.api.PartitioningFunc;
import me.hardikrakholiya.mapreduce.api.MapperFunc;
import me.hardikrakholiya.mapreduce.model.Document;
import me.hardikrakholiya.mapreduce.model.Instance;
import me.hardikrakholiya.mapreduce.model.KV;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Mapper implements Runnable {
    private final int id;
    private final String address;
    private final int port;
    private List<Document> documents;
    private List<Thread> listOfReducerHandlers = new ArrayList<>();
    private HashMap<Integer, List<KV>> store = new HashMap<>();

    public Mapper(int id, Instance instance) {
        this.id = id;
        this.address = instance.getAddress();
        this.port = instance.getPort();
    }

    @Override
    public void run() {
        try (Socket masterSocket = new Socket(Configurations.getMasterInstance().getAddress(), Configurations.getMasterInstance().getPort());
             ServerSocket serverSocket = new ServerSocket(port);
        ) {
            //receive mapper function
            ObjectOutputStream masterOS = new ObjectOutputStream(masterSocket.getOutputStream());
            masterOS.writeObject(id + ":mapper_func");
            ObjectInputStream masterIS = new ObjectInputStream(masterSocket.getInputStream());
            MapperFunc mapperFunc = (MapperFunc) masterIS.readObject();

            //receive combiner function
            masterOS.writeObject(id + ":combiner_func");
            CombinerFunc combinerFunc = (CombinerFunc) masterIS.readObject();

            //receive partition function
            masterOS.writeObject(id + ":hash_func");
            PartitioningFunc partitioningFunc = (PartitioningFunc) masterIS.readObject();

            //receive work
            masterOS.writeObject(id + ":data");
            //noinspection unchecked
            documents = (List<Document>) masterIS.readObject();

            masterOS.writeObject(id + ":reducers");
            Instance[] reducers = (Instance[]) masterIS.readObject();

            //sanitize, map and combine
            List<KV> processedKVList = combinerFunc.combine(mapperFunc.map(setup()));

            //split and store
            for (KV kv : processedKVList) {
                Object key = kv.getKey();
                int bucketId = partitioningFunc.partition(key);
                if (!store.containsKey(bucketId)) {
                    store.put(bucketId, new ArrayList<>());
                }

                store.get(bucketId).add(kv);
            }

            //notify master it has been serviced so master can start reducers
            masterOS.writeObject(id + ":exit");

            int reducerServed = 0;
            while (reducerServed++ < reducers.length) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("mapper_" + id + " establishing a connection with a reducer");

                ObjectInputStream dis = new ObjectInputStream(clientSocket.getInputStream());
                ObjectOutputStream dos = new ObjectOutputStream(clientSocket.getOutputStream());

                Thread reducerHandler = new ReducerHandler(clientSocket, dis, dos);
                listOfReducerHandlers.add(reducerHandler);
                reducerHandler.start();
            }

            //join reducer handlers
            for (Thread reducerHandler : listOfReducerHandlers) {
                try {
                    reducerHandler.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            masterOS.close();
            masterIS.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private List<KV> setup() {
        List<KV> docWordPairs = new ArrayList<>();
        for (Document document : documents) {
            String[] words = document.getContent().replaceAll("[^a-zA-Z0-9\\s]+", "").toLowerCase().split("\\s+");
            for (String word : words) {
                docWordPairs.add(new KV(document.getName(), word));
            }
        }

        return docWordPairs;
    }

    class ReducerHandler extends Thread {

        private final Socket clientSocket;
        private final ObjectInputStream ois;
        private final ObjectOutputStream oos;

        ReducerHandler(Socket clientSocket, ObjectInputStream ois, ObjectOutputStream oos) {
            this.clientSocket = clientSocket;
            this.ois = ois;
            this.oos = oos;
        }

        @Override
        public void run() {
            boolean serviced = false;
            while (!serviced) {
                try {
                    String received = ois.readObject().toString();
                    String[] request = received.split(":");
                    int reducerId = Integer.parseInt(request[0]);

                    switch (request[1]) {
                        case "data":
                            List<KV> reducerIdBucket = store.get(reducerId);

                            if (reducerIdBucket != null) {
                                System.out.println("mapper_" + id + " received: '" + request[1] + "' from reducer_" + reducerId + " and sent: "
                                        + reducerIdBucket.subList(0, Math.min(10, reducerIdBucket.size())));
                                oos.writeObject(reducerIdBucket);
                            } else {
                                System.out.println("mapper_" + id + " received: '" + request[1] + "' from reducer_" + reducerId + " and sent: " + new ArrayList<>());
                                oos.writeObject(new ArrayList<>());
                            }

                            break;
                        case "exit":
                            System.out.println("mapper_" + id + " received: '" + request[1] + "' from reducer_" + reducerId + " and closing connection...");
                            serviced = true;
                    }

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            try {
                ois.close();
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

}

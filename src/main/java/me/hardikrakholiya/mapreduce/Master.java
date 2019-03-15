package me.hardikrakholiya.mapreduce;

import me.hardikrakholiya.mapreduce.model.Document;
import me.hardikrakholiya.mapreduce.model.Instance;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static me.hardikrakholiya.mapreduce.Configurations.*;

public class Master implements Runnable {
    private String address;
    private int port;
    private List<Thread> mappers = new ArrayList<>();
    private List<Thread> reducers = new ArrayList<>();
    private List<Thread> listOfMapperHandlers = new ArrayList<>();
    private List<Thread> listOfReducerHandlers = new ArrayList<>();

    public Master(Instance instance) {
        this.port = instance.getPort();
        System.out.println("master" + getMasterInstance() + " spawned");
        spawnWorkers();
    }

    private void spawnWorkers() {
        //spawn mappers
        for (int i = 0; i < getMapperInstances().length; i++) {
            Thread mapper = new Thread(new Mapper(i, getMapperInstances()[i]), "mapper_" + i);
            System.out.println("mapper_" + i + getMapperInstances()[i] + " spawned");
            mappers.add(mapper);
        }

        //spawn reducers
        for (int i = 0; i < getReducerInstances().length; i++) {
            Thread reducer = new Thread(new Reducer(i, getReducerInstances()[i]), "reducer_" + i);
            System.out.println("reducer_" + i + getReducerInstances()[i] + " spawned");
            reducers.add(reducer);
        }
    }

    @Override
    public void run() {

        try (ServerSocket serverSocket = new ServerSocket(port);
        ) {

            //mapper phase
            for (Thread mapper : mappers) {
                System.out.println("running " + mapper.getName());
                mapper.start();
            }

            int mapperServed = 0;
            while (mapperServed++ < mappers.size()) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("master establishing a connection with mapper");
                Thread mapperHandler = new WorkerHandler(clientSocket);
                listOfMapperHandlers.add(mapperHandler);
                mapperHandler.start();
            }

            //wait for mapperHandlers to finish
            for (Thread mapperHandler : listOfMapperHandlers) {
                mapperHandler.join();
            }

            System.out.println("MAPPER PHASE COMPLETED");

            //reducer phase
            for (Thread reducer : reducers) {
                System.out.println("running " + reducer.getName());
                reducer.start();
            }

            int reducerServed = 0;
            while (reducerServed++ < reducers.size()) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("master establishing a connection with reducer");
                Thread reducerHandler = new WorkerHandler(clientSocket);
                listOfReducerHandlers.add(reducerHandler);
                reducerHandler.start();
            }

            //wait for reducerHandlers to finish
            for (Thread reducerHandler : listOfReducerHandlers) {
                reducerHandler.join();
            }

            //wait for reducers to finish
            for (Thread reducer : reducers) {
                try {
                    reducer.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("REDUCER PHASE COMPLETED");

            //wait for mappers to finish
            for (Thread mapper : mappers) {
                try {
                    mapper.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("master exiting...");
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port " + port + " or listening for a connection");
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class WorkerHandler extends Thread {

        private final ObjectInputStream ois;
        private final ObjectOutputStream oos;

        WorkerHandler(Socket clientSocket) throws IOException {
            this.ois = new ObjectInputStream(clientSocket.getInputStream());
            this.oos = new ObjectOutputStream(clientSocket.getOutputStream());
        }

        @Override
        public void run() {
            boolean serviced = false;
            while (!serviced) {
                try {

                    String received = ois.readObject().toString();
                    String[] request = received.split(":");
                    int workerId = Integer.parseInt(request[0]);

                    switch (request[1]) {
                        case "mapper_func":
                            System.out.println("master received: '" + request[1] + "' from mapper_" + workerId + " and sent: " + getMapperFuncImpl());
                            oos.writeObject(getMapperFuncImpl());
                            break;
                        case "reducer_func":
                            System.out.println("master received: '" + request[1] + "' from reducer_" + workerId + " and sent: " + getReducerFuncImpl());
                            oos.writeObject(getReducerFuncImpl());
                            break;
                        case "combiner_func":
                            System.out.println("master received: '" + request[1] + "' from mapper_" + workerId + " and sent: " + getCombinerFuncImpl());
                            oos.writeObject(getCombinerFuncImpl());
                            break;
                        case "hash_func":
                            System.out.println("master received: '" + request[1] + "' from mapper_" + workerId + " and sent: " + getPartitioningFuncImpl());
                            oos.writeObject(getPartitioningFuncImpl());
                            break;
                        case "data":
                            List<Document> listOfDocuments = new ArrayList<>();
                            for (int i = 0; i < getDocuments().length; i++) {
                                if (i % getMapperInstances().length == workerId) {
                                    listOfDocuments.add(getDocuments()[i]);
                                }
                            }
                            System.out.println("master received: '" + request[1] + "' from mapper_" + workerId + " and sent: " + listOfDocuments);
                            oos.writeObject(listOfDocuments);
                            break;
                        case "mappers":
                            System.out.println("master received: '" + request[1] + "' from reducer_" + workerId + " and sent: " + Arrays.toString(getMapperInstances()));
                            oos.writeObject(getMapperInstances());
                            break;
                        case "reducers":
                            System.out.println("master received: '" + request[1] + "' from mapper_" + workerId + " and sent: " + Arrays.toString(getReducerInstances()));
                            oos.writeObject(getReducerInstances());
                            break;
                        case "output_dir":
                            System.out.println("master received: '" + request[1] + "' from reducer_" + workerId + " and sent: " + getOutputDirectoryPath());
                            oos.writeObject(getOutputDirectoryPath());
                            break;
                        case "exit":
                            System.out.println("master received: '" + request[1] + "' from worker_" + workerId + " and closing connection...");
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

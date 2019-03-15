package me.hardikrakholiya.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MapReduce {

    private static int clusterCounter = 0;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Please provide path to config.properties as an argument");
        }

        String configPath = args[0];
        Configurations.loadConfigurations(configPath);

        Thread master = null;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
            String userInput;
            while (true) {
                System.out.print(">>> ");
                userInput = in.readLine();

                //command to exit the application
                if (userInput.equalsIgnoreCase("exit")) {
                    break;
                }

                //init a cluster
                else if (userInput.equalsIgnoreCase("init") || userInput.equalsIgnoreCase("init_cluster")) {
                    if (master != null) {
                        System.out.println("There is already a cluster running");
                        continue;
                    }
                    master = new Thread(new Master(Configurations.getMasterInstance()), "master_" + ++clusterCounter);
                    System.out.println("Cluster created with cluster_id: " + clusterCounter);

                    System.out.println("Please specify map-reduce task to run with the run command");
                    System.out.println("eg. Type 'run 1' for word count or type 'run 2' for inverted index");
                }

                //run map reduce tasks
                else if (userInput.startsWith("run")) {
                    if (master == null) {
                        System.out.println("Cluster doesn't exist. Create a cluster first with init command");
                        continue;
                    }

                    String[] arguments = userInput.split("\\s+");
                    if (arguments.length < 2) {
                        System.out.println("Please specify map-reduce task to run with the run command");
                        System.out.println("eg. Type 'run 1' for word count or type 'run 2' for inverted index");
                        continue;
                    }

                    String mapReduceTaskId = arguments[1];
                    Configurations.setMapReduceJob(mapReduceTaskId);

                    master.start();
                    master.join();
                    master = null;
                }
                
                //destroy a cluster
                else if (userInput.startsWith("destroy")) {
                    System.out.println("Cluster with id {" + clusterCounter + "} destroyed");
                    master = null;
                } else {
                    System.out.println("Invalid command");
                }
            }

        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}

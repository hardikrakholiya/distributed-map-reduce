package me.hardikrakholiya.mapreduce;

import com.google.common.base.Preconditions;
import me.hardikrakholiya.mapreduce.api.CombinerFunc;
import me.hardikrakholiya.mapreduce.api.PartitioningFunc;
import me.hardikrakholiya.mapreduce.api.MapperFunc;
import me.hardikrakholiya.mapreduce.api.ReducerFunc;
import me.hardikrakholiya.mapreduce.impl.*;
import me.hardikrakholiya.mapreduce.model.Document;
import me.hardikrakholiya.mapreduce.model.Instance;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class Configurations {

    private static Instance masterInstance;
    private static Instance[] mapperInstances;
    private static Instance[] reducerInstances;
    private static Document[] documents;
    private static String outputDirectoryPath;
    private static PartitioningFunc partitioningFuncImpl = new GenericPartitioningFunc();

    private static MapperFunc mapperFuncImpl;
    private static CombinerFunc combinerFuncImpl;
    private static ReducerFunc reducerFuncImpl;

    public static void loadConfigurations(String configPath) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configPath));
        } catch (IOException e) {
            System.out.println("Properties file at path: {" + configPath + "} not found");
            System.exit(-1);
        }

        //master instance details
        String masterString = properties.getProperty("master");
        Preconditions.checkArgument(masterString != null && !masterString.isEmpty(), "Provide instance details for master node");
        String[] masterAddressAndPort = masterString.trim().split(":");
        masterInstance = new Instance(masterAddressAndPort[0], Integer.parseInt(masterAddressAndPort[1]));
        System.out.println("master instance: " + masterInstance);

        //mapper instances details
        String mappersString = properties.getProperty("mappers");
        Preconditions.checkArgument(mappersString != null && !mappersString.isEmpty(), "Provide comma separated instance details for mapper nodes");
        List<Instance> listOfMappers = new ArrayList<>();
        Arrays.stream(mappersString.split(",")).forEach(instance -> {
            String[] instanceAddressAndPort = instance.trim().split(":");
            listOfMappers.add(new Instance(instanceAddressAndPort[0], Integer.parseInt(instanceAddressAndPort[1])));
        });
        mapperInstances = listOfMappers.toArray(new Instance[0]);
        System.out.println("mapper instances: " + Arrays.toString(mapperInstances));

        //reducer instances details
        String reducersString = properties.getProperty("reducers");
        Preconditions.checkArgument(reducersString != null && !reducersString.isEmpty(), "Provide comma separated instance details for reducer nodes");
        List<Instance> listOfReducers = new ArrayList<>();
        Arrays.stream(reducersString.split(",")).forEach(instance -> {
            String[] instanceAddressAndPort = instance.trim().split(":");
            listOfReducers.add(new Instance(instanceAddressAndPort[0], Integer.parseInt(instanceAddressAndPort[1])));
        });
        reducerInstances = listOfReducers.toArray(new Instance[0]);
        System.out.println("reducer instances: " + Arrays.toString(reducerInstances));

        //input files
        String inputDirectoryPath = properties.getProperty("input_dir");
        Preconditions.checkArgument(inputDirectoryPath != null, "Input directory path is null");

        File inputDirectory = new File(inputDirectoryPath);
        Preconditions.checkArgument(inputDirectory.exists(), "Input directory '%s' doesn't exist", inputDirectory);
        Preconditions.checkArgument(inputDirectory.isDirectory(), "Input directory '%s' is not a directory", inputDirectory);
        System.out.println("input directory: " + inputDirectory);

        File[] files = inputDirectory.listFiles();
        Preconditions.checkState(files != null && files.length > 0, "No files found in input directory");

        List<Document> listOfDocuments = new ArrayList<>();

        for (File file : files) {
            if (file.getName().endsWith(".txt")) {
                StringBuilder contentBuilder = new StringBuilder();
                try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()), StandardCharsets.UTF_8)) {
                    stream.forEach(s -> contentBuilder.append(s).append(" "));
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                listOfDocuments.add(new Document(file.getName(), contentBuilder.toString()));
            }
        }

        documents = listOfDocuments.toArray(new Document[0]);
        System.out.println("documents: " + Arrays.toString(documents));

        //output directory
        outputDirectoryPath = properties.getProperty("output_dir");
        Preconditions.checkArgument(outputDirectoryPath != null, "Output directory path is null");

        File outputDirectory = new File(outputDirectoryPath);
        System.out.println("output directory: " + outputDirectoryPath);

        if (!outputDirectory.exists()) {
            if (new File(outputDirectoryPath).mkdirs()) {
                System.out.println("output directory created: " + outputDirectoryPath);
            }
        }
    }

    public static void setMapReduceJob(String mapReduceTaskId) {
        if (mapReduceTaskId.equals("1")) {
            mapperFuncImpl = new WCMapperFunc();
            combinerFuncImpl = new WCCombinerFunc();
            reducerFuncImpl = new WCReducerFunc();
        } else {
            mapperFuncImpl = new IIMapperFunc();
            combinerFuncImpl = new IICombinerFunc();
            reducerFuncImpl = new IIReducerFunc();
        }
    }

    public static Instance getMasterInstance() {
        return masterInstance;
    }

    public static Instance[] getMapperInstances() {
        return mapperInstances;
    }

    public static Instance[] getReducerInstances() {
        return reducerInstances;
    }

    public static Document[] getDocuments() {
        return documents;
    }

    public static MapperFunc getMapperFuncImpl() {
        return mapperFuncImpl;
    }

    public static ReducerFunc getReducerFuncImpl() {
        return reducerFuncImpl;
    }

    public static CombinerFunc getCombinerFuncImpl() {
        return combinerFuncImpl;
    }

    public static PartitioningFunc getPartitioningFuncImpl() {
        return partitioningFuncImpl;
    }

    public static String getOutputDirectoryPath() {
        return outputDirectoryPath;
    }
}

import com.hadoop.processor.MyProcessor;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.common.Parameters;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rest.RestfulService;
import cn.helium.kvstore.rpc.RpcServer;
import com.beust.jcommander.JCommander;
import org.apache.commons.collections.map.HashedMap;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Work1 {


    public static void main(String[] args) throws InterruptedException {

//        MyProcessor myProcessor = new MyProcessor();
//       Map<String,String> value = myProcessor.get("123");


        Parameters parameters = new Parameters();
        new JCommander(parameters, args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            RestfulService.stop();
            RpcServer.stop();
        }));

        startServer(parameters);

        System.out.println("hosts:" + KvStoreConfig.getHost(0) + " ");
        System.out.println("hosts:" + KvStoreConfig.getHost(1) + " ");
        System.out.println("server:" + RpcServer.getRpcServerId());

//        writeMap();
//        readMap();
    }

    private static void startServer(Parameters parameters) {
        try {

            Class e = Class.forName(parameters.processorClass);
            Processor processor = (Processor) e.getConstructor(new Class[0]).newInstance(new Object[0]);
            RestfulService.startService(processor, parameters.restPort);
            KvStoreConfig.loadConfig(parameters);
            RpcServer.startServer(parameters.rpcId, parameters.rpcThreadNum, processor);
            Thread.currentThread().join();

        } catch (Exception var3) {

            var3.printStackTrace();
        }
    }

    public static void writeMap(){

        Map<String, String>map = new HashMap<>();

        map.put("name","zhangsan");
        map.put("password","123");

        try {
            FileOutputStream outStream = new FileOutputStream("/Users/wangxiaofa/Desktop/Hadoop/Work1/1.txt");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
            objectOutputStream.writeObject(map);

            outStream.close();

            System.out.println("successful");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void readMap(){

        FileInputStream freader;

        try {
            freader = new FileInputStream("/Users/wangxiaofa/Desktop/Hadoop/Work1/1.txt");
            ObjectInputStream objectInputStream = new ObjectInputStream(freader);
            Map<String,String> map ;
            map = (HashMap<String,String>)objectInputStream.readObject();
            System.out.println("The name is " + map.get("name"));
            System.out.println("The name is " + map.get("password"));
            System.out.println("The map is " + map);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}

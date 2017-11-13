package com.hadoop.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CacheUtils {

    private static final String LOCAL_DISK_PATH = "/opt/localdisk";  //本地缓存
    private static final String META_DIR = "metadata"; // 元数据文件夹
    private static final String META_DIR_PATH = LOCAL_DISK_PATH + "/" + META_DIR; // 元数据路径
    private static final String LOCAL_CACHE_DIR = "cache";
    private static final String LOCAL_CACHE_PATH = LOCAL_DISK_PATH + "/" + LOCAL_CACHE_DIR;
    private static final String FIRST_META_NAME = "firstIndex.txt";
    private static final String SECOND_META_NAME = "secondIndex";

    private static final int META_DATA_SIZE = 35 ; // 缓存数据大小为 60M
    private static final int CACHE_DATA_SIZE = 60 ; // 缓存数据大小为 60M

    private static int fileName = 1;
    private static int line = 0;


    /**
     * 直接从hdfs中读取数据
     * @param key
     * @return 返回value
     */
    public static Map<String, String> getValueFromHdfs(String key){

        String meta_path = META_DIR_PATH + "/" + FIRST_META_NAME;

        HdfsOperator hdfsOperator = new HdfsOperator();

        File file = new File(meta_path);

        if (!file.exists()){
            CacheUtils.createMetaDir();
            hdfsOperator.copyMetaToCache(meta_path,FIRST_META_NAME);
        }
        try {

            InputStreamReader read = new InputStreamReader(new FileInputStream(file));
            BufferedReader bufferedReader = new BufferedReader(read);

             String line = null;

            while ((line = bufferedReader.readLine()) != null)
            {
                String[] values = line.split(" ");

                if (values[0].equals(key)){

                    //通过key获取hdfs中对应的文件名和所在行
                    String hdfsFileName = values[1];
                    String hdfsLine = values[2];

                    String valueLine = hdfsOperator.readLineFromHdfs(hdfsFileName,Integer.valueOf(hdfsLine));


                    Gson gson = new Gson();

                    Map<String,String> map = gson.fromJson(valueLine,Map.class);

                    return map;
                }
            }

            bufferedReader.close();
            read.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将数据存入hdfs中
     * @param map
     */
    public static void saveKeysAndValues(Map<String,Map<String,String>> map){

        /**
         * 存储过程：
         * 1.建立一级索引文件firstIndex.txt，存储索引数据
         * 2.将数据写入缓存文件中，其中文件名是以数字的升序排名 1.txt,2.txt,3.txt.....
         */

        //创建索引文件夹和缓存文件件
        CacheUtils.createMetaDir();
        CacheUtils.createCacheDir();


        /** 更新索引
         * meta索引中记录的形式：100 1 23
         * 第一列是数据的键（key），100
         * 第二列是所在文件名（filename），1
         * 第三列是文件中所在的行（line）,23
         */
        String firstIndexPath = META_DIR_PATH + "/" + FIRST_META_NAME;

        String keyLines = "";
        String valueLines = "";

        int count = 0;


        Iterator entries = map.entrySet().iterator();

        while (entries.hasNext()) {

            Map.Entry entry = (Map.Entry) entries.next();

            String key = entry.getKey() .toString();

            keyLines += key + " " + fileName + " " + count + "\r\n";

            Map value = (Map) entry.getValue();

            Gson gson = new Gson();
            String json = gson.toJson(value);
            valueLines += json + "\r\n";

            count ++;

        }

        //将key写入缓存中
        CacheUtils.saveFileToCahce(firstIndexPath,keyLines);

        /**
         * 将value数据写入缓存，在写之前先判断文件缓存下的文件是否超过CACHE_DATA_SIZE，
         * 如果超过CACHE_DATA_SIZE，现将文件存入hdfs中，然后删掉缓存文件，重新建立文件，
         * 在写入到缓存中
         */
        String cacheFilePath = LOCAL_CACHE_PATH + "/" +fileName + ".txt" ;
        File cacheFile = new File(cacheFilePath);


        double fileSize = CacheUtils.checkSize(cacheFile);

        HdfsOperator hdfsOperator = new HdfsOperator();

        //超过CACHE_DATA_SIZE后，要上传到hdfs中，并且删除缓存文件
        if (fileSize >= CACHE_DATA_SIZE) {
            hdfsOperator.copyFileToHdfs(0,cacheFilePath);
            System.out.println("hdfs 数据拷贝完成");

            cacheFile.delete();

            //文件名递增
            fileName++;
            cacheFilePath = LOCAL_CACHE_PATH + "/" +fileName + ".txt" ;
        }

        //创建缓存文件并将数据存储到缓存中
        if (cacheFile.exists()){

            CacheUtils.saveFileToCahce(cacheFilePath,valueLines);
        }
        else {

            CacheUtils.saveFileToCahce(cacheFilePath,valueLines);
        }

        hdfsOperator.copyFileToHdfs(0,cacheFilePath);
        hdfsOperator.copyFileToHdfs(-1,firstIndexPath);

    }

    public static void createCacheDir() {

        CacheUtils.createDir(LOCAL_CACHE_PATH);

    }

    public static void createMetaDir(){

        CacheUtils.createDir(META_DIR_PATH);
    }

    public static void createDir(String path){

        File dirFile = new File(path);

        if (!dirFile.exists()  && !dirFile .isDirectory()){
            dirFile.mkdir();
        }

        System.out.println("create dir :" + path);
    }

    public static void createFile(String path){

        File file = new File(path);

        System.out.println(path);

        if (!file.exists()){

            try {

                file.createNewFile();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void saveFileToCahce(String path, String content){

        File file = new File(path);

        if (!file.exists()){
            CacheUtils.createFile(path);
        }

        BufferedWriter out = null;
        try {

            out = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(path, true)));
            out.write(content +"\r\n");

        } catch (FileNotFoundException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }finally {

            try {

                out.flush();
                out.close();

            } catch (IOException e) {

                e.printStackTrace();

            }
        }
    }

    public static double checkSize(File file){

        if (file.exists()){

            if (file.isDirectory()) {
                File[] children = file.listFiles();
                double size = 0;
                for (File f : children)
                    size += checkSize(f);
                return size;
            } else {

                //如果是文件则直接返回其大小,以“M”为单位
                double size = (double) file.length() / 1024 / 1024;
                return size;
            }


        }

        return 0;
    }

    public static void writeCacheToHdfs(){

        String cacheFilePath = LOCAL_CACHE_PATH + "/" + fileName + ".txt";

        HdfsOperator hdfs = new HdfsOperator();
        hdfs.copyFileToHdfs(0,cacheFilePath);
    }

    public static void main(String[] args){


//        String key = getValueFromHdfs("124");
//        System.out.println("value:" + key);
        writeCacheToHdfs();
    }


}

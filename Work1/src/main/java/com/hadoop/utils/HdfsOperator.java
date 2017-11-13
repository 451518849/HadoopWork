package com.hadoop.utils;
import cn.helium.kvstore.common.KvStoreConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URI;

public class HdfsOperator {

    private static final Logger LOG = LogManager.getLogger();

    FileSystem hdfs;
    Configuration conf = new Configuration();

    public HdfsOperator() {

        conf.get("fs.default.name");

    }

    /**
     * 将索引先取出来，放到缓存中
     * @param cachePath
     * @param metaName
     */
    public void copyMetaToCache(String cachePath,String metaName){

        String path = KvStoreConfig.getHdfsUrl() + "/"  + metaName ;

        Path hdfsPath = new Path(path);

        try {

            hdfs = FileSystem.get(URI.create(path),conf);

            FSDataInputStream hdfsFile = hdfs.open(hdfsPath);

            FileOutputStream out =  new FileOutputStream(cachePath, true);

            IOUtils.copyBytes(hdfsFile, out, 4096, true);

        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    /**
     * 从中hdfs中按照给定的行，读取value数据
     * @param filename 文件名
     * @param line value所在行
     * @return 返回读取到的value数据
     */
    public String readLineFromHdfs(String filename,int line){

        String path = KvStoreConfig.getHdfsUrl() + "/" + "0" + "/" + filename + ".txt";

        try {

            Path hdfsPath = new Path(path);
            hdfs = FileSystem.get(URI.create(path),conf);
            FSDataInputStream hdfsFile = hdfs.open(hdfsPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(hdfsFile));

            String lineStr;
            int count = 0;

            while ((lineStr = reader.readLine()) != null) {

                if (count == line){
                    return lineStr;
                }

                count++;
            }

            reader.close();
            hdfs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }


    /**
     * 将缓存文件复制到hdfs中，hdfs中的文件目录：serverId／data   如：0／1.txt
     * @param serverId 当前server的id
     * @param cachePath 缓存路径
     */
    public void copyFileToHdfs(int serverId, String cachePath){

        String[] pathList = cachePath.split("/");
        String filename = pathList[pathList.length - 1];

        try {


            String path = "";

            if (serverId == -1){

                //将索引写入HDFS
                path =KvStoreConfig.getHdfsUrl()  + "/" + filename;
            }
            else {

                //将value写入HDFS
                path = KvStoreConfig.getHdfsUrl() + "/" + serverId+ "/" + filename;
            }

            LOG.info("准备写入hdfs的path：" + path);

            Path hdfsPath = new Path(path);
            hdfs = FileSystem.get(URI.create(path),conf);

            InputStream in = new BufferedInputStream(new FileInputStream(cachePath));
            FSDataOutputStream output = hdfs.create(hdfsPath, new Progressable() {
                @Override
                public void progress() {
                    System.out.println("loading copy file to hdfs ");
                }
            });

            IOUtils.copyBytes(in, output, 4096, true);

            LOG.info("HDFS写入成功！" + path);

        } catch (FileNotFoundException e) {

            e.printStackTrace();
        } catch (IOException e) {

            e.printStackTrace();
        }


    }

}

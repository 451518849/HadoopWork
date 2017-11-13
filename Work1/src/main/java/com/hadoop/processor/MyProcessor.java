package com.hadoop.processor;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.hadoop.utils.CacheUtils;
import com.hadoop.utils.HdfsOperator;
import com.hadoop.utils.HttpClientUtils;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MyProcessor implements Processor {

    private static int lastServerId = -1; // 记录应该向哪个kvPod发数据

    private static final String INFORM_FORMAT = "connect"; //通知是否能够正常通信
    private static final String SUCCESS_INFORM_FORMAT = "success connect"; //能够正常通信

    private static ConcurrentHashMap <String, Map<String, String>> kv_store = new ConcurrentHashMap<>();
    private static int masterId = -1;  //接受数据的节点作为master节点
    private static ArrayList failedNodes = new ArrayList(); //存放通信失败的节点
    private static ArrayList availableNodes = new ArrayList(); //存放通信失败的节点

    private static int MAX_INFORM_COUNT = 200000; //计数器，

    private static Timer timer = null;
    private static int timeCount = 0; //计数器，
    private static int timerState = 0; //计数器，
    private static int TIME_OUT_COUNT = 3; //5秒内没有数据则表示，数据发送已经完成

    private static final Logger LOG = LogManager.getLogger();

    public MyProcessor() {

    }

    @Override
    public Map<String, String> get(String s) {

        Map<String, String> record = CacheUtils.getValueFromHdfs(s);

        System.out.println(record);
        return record;
    }

    @Override
    public boolean put(String s, Map<String, String> map) {


        timeCount = 0;

        if (timer == null){
            startTimer();
        }

        kv_store.put(s,map);

        return true;
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {

        timeCount = 0;

        if (timer == null){
            startTimer();
        }

        kv_store.putAll(map);

        return false;
    }

    @Override
    public int count(Map<String, String> map) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        return null;
    }

    @Override
    public byte[] process(byte[] bytes) {

        String inform_format = new String(bytes);

        System.out.println("通知消息：" + inform_format);

        /**
         * 1.测试是否能够正常通信，则返回connect success，告诉对方能够正常通信
         * 2.通信成功后，通过inform将数据传到其它节点处理
         */

        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(inform_format, JsonObject.class);
        String master = jsonObject.get("master").toString();
        String info = jsonObject.get("info").toString();

        masterId = Integer.valueOf(master);

        System.out.println("back info:" + info);

        if (info.contains(INFORM_FORMAT)) {

            return SUCCESS_INFORM_FORMAT.getBytes();
        }

        return new byte[0];
    }

    public static void saveDataToHdfs(){

        System.out.println("host1:" +KvStoreConfig.getHost(0));
        System.out.println("host2:" +KvStoreConfig.getHost(1));
        System.out.println("hdfs:" + KvStoreConfig.getHdfsUrl());
        System.out.println("local server:" + RpcServer.getRpcServerId());

        if (kv_store.size() < MAX_INFORM_COUNT){

            CacheUtils.saveKeysAndValues(kv_store);

        }
        else {

            System.out.println("ready for informing other node....");
            /**
             * 0.第一个接受到数据的kvPod默认为master节点，负责给其他节点分发数据，实现负载均衡，其他节点则为worker
             * 1.先和其他节点通讯，判断其他节点的状态
             * 2.其他节点若在正常状态，则将数据发送给其它节点处理，并将数据放入pipeline中
             * 3.若其他节点无法联系，则自己处理数据，不存入pipeline中
             */

            //将所有节点的id存入数组中
            ArrayList servers = new ArrayList();

            servers.add(0);
            servers.add(1);
            servers.add(2);


            if (masterId == -1) {
                masterId = RpcServer.getRpcServerId();
            }

            /**
             * master的工作
             */
            if (masterId == RpcServer.getRpcServerId()) {

                LOG.info("master 节点接受到数据后开始工作，准备通知其他节点");
                LOG.info("lastServerId:" + lastServerId);

                if (lastServerId == -1) {

                    //排除自己，自己不处理数据
                    servers.remove(RpcServer.getRpcServerId());

                    //获取准备通知的node
                    int serverId = (int) servers.get(0);

                    LOG.info("准备通知的节点的id是 :" + serverId);

                    if (availableNodes.contains(serverId)){

                        LOG.info("准备把数据发给此 server：" + serverId);
                        MyProcessor.postDataToServer(serverId);

                    }
                    else {

                        Boolean isSuccess = MyProcessor.informKvPod(serverId);

                        LOG.info("通知id为：" + serverId +"是否成功 :" + isSuccess);

                        if (isSuccess) {

                            LOG.info("成功");

                            MyProcessor.postDataToServer(serverId);

                        } else {


                            LOG.info("所有节点都不成功 master 自己处理");
                            //否则自己处理
                            CacheUtils.saveKeysAndValues(kv_store);

                        }

                    }


                }
                /**
                 *  下面的语句中就不不再判断是否能够正常t正常通信，所有判断工作都在上面做了。
                 */
                else {

                    //两个node节点都活着
                    if (availableNodes.size() > 1) {

                        LOG.info("第一个节点通知成功，准备通知给第二个节点，");
                        //post 给其他节点
                        servers.remove(masterId);
                        servers.remove(lastServerId);
                        int serverId = (int)servers.get(0);

                        MyProcessor.postDataToServer(serverId);

                    }

                    //一个node节点活着
                    else if (availableNodes.size() == 1){

                        MyProcessor.postDataToServer(lastServerId);

                    }
                    //一个node节点没活
                    else {
                        //自己处理数据
                        CacheUtils.saveKeysAndValues(kv_store);
                    }

                }
            } else {


                LOG.info("worker 节点连接正常，开始处理接受到的数据");
                //worker节点的处理逻辑
                CacheUtils.saveKeysAndValues(kv_store);

            }


        }


    }

    /**
     * 通知其他节点，看其他节点的网络是否正常，如果正常则告诉节点master节点的id
     * 如果通知的第一个节点失败，会继续通知第二个节点！！！！
     * @param serverId 被通知的id
     * @return
     */
    public static boolean informKvPod(int serverId) {


        //将所有节点的id存入数组中
        ArrayList servers = new ArrayList();

        servers.add(0);
        servers.add(1);
        servers.add(2);

        //排除自己的id和正在通信的server的id
        servers.remove(RpcServer.getRpcServerId());
        servers.remove(serverId);

        Gson gson = new Gson();
        Map inform_map = new HashMap();

        inform_map.put("master", masterId);
        inform_map.put("info", INFORM_FORMAT);

        String json_info = gson.toJson(inform_map);


        LOG.info("inform data：" + inform_map);

        try {

            String result = new String(RpcClientFactory.inform(serverId, json_info.getBytes()));

            LOG.info("inform result data:" + result);


            if (result.equals(SUCCESS_INFORM_FORMAT)) {

                if (!availableNodes.contains(serverId)) {
                    availableNodes.add(serverId);
                }
                lastServerId = serverId;
                return true;

            } else {

                LOG.info("the connect inform is invalid");
            }
        } catch (IOException e) {
            e.printStackTrace();

            LOG.info("id为：" + serverId + ",host为：" + KvStoreConfig.getHost(serverId) + " 的节点连接异常");

            //将连接失败的节点保存起来
            failedNodes.add(serverId);

            //有两失败节点后，就不再询问了
            if (failedNodes.size() == (KvStoreConfig.getServersNum() - 1)) {
                lastServerId = -2;
                return false;
            }

            //第二次通知其他节点
            lastServerId = (int) servers.get(0);
            MyProcessor.informKvPod(lastServerId);

        }

        return false;
    }

    public static void postDataToServer(int serverId){

        //通过post将数据传给通信成功的节点
        String url = "http://" + KvStoreConfig.getHost(serverId) + ":8500" +"/batchProcess";
        HttpClientUtils.doBatchPost(url,kv_store);

        LOG.info("数据发送成功 ：" + serverId);
    }

    public static void startTimer(){

        timerState = 1;
        timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                //计数器累加.
                timeCount ++;

                System.out.println("time:" + timeCount);

                if (timeCount > TIME_OUT_COUNT) {
                    timer.cancel();
                    timer = null;
                    timerState = 0;

                    //暂时master不做任何数据的存储
                    if (masterId != RpcServer.getRpcServerId()){
                        saveDataToHdfs();
                    }
                }
            }
        },1000,1000);

    }

}

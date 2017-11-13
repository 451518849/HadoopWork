package com.hadoop.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class CacheRunnable implements Runnable {

    private Map kv_store = new HashMap();

    public CacheRunnable(Map kv_store){

        this.kv_store = kv_store;

    }
    @Override
    public void run() {

        for (int i = 0; i < 10; i++){

//            Map map = new HashMap();
//            map.put(i,1);
//            kv_store.put("key",map);

            System.out.println(i);
        }
//        Iterator entries = kv_store.entrySet().iterator();
//
//        while (entries.hasNext()) {
//
//            Map.Entry entry = (Map.Entry) entries.next();
//
//            String key = (String)entry.getKey();
//
//            Map value = (Map) entry.getValue();
//            CacheUtils.saveMapToCacheFile(key,value);
//
//        }
    }

}

public class CacheThread {

    public static void main(String[] args){

        Map kv_store = new HashMap();

        for (int i = 0; i < 1000000; i++){

            Map map = new HashMap();
            map.put("key",i);

            kv_store.put(String.valueOf(i),map);

//            System.out.println(i);
        }

        CacheUtils.saveKeysAndValues(kv_store);

//        Iterator entries = kv_store.entrySet().iterator();
//
//        while (entries.hasNext()) {
//
//            Map.Entry entry = (Map.Entry) entries.next();
//
//            String key = (String)entry.getKey();
//
//            Map value = (Map) entry.getValue();
//            CacheUtils.saveMapToCacheFile(key,value);
//
//        }


//        CacheRunnable cacheRunnable = new CacheRunnable(kv_store);
//        new Thread(cacheRunnable).start();
//        new Thread(cacheRunnable).start();

    }
}
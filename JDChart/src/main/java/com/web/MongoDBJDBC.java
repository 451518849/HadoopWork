package com.web;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MongoDBJDBC {

    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    MongoDatabase mongoDatabase = mongoClient.getDatabase("spark");


    public ArrayList<Object> getWordsPairFromCollection(String collectionnName) {

        FindIterable<Document> findIterable = mongoDatabase.getCollection(collectionnName).find();

        ArrayList<Object> list = new ArrayList<>();

        for(Document document : findIterable) {

            for (String key: document.keySet()) {

                if (!key.endsWith("_id")){

                    ArrayList<Object> arrayList = new ArrayList<>();
                    arrayList.add(key);
                    arrayList.add(document.get(key));
                    list.add(arrayList);

//                    System.out.println(key + " "+ document.get(key) );
                }
            }

        }

        return list;
    }
    public static void main( String args[] ){
        try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("spark");
            FindIterable<Document> findIterable = mongoDatabase.getCollection("wordcount").find();

            for(Document document : findIterable) {

                for (String key: document.keySet()) {

                    if (!key.endsWith("_id")){
                        System.out.println(key + " "+ document.get(key) );
                    }
                }

            }


//            System.out.println("Connect to database successfully");

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }
}

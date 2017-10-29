package com.jd.db

import com.mongodb.DB
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoDB}

class MongodbManager extends DBManager {

  var db:MongoDB = null

  override def connnectDB(host: String, dbName: String) = {

    val mongoClient = MongoClient(host, 27017)
    db = mongoClient.getDB(dbName)
  }

  override def insertDB(collection:String,key:String,value:String): Unit = {
    db.getCollection(collection).insert(MongoDBObject(key->value))
  }


}

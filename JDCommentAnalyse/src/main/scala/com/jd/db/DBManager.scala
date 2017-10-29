package com.jd.db

trait DBManager {

  def connnectDB(host: String,dbName:String)

  def insertDB(collection:String,key:String,value:String)

}

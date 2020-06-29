package com.finfsoft.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

//mongodb 连接数据库工具类
public class MongoDBUtil {
    //不通过认证获取连接数据库对象
    public static MongoDatabase getConnect() {
        //连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");

        //返回连接数据库对象
        return mongoDatabase;
    }

    //需要密码认证方式连接
    public static MongoDatabase getConnectByAuth(String ip, int port, String username, String databaseName, String password) {
        List<ServerAddress> adds = new ArrayList<ServerAddress>();
        //ServerAddress()两个参数分别为 服务器地址 和 端口
        ServerAddress serverAddress = new ServerAddress(ip, port);
        adds.add(serverAddress);

        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(username, databaseName, password.toCharArray());
        credentials.add(mongoCredential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(adds, credentials);

        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

        //返回连接数据库对象
        return mongoDatabase;
    }
}
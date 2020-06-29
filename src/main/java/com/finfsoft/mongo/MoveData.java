package com.finfsoft.mongo;

import com.finfsoft.util.DateUtil;
import com.finfsoft.util.MongoDBUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class MoveData {

    public static final String IP = "114.215.24.232";
    public static final int PORT = 27118;
    public static final String USER_NAME = "lanyue";
    public static final String PASSWORD = "spark#123";
    public static final String DB = "lanyue";

    public static final String L_IP = "52.82.121.92";
    public static final int L_PORT = 27017;
    public static final String L_USER_NAME = "building_user";
    public static final String L_PASSWORD = "finfobuild123";
    public static final String L_DB = "building";

    private static Logger logger = Logger.getLogger(MoveData.class);


    public static void main(String[] args) {
        try {
            //读取文件excel表格数据，保存到集合中
            BufferedReader bufRead = new BufferedReader(new FileReader("E:\\work\\data_config_t.csv"));
            ArrayList dataIdList = new ArrayList<Integer>();
            String line;
            while ((line = bufRead.readLine()) != null) {
                int dataId = Integer.parseInt(line.replace("\"", ""));
                dataIdList.add(dataId);
            }
            //获取mongodb云端(114.215.24.232)数据库连接
            MongoDatabase database = MongoDBUtil.getConnectByAuth(IP, PORT, USER_NAME, DB, PASSWORD);
            //获取mongodb本地(10.88.249.14)数据库连接
            MongoDatabase database1 = MongoDBUtil.getConnectByAuth(L_IP, L_PORT, L_USER_NAME, L_DB, L_PASSWORD);
            //获取mongodb云端(114.215.24.232)'realtime_data'集合
            MongoCollection<Document> realtimeDataConn = database.getCollection("realtime_data");
            //获取mongodb本地(10.88.249.14)'realtime_data'集合
            MongoCollection<Document> localRealtimeDataConn = database1.getCollection("realtime_data");
            //使用mongodb进行条件查询，保存到集合中,然后批量插入
            //根据dataId和dataTime进行查询和批量插入
            for (int i = 0; i < dataIdList.size(); i++) {
                logger.info("开始时间：" + new Date() + " ,查询dataId数组下标为 i=：  " + i + "   dataId:" + dataIdList.get(i));
                BasicDBObject cond1 = new BasicDBObject();
                cond1.put("dataId", new BasicDBObject("$eq", dataIdList.get(i)));
                Date defaultStartDate = DateUtil.StringtoDate("2019-06-01  00:00:00");
                Date nowDate = new Date();
                //相差月份计算
                int months = DateUtil.spaceMonths(defaultStartDate, nowDate);
                long number = insertManyByMonth(months, defaultStartDate, cond1, realtimeDataConn, localRealtimeDataConn);
                logger.info("结束时间：" + new Date() + " ,查询dataId数组i=： " + i + "   dataId:  " + dataIdList.get(i) + "   数量总计： " + number);
            }
        } catch (Exception e) {
            logger.info(e);
        }
    }

    /**
     * 相同'dataId',按月份循环查询和批量插入mongo数据
     *
     * @param months                与当前时间相差月份
     * @param defaultStartDate      mongo云端数据库中第一条数据时间
     * @param cond1                 查询条件dataId
     * @param realtimeDataConn      mongo云端数据库conn
     * @param localRealtimeDataConn mongo本地数据库conn
     * @return 返回同一个dataId在mongo中总记录数
     */
    public static long insertManyByMonth(int months, Date defaultStartDate, BasicDBObject cond1, MongoCollection<Document> realtimeDataConn, MongoCollection<Document> localRealtimeDataConn) {
        //统计本次dataId下的数据总数
        long number = 0;
        for (int j = 0; j < months + 1; j++) {
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(defaultStartDate);
            calendar.add(calendar.MONTH, j); //当前月份
            Date startDate = calendar.getTime();
            calendar.add(calendar.MONTH, 1);
            Date endDate = calendar.getTime(); //下一个月
            BasicDBObject cond2 = new BasicDBObject();
            cond2.put("dataTime", new BasicDBObject("$gte", startDate));
            BasicDBObject cond3 = new BasicDBObject();
            cond3.put("dataTime", new BasicDBObject("$lt", endDate));
            BasicDBList list = new BasicDBList();
            list.add(cond1);
            list.add(cond2);
            list.add(cond3);
            BasicDBObject cond = new BasicDBObject();
            cond.put("$and", list);
            FindIterable<Document> documents = realtimeDataConn.find(cond);
            MongoCursor<Document> iterator = documents.iterator();
            ArrayList<Document> documentslist = new ArrayList<>();
            while (iterator.hasNext()) {
                Document document = iterator.next();
                logger.debug(document);
                documentslist.add(document);
            }
            logger.info("查询时间范围： " + startDate + "到： " + endDate + " 本次时间范围内查询数据个数：" + documentslist.size());
            if (documentslist.size() > 0) {
                //mongo批量插入
                localRealtimeDataConn.insertMany(documentslist);
                logger.info("查询时间范围： " + startDate + "到： " + endDate + " 本次时间范围内批量插入成功！");
            }
            number += documentslist.size();
        }
        return number;
    }
}

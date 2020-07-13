package com.finfsoft.thread;

import com.finfsoft.mongo.MoveData;
import com.finfsoft.util.DateUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class MoveDataThread extends Thread {

    private List dataIdList;
    private MongoDatabase cloudDatabase;
    private MongoDatabase localDatabase;
    private CyclicBarrier cyclicBarrier;

    public MoveDataThread(List dataIdList, MongoDatabase cloudDatabase, MongoDatabase localDatabase, CyclicBarrier cyclicBarrier) {
        this.dataIdList = dataIdList;
        this.cloudDatabase = cloudDatabase;
        this.localDatabase = localDatabase;
        this.cyclicBarrier = cyclicBarrier;
    }

    private static Logger logger = Logger.getLogger(MoveData.class);

    @Override
    public void run() {
        try {
            MongoCollection<Document> realtimeDataConn = cloudDatabase.getCollection("realtime_data");
            MongoCollection<Document> localRealtimeDataConn = localDatabase.getCollection("realtime_data");
            for (int i = 0; i < dataIdList.size(); i++) {
                logger.info("线程：" + Thread.currentThread().getName() + " ，查询dataId数组下标为 i=：  " + i + " ，dataId: " + dataIdList.get(i) + " ，开始时间：" + new Date());
                BasicDBObject cond1 = new BasicDBObject();
                cond1.put("dataId", new BasicDBObject("$eq", dataIdList.get(i)));
                Date nowDate = new Date();
                if (dataIdList.get(i).equals(13137)) {
                    Date date = DateUtil.StringtoDate("2020-02-07 10:36:19");
                    nowDate = date;
                }
                if (dataIdList.get(i).equals(14568)) {
                    Date date = DateUtil.StringtoDate("2019-05-13 10:09:24");
                    nowDate = date;
                }
                if (dataIdList.get(i).equals(13387)) {
                    Date date = DateUtil.StringtoDate("2019-11-11 10:02:14");
                    nowDate = date;
                }
//                if (dataIdList.get(i).equals(14923)) {
//                    Date date = DateUtil.StringtoDate("2019-11-11 09:49:44");
//                    nowDate = date;
//                }
                if (dataIdList.get(i).equals(13528)) {
                    Date date = DateUtil.StringtoDate("2020-06-09 11:27:05");
                    nowDate = date;
                }
                if (dataIdList.get(i).equals(16550)) {
                    Date date = DateUtil.StringtoDate("2020-04-10 11:19:03");
                    nowDate = date;
                }

                long number = insertManyByMonth(cond1, realtimeDataConn, localRealtimeDataConn, (Integer) dataIdList.get(i), nowDate);
                logger.info("线程：" + Thread.currentThread().getName() + " ，查询dataId数组下标为 i=：  " + i + " ，dataId: " + dataIdList.get(i) + " ，结束时间：" + new Date() + ",数量总计： " + number);

            }
            cyclicBarrier.await();
        } catch (Exception e) {
            logger.info(e);
        } finally {
            logger.info("线程：" + Thread.currentThread().getName() + " 停止。");
        }

    }

    /**
     * 相同'dataId',按月份循环查询和批量插入mongo数据
     *
     * @param cond1                 查询条件dataId
     * @param realtimeDataConn      mongo云端数据库conn
     * @param localRealtimeDataConn mongo本地数据库conn
     * @param dataId                当前筛选条件dataId
     * @param nowDate               当前查询的开始时间
     * @return 返回同一个dataId在mongo中总记录数
     */
    public static long insertManyByMonth(BasicDBObject cond1, MongoCollection<Document> realtimeDataConn, MongoCollection<Document> localRealtimeDataConn, Integer dataId, Date nowDate) {
        //统计本次dataId下的数据总数
        long number = 0;
        int months = 0; //当前时间距离之前月份数量
        while (true) {
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(nowDate);
            calendar.add(calendar.DATE, -months * 30); //当前月
            Date startDate = calendar.getTime();
            calendar.add(calendar.DATE, -30);
            Date endDate = calendar.getTime(); //上个月
            //插入时间条件
            BasicDBObject cond2 = new BasicDBObject();
            cond2.put("dataTime", new BasicDBObject("$lte", startDate));
            BasicDBObject cond3 = new BasicDBObject();
            cond3.put("dataTime", new BasicDBObject("$gt", endDate));
            BasicDBList list = new BasicDBList();
            list.add(cond1);
            list.add(cond2);
            list.add(cond3);
            BasicDBObject cond = new BasicDBObject();
            cond.put("$and", list);
            FindIterable<Document> documents = realtimeDataConn.find(cond);
            MongoCursor<Document> iterator = documents.iterator();
            //int documentsNum = 0;
            logger.info("线程：" + Thread.currentThread().getName() + " ，当前dataId:  " + dataId + " ，查询时间范围： " + endDate + "到： " + startDate);
            ArrayList<Document> documentslist = new ArrayList<>();
            while (iterator.hasNext()) {
                Document document = iterator.next();
                documentslist.add(document);
                logger.info("线程：" + Thread.currentThread().getName() + " ,当前dataId:  " + dataId + " ，" + document);
            }
            if (documentslist.size() <= 0) {
                logger.info("线程：" + Thread.currentThread().getName() + " ,当前dataId:  " + dataId + " ，中mongo数据已经全部读取完成,开始插入到mongodb数据库时间是：" + endDate);
                break;
            }
            logger.info("线程：" + Thread.currentThread().getName() + " ，当前dataId:  " + dataId + " ，本次时间范围内查询数据个数：" + documentslist.size());
            if (documentslist.size() > 0) {
                //mongo批量插入
                localRealtimeDataConn.insertMany(documentslist);
                logger.info("线程：" + Thread.currentThread().getName() + " ，当前dataId:  " + dataId + " ，本次时间范围内批量插入成功！");
            }
            number += documentslist.size();
            months++;
        }
        return number;
    }


}

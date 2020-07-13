package com.finfsoft.mongo;

import com.finfsoft.thread.MoveDataThread;
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
import java.util.*;
import java.util.concurrent.CyclicBarrier;

public class MoveData {

    public static final String IP = "114.215.24.232";
    public static final int PORT = 27118;
    public static final String USER_NAME = "lanyue";
    public static final String PASSWORD = "spark#123";
    public static final String DB = "lanyue";

    public static final String L_IP = "10.88.249.14";
    public static final int L_PORT = 19912;
    public static final String L_USER_NAME = "building_user";
    public static final String L_PASSWORD = "finfobuild123";
    public static final String L_DB = "building";

    private static Logger logger = Logger.getLogger(MoveData.class);


    public static void main(String[] args) {

        try {
            //读取文件excel表格数据，保存到集合中
            //BufferedReader bufRead = new BufferedReader(new FileReader("E:\\work\\data_config_t.csv"));
            BufferedReader bufRead = new BufferedReader(new FileReader("/data/mongo/condition/data_config_t.csv"));
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
            //MongoCollection<Document> realtimeDataConn = database.getCollection("realtime_data");
            //获取mongodb本地(10.88.249.14)'realtime_data'集合
            //MongoCollection<Document> localRealtimeDataConn = database1.getCollection("realtime_data");
            //使用mongodb进行条件查询，保存到集合中,然后批量插入
            //根据dataId和dataTime进行查询和批量插入
            //CyclicBarrier保证所有子线程先执行，主线程最后结束，PLAYER_NUM开启线程数
            final int PLAYER_NUM = 2;
            CyclicBarrier cyclicBarrier = new CyclicBarrier(PLAYER_NUM, new Runnable() {
                @Override
                public void run() {
                    logger.info("mongo云端数据迁移到本地10.88.249.12完成！");
                }
            });
            //mongo迁移中断过后开始从list数组下标408位继续迁移
            List nowDataIdList = distinctList(dataIdList);
            int index = nowDataIdList.size() / 2;
            //线程1
            List threadList1 = nowDataIdList.subList(0, index);
            threadList1=distinctList1(threadList1,689);
            threadList1=distinctList1(threadList1,241);
            threadList1=distinctList1(threadList1,123);
            MoveDataThread moveDataThread1 = new MoveDataThread(threadList1, database, database1,cyclicBarrier);
            moveDataThread1.start();
            //线程2
            List threadList2 = nowDataIdList.subList(index, nowDataIdList.size());
            threadList2=distinctList1(threadList2,591);
            threadList2=distinctList1(threadList2,211);
            threadList2=distinctList1(threadList2,61);
            MoveDataThread moveDataThread2 = new MoveDataThread(threadList2, database, database1,cyclicBarrier);
            moveDataThread2.start();
        } catch (Exception e) {
            logger.info(e);
        }

    }

    /**
     * 由于刚开始dataIList没有去重，现在按照log日志去除重复数据
     *
     * @param dataIdList
     * @return
     */
    public static List distinctList(ArrayList dataIdList) {
        List oldList = dataIdList.subList(0, 408);
        List nowList = dataIdList.subList(408, dataIdList.size());
        Set set = new HashSet();
        List newList = new ArrayList();
        for (Iterator nowIter = nowList.iterator(); nowIter.hasNext(); ) {
            Object element = nowIter.next();
            if (set.add(element)) {
                newList.add(element);
            }
        }
        //判断一下现在list中是否存在之前oldList中数据，如果存在删除
        for (Iterator oldIter = oldList.iterator(); oldIter.hasNext(); ) {
            Object element = oldIter.next();
            if (newList.contains(element)) {
                newList.remove(element);
            }
        }
        return newList;
    }

    /**
     * 根据下标截取list
     */
    public static List distinctList1(List dataIdList,int index) {
        return dataIdList.subList(index, dataIdList.size());
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
//    public static long insertManyByMonth(BasicDBObject cond1, MongoCollection<Document> realtimeDataConn, MongoCollection<Document> localRealtimeDataConn, Integer dataId, Date nowDate) {
//        //统计本次dataId下的数据总数
//        long number = 0;
//        int months = 0; //当前时间距离之前月份数量
//        while (true) {
//            Calendar calendar = new GregorianCalendar();
//            calendar.setTime(nowDate);
//            calendar.add(calendar.DATE, -months * 30); //当前月
//            Date startDate = calendar.getTime();
//            calendar.add(calendar.DATE, -30);
//            Date endDate = calendar.getTime(); //上个月
//            //插入时间条件
//            BasicDBObject cond2 = new BasicDBObject();
//            cond2.put("dataTime", new BasicDBObject("$lte", startDate));
//            BasicDBObject cond3 = new BasicDBObject();
//            cond3.put("dataTime", new BasicDBObject("$gt", endDate));
//            BasicDBList list = new BasicDBList();
//            list.add(cond1);
//            list.add(cond2);
//            list.add(cond3);
//            BasicDBObject cond = new BasicDBObject();
//            cond.put("$and", list);
//            FindIterable<Document> documents = realtimeDataConn.find(cond);
//            MongoCursor<Document> iterator = documents.iterator();
//            //int documentsNum = 0;
//            logger.info("查询时间范围： " + endDate + "到： " + startDate);
//            ArrayList<Document> documentslist = new ArrayList<>();
//            while (iterator.hasNext()) {
//                Document document = iterator.next();
//                documentslist.add(document);
//                logger.info(document);
//            }
//            if (documentslist.size() <= 0) {
//                logger.info("当前dataId:  " + dataId + " 中mongo数据已经全部读取完成,开始插入到mongodb数据库时间是：" + endDate);
//                break;
//            }
//            logger.info("本次时间范围内查询数据个数：" + documentslist.size());
//            if (documentslist.size() > 0) {
//                //mongo批量插入
//                localRealtimeDataConn.insertMany(documentslist);
//                logger.info("本次时间范围内批量插入成功！");
//            }
//            number += documentslist.size();
//            months++;
//        }
//        return number;
//    }
}
        /*Date defaultStartDate = DateUtil.StringtoDate("2019-06-01  00:00:00");
        Date nowDate = new Date();
        //相差月份计算
        int months = DateUtil.spaceMonths(defaultStartDate, nowDate);
        long number = insertManyByMonth(months, defaultStartDate, cond1, realtimeDataConn, localRealtimeDataConn);
        logger.info("结束时间：" + new Date() + " ,查询dataId数组i=： " + i + "   dataId:  " + dataIdList.get(i) + "   数量总计： " + number);*/

        /*for (int j = 0; j < months + 1; j++) {
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
                logger.info(document);
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
        return number;*/


//    public static long insertManyByMonth(int months, Date defaultStartDate, BasicDBObject cond1, MongoCollection<Document> realtimeDataConn, MongoCollection<Document> localRealtimeDataConn) {
//        //统计本次dataId下的数据总数
//        long number = 0;
//        for (int j = 0; j < months + 1; j++) {
//            Calendar calendar = new GregorianCalendar();
//            calendar.setTime(defaultStartDate);
//            calendar.add(calendar.MONTH, j); //当前月份
//            Date startDate = calendar.getTime();
//            calendar.add(calendar.MONTH, 1);
//            Date endDate = calendar.getTime(); //下一个月
//            BasicDBObject cond2 = new BasicDBObject();
//            cond2.put("dataTime", new BasicDBObject("$gte", startDate));
//            BasicDBObject cond3 = new BasicDBObject();
//            cond3.put("dataTime", new BasicDBObject("$lt", endDate));
//            BasicDBList list = new BasicDBList();
//            list.add(cond1);
//            list.add(cond2);
//            list.add(cond3);
//            BasicDBObject cond = new BasicDBObject();
//            cond.put("$and", list);
//            FindIterable<Document> documents = realtimeDataConn.find(cond);
//            MongoCursor<Document> iterator = documents.iterator();
//            ArrayList<Document> documentslist = new ArrayList<>();
//            while (iterator.hasNext()) {
//                Document document = iterator.next();
//                logger.info(document);
//                documentslist.add(document);
//            }
//            logger.info("查询时间范围： " + startDate + "到： " + endDate + " 本次时间范围内查询数据个数：" + documentslist.size());
//            if (documentslist.size() > 0) {
//                //mongo批量插入
//                localRealtimeDataConn.insertMany(documentslist);
//                logger.info("查询时间范围： " + startDate + "到： " + endDate + " 本次时间范围内批量插入成功！");
//            }
//            number += documentslist.size();
//        }
//        return number;
//    }


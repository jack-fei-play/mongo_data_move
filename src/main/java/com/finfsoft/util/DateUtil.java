package com.finfsoft.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    //字符串转日期类型
    public static Date StringtoDate(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss");
        Date date = sdf.parse(dateStr);
        return date;
    }

    //日期相差天数
    public static long spaceDays(Date startDate, Date endDate) {
        long days = (endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24);
        return days;
    }

    //日期相差月份个数
    public static int spaceMonths(Date startDate, Date endDate) {
        Calendar from = Calendar.getInstance();
        from.setTime(startDate);
        Calendar to = Calendar.getInstance();
        to.setTime(endDate);
        int fromYear = from.get(Calendar.YEAR);
        int fromMonth = from.get(Calendar.MONTH);
        int toYear = to.get(Calendar.YEAR);
        int toMonth = to.get(Calendar.MONTH);
        return toYear * 12 + toMonth - (fromYear * 12 + fromMonth);
    }

    /**
     * 时间戳转成本机时区的格林尼治时间
     * @param date
     * @return
     */
//    public static String dateLongToiso8601(long date) {
//        DateTime dateTime = new DateTime(date);
//        return dateTime.toString("yyyy-MM-dd'T'HH:mm:ssZ");
//    }


}

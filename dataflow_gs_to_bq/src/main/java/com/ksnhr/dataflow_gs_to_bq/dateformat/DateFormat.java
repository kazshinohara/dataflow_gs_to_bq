package com.ksnhr.dataflow_gs_to_bq.dateformat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.TimeZone;


public class DateFormat {
    // change time format from YYYY-MM-DD'T'HH:mm:ss.SSS to YYYY-MM-dd HH:mm:ss
    // 2013-02-17T23:50:32.000
    // 2018-04-29T01:01:14.000
    public static String convert(String datetime_info) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ENGLISH);
            Date sdfdate = sdf1.parse(datetime_info);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
            String expected_format = sdf2.format(sdfdate);
            return expected_format;
        } catch (Exception e){
            return null;
        }
    }
}
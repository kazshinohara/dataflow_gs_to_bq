package com.ksnhr.dataflow_gs_to_bq.dateformat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.TimeZone;


public class DateFormat {
    // change time format from MM/dd/yyyy hh:mm:ss aa to YYYY-MM-dd HH:mm:ss
    public static String convert(String datetime_info) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa", Locale.ENGLISH);
            Date sdfdate = sdf1.parse(datetime_info);
            SimpleDateFormat sdf2 = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss", Locale.ENGLISH);
            String expected_format = sdf2.format(sdfdate);
            return expected_format;
        } catch (Exception e){
            return null;
        }
    }
}
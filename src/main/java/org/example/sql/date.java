package org.example.sql;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Abram Guo
 * @date 2021-03-27 05:39
 */
public class date {
    public static void main(String[] args){

        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = formatter.format(currentTime);
        System.out.print(dateString);

    }

}

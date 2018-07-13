/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.arkuzo.smartsoft.csv.parser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 *
 * @author arseniy
 */
class DBInserter {
    Connection conn;
    PreparedStatement query;

    public DBInserter(Connection conn) throws SQLException {
        this.conn = conn;
        this.conn = conn;
        this.conn.setAutoCommit(false);
        this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        prepareDatabase();
        prepareStatement();
    }
    
    void insertRecordToDb(String[] r) throws SQLException {
        query.setString(1,  r[0]);
        Timestamp timestamp = new Timestamp((Long.valueOf(r[1])*1000));
        query.setTimestamp(2, timestamp);
        query.setString(3,  r[2]);
        query.setString(4,  r[3]);
        query.setString(5,  r[4]);
        query.setString(6,  r[5]);
        query.setString(7,  r[6]);
        query.setString(8,  r[7]);
        query.setString(9,  r[8]);
        query.setString(10, r[9]);
        query.setString(11, r[10]);
        query.setString(12, representTimestamp(timestamp));
        query.execute();
    }

    private void prepareDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS public.smartsoft;" +
            "CREATE TABLE public.smartsoft(" +
            "    recordid serial PRIMARY KEY," +
            "    ssoid text NOT NULL," +
            "    ts timestamp NOT NULL," +
            "    grp text NOT NULL," +
            "    type text NOT NULL," +
            "    subtype text NOT NULL," +
            "    url text," +
            "    origid text," +
            "    formid text," +
            "    code text," +
            "    ltpa text," +
            "    sudirresponse text," +
            "    ymdh text" +
            ")");
    }

    private void prepareStatement() throws SQLException {
        query = conn.prepareStatement("INSERT INTO public.smartsoft(" +
                "ssoid, ts, grp, type, subtype, url, origid, "
                + "formid, code, ltpa, sudirresponse, ymdh)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    }

    void commit() throws SQLException {
        conn.commit();
    }

    private String representTimestamp(Timestamp timestamp) {
        StringBuilder result = new StringBuilder();
        result.append(String.format("%04d", timestamp.getYear()+1900))
                .append('-').append(String.format("%02d", timestamp.getMonth()+1))
                .append('-').append(String.format("%02d", timestamp.getDate()))
                .append('-').append(String.format("%02d", timestamp.getHours()));
        return new String(result);
    }
    
}

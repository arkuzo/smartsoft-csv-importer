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
import java.text.SimpleDateFormat;

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
        for(int i=2; i<11; i++){
            query.setString(i+1, r[i]);
        }
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
        return new SimpleDateFormat("yyyy-MM-dd-HH").format(timestamp);
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.arkuzo.smartsoft.csv.parser;

import au.com.bytecode.opencsv.CSVReader;
import java.io.*;
import java.sql.*;
import java.util.List;

/**
 *
 * @author arseniy
 */
public class Parser {
    private final File csvFile;
    
    private final DBInserter inserter;

    public Parser(File csvFile, Connection conn) throws SQLException {
        this.csvFile = csvFile;
        this.inserter = new DBInserter(conn);
    }

    public void parse() throws Exception{
        try(CSVReader reader = new CSVReader(new FileReader(csvFile),';','"',1)){
            List<String[]> results = reader.readAll();
            for (String[] r : results){
                inserter.insertRecordToDb(r);
            }
            inserter.commit();
        }
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.arkuzo.smartsoft.csv.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 *
 * @author arseniy
 * 
 */
public class Launcher {
    
    private static File csvFile;
    private static File paramFile;
    private static String dbAddress;
    private static String dbUser;
    private static String dbPassword;
    
    public static void main(String[] args) throws Exception {
        try{
            initArgs(args);
        } catch (Exception e){
            System.out.println(e);
            printUsage();
            return;
        }
        Class.forName("org.postgresql.Driver");
        try(Connection conn = DriverManager.getConnection(dbAddress, dbUser, dbPassword)){
            Parser p = new Parser(csvFile,conn);
            p.parse();
        }
    }

    private static void initArgs(String[] args) throws Exception {
        switch(args.length){
            case 3: if("--param-file".equals(args[1])){
                    paramFile = initFile(args[2]);
                    loadConfiguration();
                } else {
                    throw new Exception("Unknown argument: \""+args[1]+'\"');
                }
                csvFile=initFile(args[0]);
                break;
            default: throw new Exception("Can't parse command line args");
        }
        
    }

    private static File initFile(String arg) throws Exception {
        File file = new File(arg);
        if(!file.exists())
            throw new Exception("File at " + file.getAbsolutePath() + " not found");
        return file;
    }

    private static void loadConfiguration() throws FileNotFoundException, IOException {
        Properties params = new Properties();
        params.load(new FileInputStream(paramFile));
        dbAddress = params.getProperty("dbAddress");
        dbUser = params.getProperty("dbUser");
        dbPassword = params.getProperty("dbPassword");
    }

    private static void printUsage() {
        System.out.println("Usage: <start command> <path to csv> "
                + "--param-file <path to param file>");
    }
    
}

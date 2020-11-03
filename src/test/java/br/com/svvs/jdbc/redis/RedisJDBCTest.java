package br.com.svvs.jdbc.redis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class RedisJDBCTest {
//	br.com.svvs.jdbc.redis.RedisDriver
	public static String getConnectionString() {
        String connUrl = "jdbc:redis://127.0.0.1:32792/0";
        return connUrl;
    }
	public static Connection getConnection() {
        try {
        	Properties prop = new Properties();
        	prop.put("username", "tadpole1");
        	prop.put("password", "tadpole");
        	
//            return DriverManager.getConnection("jdbc:redis://127.0.0.1:32769/0", "", "tadpole");//this.getConnectionString());
            return DriverManager.getConnection(getConnectionString(), prop);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
	
	public static void main(String[] args) {
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try {
    		conn = getConnection();
    		stmt = conn.createStatement();
    		
    		boolean rsm = conn.getAutoCommit();
    		System.out.println(rsm);
    		
    		
    		boolean isExecute = stmt.execute("KEYS **");// my_first_value");
    		rs = stmt.getResultSet();
    		
//    		ResultSet rs = stmt.executeQuery("get tools");
    		while(rs.next()) {
    			System.out.print(rs.getString(0) + ":" );
    			System.out.println(rs.getString(2));
    		}
    	} catch(Exception e) {
    		e.printStackTrace();
    	} finally {
    		if(rs != null) try { rs.close(); } catch(Exception e) {}
    		if(stmt != null) try { stmt.close(); } catch(Exception e) {}
    		if(conn != null) try { conn.close(); } catch(Exception e) {}
    	}
    }

}

package com.ora.api;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleJDBC {

	String jndi;
	String tableName;
	String fileName;
	
	public OracleJDBC(String jndi, String tableName, String fileName ){
		this.jndi=jndi;
		this.tableName=tableName;
		this.fileName=fileName;
		
	}
    public void init() {
    	
    	
        //System.out.println("-------- Oracle JDBC Connection Testing ------");

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();
            return;

        }

        //System.out.println("Oracle JDBC Driver Registered!");

        Connection connection = null;

        try {

        	String[] _jndi=jndi.split("\\|");
        	
           /* connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@10.251.38.228:1521:ODUXP", "uat_mappingtools", "uat_mappingtools");
            */
        	 connection = DriverManager.getConnection(_jndi[0], _jndi[1], _jndi[2]);
             
        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        }

        if (connection != null) {
            //System.out.println("connect!");
            exportTable(connection, tableName, fileName);
            
            // String tableName="PROD10MAY_SERVICES_DATA";
            //exportTable(connection, tableName,"D://webapps/juliet/temp/"+tableName+".sql");
           
            /*tableName="PROD10MAY_SERVICES_TALKMANIA";
            exportTable(connection, tableName,"D://webapps/juliet/temp/"+tableName+".sql");
            */
            
        } else {
            System.out.println("Failed to make connection!");
        }
    }
    
    public static boolean exportTable(Connection con, String tableName, String filename){
    	
    	boolean bool=false;
    	
    	try {
			
    		//step3 create the statement object  
	    	Statement stmt=con.createStatement();  
	    	  
	    	//step4 execute query  
	    	//ResultSet rs=stmt.executeQuery("select * from "+tableName +" where ROWNUM <10");  
	    	ResultSet rstot=stmt.executeQuery("select count(*) as total from "+tableName );  
	    	int rowtot=0;
	    	if(rstot.next())rowtot=rstot.getInt(1);
	    	
	    	ResultSet rs=stmt.executeQuery("select * from "+tableName );  
	    	ResultSetMetaData meta = rs.getMetaData();
	    	String sql="";
			int total=0;
	    	while(rs.next()) {
	    		//System.out.println(rs.getString(1)+"  "+rs.getString(2)+"  "+rs.getString(3)); 
	    		String values="";
	    		for (int i=0;i<meta.getColumnCount();i++) {
	    			//meta.getColumnTypeName(i);
	    			values+="'"+rs.getObject(i+1)+"',";
	    		}
	    		values=values.substring(0,values.length()-1);
	    		
	    		sql+="INSERT INTO "+tableName+" VALUES ("+values+");\n";
	    		/*System.out.print(".");
	    		if(total%30==0)
	    			System.out.println("");*/
	    		long progress=(total*100/rowtot);
	    		System.out.println(progress+" %");
	    		
	    		total++;
	    	}
	    	
	    	 FileUtils.writeToTextFile(filename, sql);
	    	 System.out.println("total:"+total);
	    	 System.out.println("Alhamdulillah");
	    	 
	    	//step5 close the connection object  
	    	con.close();  
    	  
    	} catch (Exception e) {
			// TODO: handle exception
    		System.out.println(e.getMessage());
		}
    	
    	return bool;
    }

}
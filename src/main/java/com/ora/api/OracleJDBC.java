package com.ora.api;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleJDBC {

	String jndi;
	String tableName;
	String newTableName;
	
	String fileName;
	
	public OracleJDBC(String jndi, String tableName, String newTableName, String fileName ){
		this.jndi=jndi;
		this.tableName=tableName;
		this.newTableName=newTableName;
		
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

        	String[] _jndi=jndi.split(";");
        	
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
            exportTable(connection, tableName, newTableName, fileName);
            /*tableName="PROD10MAY_SERVICES_TALKMANIA";
            exportTable(connection, tableName,"D://webapps/juliet/temp/"+tableName+".sql");
            */
            
        } else {
            System.out.println("Failed to make connection!");
        }
    }
    
    public static boolean exportTable(Connection con, String tableName, String newTableName, String filename){
    	
    	boolean bool=false;
    	
    	try {
			
    		//step3 create the statement object  
	    	Statement stmt=con.createStatement();  
	    	  
	    	//query schema
	    	String schema="";
	    	ResultSet rschema=stmt.executeQuery("select DBMS_METADATA.GET_DDL('TABLE','"+tableName+"') from DUAL where ROWNUM <2");  
	    	if(rschema.next())
	    		schema=clobToString(rschema.getClob(1));
	    	
	    	//build mysql
	    	String _mysqlSchema=schema.substring(schema.indexOf("("),schema.indexOf("SEGMENT"));
	    	_mysqlSchema=_mysqlSchema.replaceAll("\"","`").replaceAll("VARCHAR2","TEXT").replaceAll("CLOB", "TEXT").replaceAll(" CHAR\\)",")");
	    	_mysqlSchema= "CREATE TABLE "+newTableName+"\n"+_mysqlSchema+";";
	    	
	    	
	    	//System.exit(0);
	    	
	    	
	    	
	    	
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
	    			//System.out.println(meta.getColumnTypeName(i+1));
	    			if(meta.getColumnTypeName(i+1).equals("CLOB")){
	    				Clob a=rs.getClob(i+1);
	    				if(a!=null)values+="'"+clobToString(a)+"',";
	    				else values+="NULL,";
	    				
	    			}
	    			else{
	    				Object a=rs.getObject(i+1);
	    				if(a!=null){
	    					a=a.toString().replaceAll("'","");
		    				values+="'"+a+"',";
	    				}
	    				else values+="NULL,";
	    				
	    				//values+=rs.getObject(i+1)+"",";
	    				//System.out.println(values);
	    	    		
	    			}
		    				
	    		}
	    		values=values.substring(0,values.length()-1);
	    		
	    		sql+="INSERT INTO "+newTableName+" VALUES ("+values+");\n";
	    		/*System.out.print(".");
	    		if(total%30==0)
	    			System.out.println("");*/
	    		long progress=(total*100/rowtot);
	    		System.out.println(progress+" %");
	    		
	    		total++;
	    	}
	    	
	    	//append schema table
	    	 sql=_mysqlSchema+"\n"+sql;
	    	
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
    
    
    private static String clobToString(java.sql.Clob data)
    {
        final StringBuilder sb = new StringBuilder();

        try
        {
            final Reader  reader = data.getCharacterStream();
            final BufferedReader br = new BufferedReader(reader);

            int b;
            while(-1 != (b = br.read()))
            {
                sb.append((char)b);
            }

            br.close();
        }
        catch (SQLException e)
        {
            System.out.println("SQL. Could not convert CLOB to string");
            return e.toString();
        }
        catch (IOException e)
        {
        	System.out.println("IO. Could not convert CLOB to string");
            return e.toString();
        }

        return sb.toString();
    }

}
package com.ora.api;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Bismillah" );
       
        if(args.length>0){
        	
        }else{
        	args = new String[3];
        
	        args[0]="jdbc:oracle:thin:@10.251.38.228:1521:ODUXP|uat_mappingtools|uat_mappingtools";
	        args[1]="PROD10MAY_SERVICES_TALKMANIA";
	        args[2]="D://webapps/juliet/temp/"+args[1]+".sql";
        }
        
        OracleJDBC ora=new OracleJDBC(args[0],args[1],args[2]);
        ora.init();
        
    }
}

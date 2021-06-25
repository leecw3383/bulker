package com.rayful.localize;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Localization {

	/** Localize Language */
	public static final String LOCLALIZE_KR = "KR";
	public static final String LOCLALIZE_EN = "EN";
	public static final String LOCLALIZE_US = "US";
	
	public static HashMap<String,String> mapMessage = null;
	
	/**
	* LogBundle 파일로 부터 메세지를 읽어들여 Localization 
	* 변수에 설정값을 세팅한다.
	* <p>
	* @param	sPropFileName	LogBundle 파일이 있는 경로명
	*/		
	public static void load( String sPropFileName )
	throws FileNotFoundException, IOException, Exception
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream( sPropFileName )));
		
		String sMessage = null;
		String sReplaceStr = null;

		mapMessage = new HashMap<String, String>();
		
		while( (sMessage=br.readLine() ) != null ) {
			//System.out.println( "message1 : " + sMessage);
			
			String aMessage[] = sMessage.split("=");
			
			if ( aMessage.length == 2 ) {
				sReplaceStr = aMessage[1];
				sReplaceStr = sReplaceStr.replace("%rn", "\r\n");    // \r\n 으로 변경하여 반환한다.
				mapMessage.put(aMessage[0], sReplaceStr);
			}
			else if ( aMessage.length > 2) {
				//System.out.println( "message : " + sMessage.substring( sMessage.indexOf("=") + 1) );
				sReplaceStr = sMessage.substring( sMessage.indexOf("=") + 1 );
				   
				sReplaceStr = sReplaceStr.replace("%rn", "\r\n");    // \r\n 으로 변경하여 반환한다.
				mapMessage.put(aMessage[0], sReplaceStr );
			}
		}
		
		br.close();
	}
	
	public static String getMessage(String sMessageCode)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, String sMsg)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", sMsg);
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, int iMsg)
	{
		String sMessage = null;
		
		try {
			sMessage = getMessage( sMessageCode, Integer.toString(iMsg) );
		} catch(Exception e) {
			sMessage = "No Message";
		}
		
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, String sMsg1, String sMsg2)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", sMsg1).replaceFirst("%s", sMsg2);
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, String sMsg1, String sMsg2, int iMsg)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", sMsg1).replaceFirst("%s", sMsg2).replaceFirst("%s", Integer.toString(iMsg));
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, int iMsg, String sMsg)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", Integer.toString(iMsg)).replaceFirst("%s", sMsg = sMsg == null ? "" : sMsg);
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, String sMsg, int iMsg)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", sMsg).replaceFirst("%s", Integer.toString(iMsg));
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
	public static String getMessage(String sMessageCode, String sMsg1, int iMsg1, int iMsg2)
	{
		String sMessage = null;
		
		try {
			sMessage = mapMessage.get( sMessageCode );
			sMessage = sMessage.replaceFirst("%s", sMsg1).replaceFirst("%s", Integer.toString(iMsg1)).replaceFirst("%s", Integer.toString(iMsg2));
		} catch(Exception e) {
			sMessage = "No Message";
		}
		return sMessage;
	}
	
}

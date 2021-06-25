package com.rayful.bulk.index.indexer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(checkValidateDate("2011-07-04T00:00:00Z"))
			System.out.println("Validate Date");
		else
			System.out.println("No Validate Date");
	}
	
	public static boolean checkValidateDate(String checkDate) 
	{
		boolean bCheck = false;

		//checkDate = checkDate.replaceAll("-", "");
		checkDate = checkDate.replace("T", " ").replace("Z", "");
		
		//SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", java.util.Locale.UK);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		//일자, 시각해석을 엄밀하게 실시할지 설정함      
		//true일 경우는 엄밀하지 않는 해석, 디폴트       
		formatter.setLenient ( false );
		try {
			
			System.out.println(" > CheckDate : " + checkDate);
			
			//Date formatDate = formatter.parse(checkDate);
			System.out.println("Convert Date : " + dateFormatString(formatter.parse(checkDate),"yyyy-MM-dd", "fr", "FR"));
			
			bCheck = true;
		} catch (java.text.ParseException e){
			e.printStackTrace();
			bCheck = false;
		}

		return bCheck;
	}
	
	public static String dateFormatString ( Date oDate, String sDateFormat, String language, String country ) {
		SimpleDateFormat oSdf = new SimpleDateFormat( sDateFormat, new Locale(language, country, "") );
		String sDate = oSdf.format( oDate );
		return sDate;
	}
	/*
    static public final Locale SIMPLIFIED_CHINESE = new Locale("zh","CN","");
    static public final Locale TRADITIONAL_CHINESE = new Locale("zh","TW","");
    static public final Locale FRANCE = new Locale("fr","FR","");
    static public final Locale GERMANY = new Locale("de","DE","");
    static public final Locale ITALY = new Locale("it","IT","");
    static public final Locale JAPAN = new Locale("ja","JP","");
    static public final Locale KOREA = new Locale("ko","KR","");
    static public final Locale CHINA = new Locale("zh","CN","");
    static public final Locale PRC = new Locale("zh","CN","");
    static public final Locale TAIWAN = new Locale("zh","TW","");
    static public final Locale UK = new Locale("en","GB","");
    static public final Locale US = new Locale("en","US","");
    static public final Locale CANADA = new Locale("en","CA","");
    static public final Locale CANADA_FRENCH = new Locale("fr","CA","");
	*/
}

/**
 *******************************************************************
 * 파일명 : SSTypes.java
 * 파일설명 : SearchServer 컬럼타입 클래스 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/08   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

/**
 * SearchServer 컬럼타입 정의
*/
public class ESPTypes {
	
	/** ESP Numeric Type */
	public static final int NONE = -1;
	
	/** ESP Numeric Type */
	public static final int INTEGER = 1;
	
	/** ESP Numeric Type */
	public static final int REAL = 2;		

	/** ESP Datetime Type */
	public static final int DATE = 3;
	
	/** ESP String Type */
	public static final int STRING = 4;	
	
	/** ESP String Type */
	public static final int LONGTEXT = 5;
	
	/** ESP DOM Element Type */
	public static final int DOMELEMENT = 6;

	
	/** 
	* 타임스탬프 포맷의 스트링을 long형값으로 변환
	* <p>
	* @param	sTimestamp 스트링으로 타임스탬프값 (YYYY/MM/DD HH:Mi:ss)
	* @return	숫자값으로 변환한 값
	*/
	public static long dateStringToLong( String sTimestamp ) 
	{
		java.sql.Timestamp oTimestamp = null;
		String sCnvTimeStamp = null;

		sCnvTimeStamp = sTimestamp.replaceAll( "/", "-" );
		try {
			if ( sTimestamp.length() <= 10 ) {
				oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp + " 00:00:01" );
			} else if ( sTimestamp.indexOf("AM") >= 0 ||  sTimestamp.indexOf("PM") >= 0 ) {
				sCnvTimeStamp = sCnvTimeStamp.replaceFirst("AM", "");
				sCnvTimeStamp = sCnvTimeStamp.replaceFirst("PM", "");
				sCnvTimeStamp = sCnvTimeStamp.trim();
				oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp  );
			} else {
				oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp );
			}
			
			return (long) (oTimestamp.getTime() /1000);
		} catch ( IllegalArgumentException iae ) {
			return 0;
		}
	}
	
	/** 
	* long형 타임스탬프값을 타임스탬프 포맷의 스트링으로 변환
	* <p>
	* @param	lDate long형 타임스탬프값
	* @return	타임스탬프 포맷의 스트링 (YYYY/MM/DD HH:Mi:ss)
	*/	
	public static String longToDateString ( long lDate )
	{
		java.sql.Timestamp oTimeStamp = new java.sql.Timestamp( lDate * 1000 );
		return oTimeStamp.toString();
	}
	
	/** 
	* date형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	* <p>
	* @return	sDateFormat형태로 변환된 날짜스트링
	*/	
	public static String dateFormatString ( Date oDate, String sDateFormat, String sTimeZone ) {
		
		sTimeZone = sTimeZone == null ? "" : sTimeZone;
		
		SimpleDateFormat oSdf = new SimpleDateFormat( sDateFormat );
		oSdf.setTimeZone ( TimeZone.getTimeZone ( sTimeZone ) );
		String sDate = oSdf.format( oDate );
		return sDate;
	}
	
	/** 
	* date형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	* <p>
	* @return	sDateFormat형태로 변환된 날짜스트링
	*/	
	public static String dateFormatString ( Date oDate, String sDateFormat ) {
		SimpleDateFormat oSdf = new SimpleDateFormat( sDateFormat );
		String sDate = oSdf.format( oDate );
		return sDate;
	}
	
	/** 
	* date형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	* <p>
	* @return	sDateFormat형태로 변환된 날짜스트링
	*/	
	public static String dateFormatString ( Date oDate ) {
		return dateFormatString ( oDate , "yyyy-MM-dd HH:mm:ss");
	}
	
	
	/** 
	* Timestamp형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	* <p>
	* @return	sDateFormat형태로 변환된 날짜스트링
	*/	
	public static String dateFormatString ( java.sql.Timestamp oTimestamp, String sDateFormat ) {
		return dateFormatString ( new java.util.Date( oTimestamp.getTime()) , sDateFormat);
	}
	
	/** 
	* Timestamp형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	* <p>
	* @return	sDateFormat형태로 변환된 날짜스트링
	*/	
	public static String dateFormatString ( java.sql.Timestamp oTimestamp ) {
		return dateFormatString ( new java.util.Date( oTimestamp.getTime()) , "yyyy-MM-dd HH:mm:ss");
	}
	
	
	/** 
	* 타임스탬프 포맷의 스트링을 java.sql.Timestamp 형값으로 변환
	* <p>
	* @param	sTimestamp 스트링으로 타임스탬프값 (YYYY/MM/DD HH:Mi:ss)
	* @return	java.sql.Timestamp 객체
	*/
	public static String dateFormatString( String sTimestamp, String sDateFormat ) 
	throws IllegalArgumentException
	{
		java.sql.Timestamp oTimestamp = null;
		String sCnvTimeStamp = null;

		sCnvTimeStamp = sTimestamp.replaceAll( "/", "-" );
		if ( sTimestamp.length() <= 10 ) {
			oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp + " 00:00:01" );
		} else if ( sTimestamp.indexOf("AM") >= 0 ||  sTimestamp.indexOf("PM") >= 0 ) {
			sCnvTimeStamp = sCnvTimeStamp.replaceFirst("AM", "");
			sCnvTimeStamp = sCnvTimeStamp.replaceFirst("PM","");
			sCnvTimeStamp = sCnvTimeStamp.trim();
			oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp  );
		} else {
			oTimestamp = java.sql.Timestamp.valueOf( sCnvTimeStamp );
		}
		return  dateFormatString( oTimestamp, sDateFormat );
	}
	
	/** 
	* 타임스탬프 포맷의 스트링을 java.sql.Timestamp 형값으로 변환
	* <p>
	* @param	sTimestamp 스트링으로 타임스탬프값 (YYYY/MM/DD HH:Mi:ss)
	* @return	java.sql.Timestamp 객체
	*/
	public static String dateFormatString( String sTimestamp ) 
	throws IllegalArgumentException
	{
		return  dateFormatString( sTimestamp, "yyyy-MM-dd HH:mm:ss" );
	}	

}
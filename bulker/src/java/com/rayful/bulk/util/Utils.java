package com.rayful.bulk.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
//java 1.8에서 지원되는 형식으로 변경
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import org.apache.log4j.Logger;
//java 1.8 에서 지원되지 않아 주석 처리
//import sun.net.www.protocol.http.HttpURLConnection;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.Crypto;
import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.logging.RayfulLogger;

public class Utils {

	static Logger logger = RayfulLogger.getLogger( Utils.class.getName(), Config.LOG_PATH );
	
	/** 
	 * 16진수로 변경
	 */
	public String toHexString(int x, int length) {
		StringBuffer s = new StringBuffer(Integer.toHexString(x));
		int y = s.length();
		for (int i = 0; i < (length - y); i++) {
			s.insert(0, '0');
		}
		return s.toString();
	}
    
	/**
	 * 파일 이름에서 확장자 구하기
	 */
	public String getFileNameExtension(String sFileName) {
		String sExtension = "";
		int iPos = 0;

		if (sFileName != null && sFileName.trim().length() > 0 ) {

			iPos = sFileName.lastIndexOf(".");

			if (iPos > 0) {
				sExtension = sFileName.substring(iPos+1);
				if (sExtension.length() > 0) sExtension = sExtension.toUpperCase();
			}
		} 

		return sExtension;
	}
    
	/**
	 * Crypto를 사용하여 Encrypt 된 문자열을 Decrypt
	 */
	public String Decrypt(String sEncrypt, String sBool) {
		String sRtn = null;

		try {
			if (sBool == null) {
				sRtn = sEncrypt;
			}
			else if (sBool.equalsIgnoreCase("true")) {
				sRtn = Crypto.deCrypt(sEncrypt);
    		}
			else { 
    			sRtn = sEncrypt;
			}
		} catch (Exception e) {
			sRtn = "";
		}

		return sRtn;
	}
    
	/** 
	 * date형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	 * <p>
	 * @return	sDateFormat형태로 변환된 날짜스트링
	 */	
	public String dateFormatString ( Date oDate, String sDateFormat ) {
		SimpleDateFormat oSdf = new SimpleDateFormat( sDateFormat );
		String sDate = oSdf.format( oDate );
		return sDate;
	}

	/** 
	 * date형 값을 sDateForamt에 맞게 스트링으로 변환한다.
	 * <p>
	 * @return	sDateFormat형태로 변환된 날짜스트링
	 */	
	public String dateFormatString ( Date oDate ) {
		return dateFormatString ( oDate , "yyyy-MM-dd HH:mm:ss");
	}

	/** 
	 * 타임스탬프 포맷의 스트링을 long형값으로 변환
	 * <p>
	 * @param sTimestamp 스트링으로 타임스탬프값 (YYYY/MM/DD HH:Mi:ss)
	 * @return   숫자값으로 변환한 값
	 */
	public long dateStringToLong( String sTimestamp ) 
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
	       
		return (long) (oTimestamp.getTime() /1000);
	}

	/** 
	 * 타임스탬프 포맷의 스트링이 유효한 날짜 포맷인지 확인
	 * <p>
	 * @param checkDate 스트링으로 타임스탬프값 (YYYY-MM-DD)
	 * @return 유효 여부를 true/false로 반환
	 */
	public boolean checkValidateDate(String checkDate) 
	{
		boolean bCheck = false;

		checkDate = checkDate.replaceAll("-", "");

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMDD", java.util.Locale.KOREA);

		//일자, 시각해석을 엄밀하게 실시할지 설정함      
		//true일 경우는 엄밀하지 않는 해석, 디폴트       
		formatter.setLenient ( false );
		try {
			if ( logger.isDebugEnabled() ) {
				logger.debug( Utils.class.getName() + " > CheckDate : " + checkDate);
			}
			//Date formatDate = formatter.parse(checkDate);
			formatter.parse(checkDate);
			bCheck = true;
		} catch (java.text.ParseException e){             
			bCheck = false;
		}

		return bCheck;
	}

	/**
	 * 현재 시스템의 날짜를 yyyyMMddHHmmss.SSS 형식으로 구한다.
	 * <p>
	 * @return 현재 시스템의 날짜를 타임스탬프 포맷의 문자열로 반환
	 */
	public String getCurrentTime() 
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		String currentTime = formatter.format(new java.util.Date(System.currentTimeMillis()));

		return currentTime;
	}

	/**
	 * 현재 시스템의 날짜를 입력한 포맷 형식으로 구한다.
	 * 입력한 포맷 형식이 없으면 yyyy/MM/dd 형식으로 구한다.
	 * <p>
	 * @param sFormat 날짜 포맷 형식(yyyy-MM-dd)
	 * @return 타임스탬프 포맷의 문자열로 반환
	 */
	public String getCurrentDate(String sFormat) 
	{
		sFormat = sFormat == null ? "yyyy/MM/dd" : sFormat.trim().length() == 0 ? "yyyy/MM/dd" : sFormat;

		SimpleDateFormat formatter = new SimpleDateFormat(sFormat);
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

		String currentDate = formatter.format(new java.util.Date(System.currentTimeMillis()));

		return currentDate;
	}

	/**
	 * 현재 시스템의 날짜를 구하고 입력 받은 시간을 더한다.
	 * 입력한 포맷 형식이 없으면 yyyy/MM/dd 형식으로 구한다.
	 * <p>
	 * @param sFormat 날짜 포맷 형식(yyyy-MM-dd)
	 * @param millis 현재 시스템 날짜에 더하고 싶은 값
	 * @return 타임스템프 포맷의 문자열로 반환
	 */
	public String getDate(String sFormat, long millis)
	{
		sFormat = sFormat == null ? "yyyy/MM/dd" : sFormat.trim().length() == 0 ? "yyyy/MM/dd" : sFormat;

		SimpleDateFormat formatter = new SimpleDateFormat(sFormat);
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

		String currentDate = formatter.format(new java.util.Date(System.currentTimeMillis() + millis));

		return currentDate;
	}

	/**
	 * 특정 문자열( ' | " | -- )을 빈 문자열로 변환한다.
	 * <p>
	 * @param sSource 변환하고자 하는 문자열
	 * @return 문자열중에서 특정 문자열을 빈 문자열로 변환하고 반환한다.
	 */
	public String ChangeValidString(String sSource)
	{
		String sChangeStr = "";

		if ( sSource == null ) {
			return "";
		}
		else {
			sChangeStr = sSource.trim();

			String sRegEx = "'|\"|(--){1}";

			Pattern pattern = Pattern.compile(sRegEx);

			Matcher matcher = pattern.matcher(sChangeStr);

			while(matcher.find()) {
				sChangeStr = sChangeStr.replaceFirst(sRegEx, "");
			}
		}
	      
		return sChangeStr;
	}

	/**
	 * 특정 문자열( ' | " | -- )을 빈 문자열로 변환한다.
	 * <p>
	 * @param sSource 변환하고자 하는 문자열
	 * @return 문자열중에서 특정 문자열을 빈 문자열로 변환하고 반환한다.
	 */
	public String DataValidString(String sSource)
	{
		String sChangeStr = "";

		if ( sSource == null ) {
			return "";
		}
		else {
			sChangeStr = sSource.trim();

			String sRegEx = "'|\"|(--){1}";

			Pattern pattern = Pattern.compile(sRegEx);

			Matcher matcher = pattern.matcher(sChangeStr);

			while(matcher.find()) {
				sChangeStr = sChangeStr.replaceFirst(sRegEx, "");
			}
		}

		return sChangeStr;
	}

	/**
	 * 윤년인지 여부를 체크한다.
	 * <p>
	 * @param year 체크하고자 하는 년도
	 * @return 윤년 여부를 true/false로 반환한다. 
	 */
	public boolean isLeapYear(int year) {
		return ((year%4==0)&&(year%100!=0)||(year%400==0));
	} 

	/**
	 * 시작일과 마지막일을 체크한다.
	 * <p>
	 * @param startDate 시작일(타임스템프 포맷의 문자열)
	 * @param endDate 마지막일(타임스템프 포맷의 문자열) 
	 * @return 날짜 체크 여부를 반환한다.( 0 : 정상, -1 : 시작날짜가 큰 오류, -2 : 윤년체크 오류 )
	 */
	public String checkDate(String startDate, String endDate) { 
		String ymdCheck = "0"; //0 : 정상, -1 : 시작날짜가 큰 오류, -2 : 윤달체크 오류

		int snumOfLeapYear = 0;       // 윤년일 경우 시작일
		int enumOfLeapYear = 0;       // 윤년일 경우 종료일
		//int numOfLeapYear = 0;        // 평년일 경우

		int[] endOfYear = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}; 
		int[] endOfMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

		int yearschk = 0;
		int yearechk = 0;

		startDate = startDate == null ? "" : startDate;
		endDate = endDate == null ? "" : endDate;

		//구분자를  "-"으로 변경한다.
		for( int n = 0; n < startDate.length(); n++ ){
			yearschk++;
			char check = startDate.charAt(n);
			if( check == '/' || check == '.'){ 
				startDate = startDate.replace(startDate.substring(n, n+1), "-");
			}
			if ( yearschk > startDate.length()-1 ){
				break;
			}
		}

		for( int n = 0; n < endDate.length(); n++ ){
			yearechk++;
			char check = endDate.charAt(n);
			if( check == '/' || check == '.'){
				endDate = endDate.replace(endDate.substring(n, n+1), "-");
			}
			if ( yearechk > 8 ){
				break;
			}
		}

		//년도 자릿수를 체크.
		if ( startDate != null ) {
			if ( startDate.indexOf("-", 1) < 4 ){
				return ymdCheck = "-4";
			}
		}

		if ( endDate != null ) {
			if ( endDate.indexOf("-", 1) < 4 ){
				return ymdCheck = "-4";
			}
		}

		//int sstmp = startDate.indexOf("-", 1);    //'-'의 최초 자릿수를 찾는다.
		//int sltmp = startDate.lastIndexOf("-");   //'-'의 마지막 자릿수를 찾는다.
		//int stot = startDate.length();            //시작날짜의 총자릿수 찾는다.
		//int stmtmp = sstmp-sltmp;                 //시작 월 자릿수
		//int stdtmp = sltmp-stot;                  //시작 자릿수

		//int estmp = endDate.indexOf("-", 1);   //'-'의 최초 자릿수를 찾는다.
		//int eltmp = endDate.lastIndexOf("-");  //'-'의 마지막 자릿수를 찾는다.
		//int etot = endDate.length();           //종료날짜의 총자릿수 찾는다.
		//int etmtmp = estmp-eltmp;              //종료 월 자릿수
		//int etdtmp = eltmp-etot;               //종료 일 자릿수

		String sy = "";
		String sm = "";
		String sd = "";

		String ey = "";
		String em = "";
		String ed = "";

		try {
			//날짜에 자릿수 확인해서 한자리로 들어올경우 두자리로 만들어준다.
			/*
			sy = startDate.substring(0, 4);

			if( stmtmp == -2 ){
				sm = "0"+startDate.substring(5, 6);
			}
			else {
				sm = startDate.substring(5, 7);
			}

			if( stdtmp == -2 ){
				sd = "0"+startDate.substring(sltmp+1,stot);
			}
			else {
				sd = startDate.substring(sltmp+1,stot);
			}

			ey = endDate.substring(0, 4);
			if( etmtmp == -2 ){
				em = "0"+endDate.substring(5, 6);
			}
			else {
				em = endDate.substring(5, 7);
			}

			if( etdtmp == -2 ){
				ed = "0"+endDate.substring(eltmp+1,etot);
			}
			else {
				ed = endDate.substring(eltmp+1,etot);
			}
			*/

			if ( startDate !=null && endDate !=null ) {
				String aStart[] = startDate.split("-");
				String aEnd[] = endDate.split("-");

				//날짜에 자릿수 확인해서 한자리로 들어올경우 두자리로 만들어준다.
				if ( startDate != null && endDate != null ) {
					if ( aStart.length == 3 ) {
						sy = aStart[0];
						sm = aStart[1];
						sd = aStart[2];

						if ( sm !=null && sm.length() == 1 ) {
							sm = "0" + sm;
						}

						if ( sd !=null && sd.length() == 1 ) {
							sd = "0" + sd;
						}
					}

					if ( aEnd.length == 3 ) {
						ey = aEnd[0];
						em = aEnd[1];
						ed = aEnd[2];

						if ( em !=null && em.length() == 1 ) {
							em = "0" + em;
						}

						if ( ed !=null && ed.length() == 1 ) {
							ed = "0" + ed;
						}
					}
				}
			}

			if ( logger.isDebugEnabled() ) {
				logger.debug( Utils.class.getName() + " > ************Util****chekDatet***sm****"+sm +"******sd***"+sd );   
				logger.debug( Utils.class.getName() + " > ************Util****chekDatet***em****"+em+"******ed***"+ed );
			}

			if(sm.equals("00") || sm.equals("0")  || sm.equals("") ){
				return ymdCheck = "-4";            
			}if(em.equals("00") || em.equals("0")  || em.equals("") ){
				return ymdCheck = "-4";            
			}
			if(sd.equals("00") || sd.equals("0")  || sd.equals("")){
				return ymdCheck = "-4";            
			}if(ed.equals("00") || ed.equals("0")  || ed.equals("") ){
				return ymdCheck = "-4";            
			}

			// 입력된 날짜가 숫자인지 확인한다.      
			String chkstemp = sy+sm+sd;
			String chketemp = ey+em+ed;
	                  
			int sntmp =0;
			int sdm =0;

			int entmp =0;
			int edm =0;

			//윤년인지 확인한다.       
			//시작일 숫자가 아닌 문자열이 들어올 경우
			for( int j = 0; j < sy.length(); j++ ){
				sdm++;
				char check = sy.charAt(j);
				if( 0x30 <= check && check <= 0x39 ){
					sntmp++;
				}
				if (sntmp != sdm){
					break;
				}
			}
			if (sntmp != sdm){
				return ymdCheck = "-3";
			}
			else {
				if( isLeapYear(Integer.parseInt(sy)) ){
					snumOfLeapYear=1; //윤년인것을 알기 위해 증가시킨다.         
				} 
			}

			//종료일 숫자가 아닌 문자열이 들어올 경우
			for( int j = 0; j < ey.length(); j++ ){
				sdm++;
				char check = ey.charAt(j);
				if( 0x30 <= check && check <= 0x39 ){
					sntmp++;
				}
				if (sntmp != sdm){
					break;
				}
			}
			if (sntmp != sdm){
				return ymdCheck = "-3";
			} 
			else {
				if( isLeapYear(Integer.parseInt(ey)) ){
					enumOfLeapYear=1; //윤년인것을 알기 위해 증가시킨다.         
				} 
			}

			if ( snumOfLeapYear !=0 && Integer.parseInt(sm) == 2 ){
				// 숫자가 아닌 문자열이 들어올 경우
				for( int j = 0; j < chkstemp.length(); j++ ){
					sdm++;
					char check = chkstemp.charAt(j);
					if( 0x30 <= check && check <= 0x39 ){
						sntmp++;
					}
					if (sntmp != sdm){
						break;
					}
				}
				if (sntmp != sdm){
					return ymdCheck = "-3";
				}

				if ( Integer.parseInt(sd) > 29 ) {
					return ymdCheck = "-2";
				}
			} else{    //윤년의 2월을 뺀 나머지.
				// 숫자가 아닌 문자열이 들어올 경우
				for( int j = 0; j < chkstemp.length(); j++ ){
					sdm++;
					char check = chkstemp.charAt(j);
					if( 0x30 <= check && check <= 0x39 ){
						sntmp++;
					}
					if (sntmp != sdm){
						break;
					}
				}
				if (sntmp != sdm){
					return ymdCheck = "-3";
				}

				//평년의 시작일
				for(int i = 0; i < 12; i++){
					if( Integer.parseInt(sm) == endOfYear[i] ){
						//시작일의 일수가 비정상적으로 큰 수일경우
						if ( Integer.parseInt(sd) > endOfMonth[i] ) {                     
							return ymdCheck = "-5";
						}
					}
				}            
			}

			if ( enumOfLeapYear !=0 && Integer.parseInt(em) == 2 ){

				// 숫자가 아닌 문자열이 들어올 경우
				for( int j = 0; j < chketemp.length(); j++ ){
					edm++;
					char check = chketemp.charAt(j);
					if( 0x30 <= check && check <= 0x39 ){
						entmp++;
					}
					if (entmp != edm){
						break;
					}
				}
				if (entmp != edm){
					return ymdCheck = "-3";
				}

				if ( Integer.parseInt(ed) > 29 ) {
					return ymdCheck = "-2";
				}
			} else {   //윤년의 2월을 뺀 나머지.

				// 숫자가 아닌 문자열이 들어올 경우
				for( int j = 0; j < chketemp.length(); j++ ){
					edm++;
					char check = chketemp.charAt(j);
					if( 0x30 <= check && check <= 0x39 ){
						entmp++;
					}
					if (entmp != edm){
						break;
					}
				}
				if (entmp != edm){
					return ymdCheck = "-3";
				}

				//평년의 종료일
				for(int i = 0; i < 12; i++){
					if( Integer.parseInt(em) == endOfYear[i] ){
						//종료일의 일수가 비정상적으로 큰 수일경우
						if ( Integer.parseInt(ed) > endOfMonth[i] ) {
							return ymdCheck = "-5";
						}
					}
				}
			}  

			/*
			long lStart = 0;
			long lEnd = 0;

			try {
				lStart = dateStringToLong(sy+"-"+sm+"-"+sd);
				lEnd = dateStringToLong(ey+"-"+em+"-"+ed);
			} catch(IllegalArgumentException ie) {
			}
			*/
			
			String DSchk = sy+sm+sd;
			String DEchk = ey+em+ed;

			if( Integer.parseInt(DSchk) > Integer.parseInt(DEchk) ){
				return ymdCheck = "-1";
			}

		} catch (java.lang.NumberFormatException e){
			logger.error( Utils.class.getName() + " > NumberFormatException test Exception" );
		}
		finally {} 

		return ymdCheck;
	}

	/**
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public String ycheckDate(String startDate, String endDate)
	{ 
		String ymdCheck = "0"; //0 : 정상, -1 : 윤달체크 오류

		//int snumOfLeapYear = 0;       // 윤년일 경우 시작일
		//int enumOfLeapYear = 0;       // 윤년일 경우 종료일
		//int numOfLeapYear = 0;        // 평년일 경우

		int[] endOfYear = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}; 
		int[] endOfMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

		//년도 자릿수를 체크.
		String sy = "";
		String sm = "";
		String sd = "";

		String ey = "";
		String em = "";
		String ed = "";

		try{
			startDate = startDate == null ? "" : startDate;
			endDate = endDate == null ? "" : endDate;

			if ( startDate != null && endDate != null ) {
				String aStart[] = startDate.split("-");
				String aEnd[] = endDate.split("-");

				// 날짜에 자릿수 확인해서 한자리로 들어올경우 두자리로 만들어준다.
				if ( startDate != null && endDate != null ) {
					if ( aStart.length == 3 ) {
						sy = aStart[0];
						sm = aStart[1];
						sd = aStart[2];

						if ( sm !=null && sm.length() == 1 ) {
							sm = "0" + sm;
						}

						if ( sd !=null && sd.length() == 1 ) {
							sd = "0" + sd;
						}
					}

					if ( aEnd.length == 3 ) {
						ey = aEnd[0];
						em = aEnd[1];
						ed = aEnd[2];

						if ( em !=null && em.length() == 1 ) {
							em = "0" + em;
						}

						if ( ed !=null && ed.length() == 1 ) {
							ed = "0" + ed;
						}
					}
				}

				//시작날짜와 종료날짜 비교
				String DSchk = sy+sm+sd;
				String DEchk = ey+em+ed;

				if( Integer.parseInt(DSchk) > Integer.parseInt(DEchk) ){
					return ymdCheck = "-4";
				}

				if ( aStart.length == 3 ) {
					sy = aStart[0];
					sm = aStart[1];
					sd = aStart[2];
				}

				if ( aEnd.length == 3 ) {
					ey = aEnd[0];
					em = aEnd[1];
					ed = aEnd[2];
				}
			}

			/*
			//윤년인지 확인한다.       
			if( isLeapYear(Integer.parseInt(sy)) ){
				snumOfLeapYear=1; //윤년인것을 알기 위해 증가시킨다.         
			} else  if( isLeapYear(Integer.parseInt(ey)) ){
				enumOfLeapYear=1; //윤년인것을 알기 위해 증가시킨다.         
			} else{
				numOfLeapYear=0;        
			}
			*/

			if ( isLeapYear(Integer.parseInt(sy)) == true && Integer.parseInt(sm) == 2 ){
				if ( Integer.parseInt(sd) > 29 ) {
					return ymdCheck = "-1";
				}
			} else {    
				for(int i = 0; i < 12; i++){
					if( Integer.parseInt(sm) == endOfYear[i] ){   
						if ( Integer.parseInt(sd) > endOfMonth[i] ) {   
							return ymdCheck = "-2";
						}
					}
					if( Integer.parseInt(sm) > 12 ){
						return ymdCheck = "-5";
					}
				}
			}

			//평년의 종료일
			if ( isLeapYear(Integer.parseInt(ey)) == true && Integer.parseInt(em) == 2 ){
				if ( Integer.parseInt(ed) > 29 ) {
					return ymdCheck = "-1";
				}
			} else {
				for(int i = 0; i < 12; i++){
					if( Integer.parseInt(em) == endOfYear[i] ){
						if (  Integer.parseInt(ed) > endOfMonth[i] ) {
							return ymdCheck = "-3";
						}
					}
					if( Integer.parseInt(sm) > 12 ){
						return ymdCheck = "-5";
					}
				}
			}

		} catch (java.lang.NumberFormatException e){
            logger.error( Utils.class.getName() + " > NumberFormatException test Exception => Start Date : " + startDate + " / End Date : " + endDate);
		}
		finally {} 

		return ymdCheck; 
	}   

	/**
	 * 
	 * @param startDate
	 * @return
	 */
	public String yfcheckDate(String startDate) { 
		String ymdCheck = "0"; 

		//int snumOfLeapYear = 0;       // 윤년일 경우 시작일
		//int numOfLeapYear = 0;        // 평년일 경우

		int[] endOfYear = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}; 
		int[] endOfMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

		//년도 자릿수를 체크.
		String sy = "";
		String sm = "";
		String sd = "";

		try{
			if ( startDate != null ) {
				String aStart[] = startDate.split("-");

				//날짜에 자릿수 확인해서 한자리로 들어올경우 두자리로 만들어준다.
				if ( aStart.length == 3 ) {
					sy = aStart[0];
					sm = aStart[1];
					sd = aStart[2];
				}
			}

			/*
			//윤년인지 확인한다.       
			if( isLeapYear(Integer.parseInt(sy)) ){
				snumOfLeapYear=1; //윤년인것을 알기 위해 증가시킨다.         
			} else{
				numOfLeapYear=0;        
			}
			*/

			if ( isLeapYear(Integer.parseInt(sy)) == true && Integer.parseInt(sm) == 2 ){
				if ( Integer.parseInt(sd) > 29 ) {
					return ymdCheck = "-1";
				}
			} else {
				for(int i = 0; i < 12; i++){
					if( Integer.parseInt(sm) == endOfYear[i] ){
						if ( Integer.parseInt(sd) > endOfMonth[i] ) {                     
							return ymdCheck = "-2";
						}
					} else if( Integer.parseInt(sm) > endOfYear[i] ){
						return ymdCheck = "-2";
					}
				}
			}

		} catch (java.lang.NumberFormatException e){
			logger.error( Utils.class.getName() + " > NumberFormatException test Exception");
		}
		finally {} 

		return ymdCheck; 
	}

	/**
	 * 날짜 체크하여 월, 일이 한자리로 들어오면 두 자릿수로 변경해서 반환한다.
	 * <p>
	 * @param chDate 날짜(타임스템프 포맷의 문자열) 
	 * @return 날짜 월, 일의 자릿수 체크 하여 1~9일의 경우 01~09로  반환한다.
	 */
	public String checkDt(String chDate) { 

		chDate = chDate == null ? "" : chDate;

		if ( chDate != null ) {
			String aStart[] = chDate.split("-");

			String sy = "";
			String sm = "";
			String sd = "";

			//날짜에 자릿수 확인해서 한자리로 들어올경우 두자리로 만들어준다.
			if ( aStart.length == 3 ) {
				sy = aStart[0];
				sm = aStart[1];
				sd = aStart[2];

				if( sm.length() < 2 ){
					sm = "0"+sm;
					chDate = sy+"-"+sm+"-"+sd;
				}

				if( sd.length() < 2 ){
					sd = "0"+sd;
					chDate = sy+"-"+sm+"-"+sd;
				}
			}
		}

		return chDate;
	}

	/**
	 * 입력한 문자열 날짜 데이터를 표준 포맷으로 변경
	 * (즉, 1999-1-1은 1999-01-01로 변경됨)
	 * 
	 * @param strDate 입력한 문자열 날짜 데이터
	 * @return 표준 포맷의 날짜 데이터 문자열
	 */
	public String replaceStandardDate(String strDate) {

		String sDate = strDate;

		if ( sDate != null ) {
			String aDate[] = sDate.split("-");

			if ( aDate.length == 3 ) {
				if ( aDate[1] != null && aDate[1].length() == 1 ) {
					sDate = aDate[0] + "-" + "0" + aDate[1];
				} else {
					sDate = aDate[0] + "-" + aDate[1];
				}

				if ( aDate[2] != null && aDate[2].length() == 1 ) {
					sDate = sDate + "-" + "0" + aDate[2];
				} else {
					sDate = sDate + "-" + aDate[2];
				}
			}
		}

		return sDate;
	}

	/**
	 * 수행 시간 로그를 찍기 위한 함수
	 * <p>
	 * @param Message 로그 문자열
	 * @param startDate 측정 시작 시간
	 */
	public static void logRunTime(String Message, Date startDate) 
	{
		Date endDate = new Date();

		if ( logger.isInfoEnabled() ) {
			logger.info( Message + " runtime : " + Long.toString(endDate.getTime() - startDate.getTime()) );
		}
	}
	
	public String changeltgt(String sSource)
	{
		String sChangeData = null;
		
		sChangeData = sSource == null ? "" : sSource.trim();
		
		while( sChangeData.indexOf("&lt;") > -1 ) 
		{
			sChangeData = sChangeData.replace("&lt;", "<");
		}
		
		while( sChangeData.indexOf("&gt;") > -1 ) 
		{
			sChangeData = sChangeData.replace("&gt;", ">");
		}
		
		return sChangeData;
	}
	
	public String getHtmlFromURL(String urlPath) {
		
		logger.info( "	>>> getHtmlFromURL");
		
		StringBuffer sb = new StringBuffer();
		
		try
		{	
			URL url = new URL( urlPath );
			URLConnection conn = url.openConnection();
			conn.setDoOutput( true );
			OutputStreamWriter wr =
					new OutputStreamWriter( conn.getOutputStream() );

			wr.write( sb.toString() );
			wr.flush();
			wr.close();
			
			String line = null;
			BufferedReader rd =
					new BufferedReader( new InputStreamReader( conn.getInputStream(), "UTF-8" ) );
			while( ( line = rd.readLine() ) != null )
			{
				//System.out.println( line );
				sb.append(line);
			}

			rd.close();
		}
		catch( Exception e )
		{
			throw new RuntimeException(e.getMessage());
		}
		
		return sb.toString();
	}
	
	public String getHtmlFromURLofDe(String urlPath) {
		
		logger.info( "	>>> getHtmlFromURL");
		
		StringBuffer sb = new StringBuffer("");
		
		try
		{	
			URL url = new URL( urlPath );
			URLConnection conn = url.openConnection();
			conn.setDoOutput( true );			
			
			String line = null;
			BufferedReader rd = new BufferedReader( new InputStreamReader( conn.getInputStream() ) );
			
			while( ( line = rd.readLine() ) != null )
			{
				//System.out.println( line );
				sb.append(line);
			}

			rd.close();
		}
		catch( FileNotFoundException fa){
		    logger.info( urlPath +"   >>> NotFoundURL");
		}
		catch( Exception e )
		{
			throw new RuntimeException(e.getMessage());
		}
		return sb.toString();
	}
	
	/**
     * html tag를 제외한 text만 뽑아서 리턴한다
     * @param htmlText
     * @return tag를 제외한 text
     */
    public String getTextFromHtml(String htmlText) {

        final StringBuffer buf = new StringBuffer();
        HTMLEditorKit.ParserCallback callback = new HTMLEditorKit.ParserCallback() {
            boolean isSkip = false;

            public void handleText(char[] data, int pos) {
                if (!isSkip) {
                    buf.append(new String(data));
                }
            }

            public void handleStartTag(HTML.Tag t, MutableAttributeSet a, int pos) {
                String tag = t.toString().toLowerCase();

                
                if ("script".equals(tag) || "head".equals(tag)) {
                    isSkip = true;
                }
            }

            public void handleEndTag(HTML.Tag t, int pos) {

                String tag = t.toString().toLowerCase();

                if("script".equals(tag) || "head".equals(tag)) {
                    isSkip = false;
                }
            }
            
//            public void handleError(String errorMsg, int pos)
//            {
//            	System.out.println("ERROR\t" + new String(errorMsg));
//            	isSkip = true;
//            }
        };
        try {
            Reader reader = new StringReader(htmlText);
            
            new ParserDelegator().parse(reader, callback, false);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return buf.toString();

    }
    
    /**
     * Presse Kit 정보 추출
     * @param dataMap   본문 데이터
     * @param resultMap 색인 데이터
     * @throws Exception
     */  
    public void getServlet(String urlPath)
    {
        
        try {
            URL url = new URL( urlPath );
            URLConnection conn = url.openConnection();
            conn.setDoOutput( true );
            OutputStreamWriter wr =
                    new OutputStreamWriter( conn.getOutputStream() );
    
            StringBuffer sb = new StringBuffer();
            //sb.append( "<!-- ***  Get Site( " + request + " ) START ***" );
            sb.append( wr.toString() );
            //sb.append( "*** Get Site END *** -->" );
    
            wr.write( sb.toString() );
            wr.flush();
            wr.close();
    
            System.out.println( " *** Get Site( " + urlPath + " ) ***" );
            
            System.out.println("Content : " + conn.getContent().getClass());
            
            HttpURLConnection abc = (HttpURLConnection) conn;
            
            int iResponseCode = abc.getResponseCode();
            
            System.out.println("Response Code : " + iResponseCode);
            
            if(iResponseCode == 200) {
                System.out.println("Url : " + urlPath + " > Sucess");
            } else {
                System.out.println("Url : " + urlPath + " > Fail");
            }
            
            String line = null;
            BufferedReader rd =
                    new BufferedReader( new InputStreamReader( conn
                            .getInputStream() ) );
            while( ( line = rd.readLine() ) != null )
            {
                System.out.println( line );
            }
    
            rd.close();
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }
    
    public String checkNull(String s){
		return checkNull(s, "");
	}
	
	public String checkNull(String s, String s1)
	{		
		String o = "";
		
		if(s == null || s.equals("") || s.equals("null"))
			o = s1;
		else
			o = s.trim();
		
		return o;	
	}
	
	public int checkNull(int i)
	{	
		return i;		
	}
	
    /**
     * Language And Encoding Setting
     * @param dataMap   본문 데이터
     * @param resultMap 색인 데이터
     * @throws Exception
     */
    public void setLanguageAndEncoding( Map<String, Object> dataMap, Map<String, Object> resultMap )
    throws Exception
    {
        logger.info( "  >>>setLanguageAndEncoding");
        int iRowCnt = 0;

        try {
            String sSiteCD = (String)dataMap.get("DSITE_CD");
            String sLanguage = null;
            String sLanguages = null;
            String sCharset = null;

            if (sSiteCD.equalsIgnoreCase("ae")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ae_ar")) {
                sLanguage = "ar";
                sLanguages = "ar";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("africa_fr")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("africa_pt")) {
                sLanguage = "pt";
                sLanguages = "pt";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ar")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("at")) {
                sLanguage = "de";
                sLanguages = "de";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("au")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("baltic")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("be")) {
                sLanguage = "nl";
                sLanguages = "nl";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("be_fr")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("br")) {
                sLanguage = "pt";
                sLanguages = "pt";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("bg")) {
                sLanguage = "bg";
                sLanguages = "bg";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ca")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ca_fr")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ch")) {
                sLanguage = "de";
                sLanguages = "de";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ch_fr")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("cl")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("cn")) {
                sLanguage = "zh-simplified";
                sLanguages = "zh-simplified";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("co")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("cz")) {
                sLanguage = "cs";
                sLanguages = "cs";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("de")) {
                sLanguage = "de";
                sLanguages = "de";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("dk")) {
                sLanguage = "da";
                sLanguages = "da";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ee")) {
                sLanguage = "et";
                sLanguages = "et";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("eg")) {
                sLanguage = "ar";
                sLanguages = "ar";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("es")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("eu")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("fi")) {
                sLanguage = "fi";
                sLanguages = "fi";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("fr")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("gr")) {
                sLanguage = "el";
                sLanguages = "el";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("hk")) {
                sLanguage = "zh-traditional";
                sLanguages = "zh-traditional";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("hk_en")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("hr")) {
                sLanguage = "hr";
                sLanguages = "hr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("hu")) {
                sLanguage = "hu";
                sLanguages = "hu";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("id")) {
                sLanguage = "id";
                sLanguages = "id";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ie")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("il")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("in")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("iran")) {
                sLanguage = "fa";
                sLanguages = "fa";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("it")) {
                sLanguage = "it";
                sLanguages = "it";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("jp")) {
                sLanguage = "ja";
                sLanguages = "ja";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("kz_ru")) {
                sLanguage = "ru";
                sLanguages = "ru";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("latin")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("latin_en")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("levant")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("lt")) {
                sLanguage = "lt";
                sLanguages = "lt";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("lv")) {
                sLanguage = "lv";
                sLanguages = "lv";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("mx")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("my")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("n_africa")) {
                sLanguage = "fr";
                sLanguages = "fr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("nl")) {
                sLanguage = "nl";
                sLanguages = "nl";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("no")) {
                sLanguage = "nn";
                sLanguages = "nn";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("nz")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("pe")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ph")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("pk")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("pl")) {
                sLanguage = "pl";
                sLanguages = "pl";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("pt")) {
                sLanguage = "pt";
                sLanguages = "pt";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ro")) {
                sLanguage = "ro";
                sLanguages = "ro";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("rs")) {
                sLanguage = "sr";
                sLanguages = "sr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ru")) {
                sLanguage = "ru";
                sLanguages = "ru";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("sa")) {
                sLanguage = "ar";
                sLanguages = "ar";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("sa_en")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("se")) {
                sLanguage = "sv";
                sLanguages = "sv";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("sec")) {
                sLanguage = "ko";
                sLanguages = "ko";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("sg")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("sk")) {
                sLanguage = "sk";
                sLanguages = "sk";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("th")) {
                sLanguage = "th";
                sLanguages = "th";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("tr")) {
                sLanguage = "tr";
                sLanguages = "tr";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("tw")) {
                sLanguage = "zh-traditional";
                sLanguages = "zh-traditional";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ua")) {
                sLanguage = "uk";
                sLanguages = "uk";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ua_ru")) {
                sLanguage = "ru";
                sLanguages = "ru";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("uk")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("us")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("ve")) {
                sLanguage = "es";
                sLanguages = "es";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("vn")) {
                sLanguage = "vi";
                sLanguages = "vi";
                sCharset = "utf-8";
                iRowCnt ++;
            } else if (sSiteCD.equalsIgnoreCase("za")) {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
                iRowCnt ++;
            } else {
                sLanguage = "en";
                sLanguages = "en";
                sCharset = "utf-8";
            }
            
            logger.info( "  iRowCnt=" + iRowCnt + " / language=" + sLanguage + " / charset=" + sCharset);
            
            resultMap.put( "language", sLanguage);
            resultMap.put( "languages", sLanguages);
            resultMap.put( "charset", sCharset);
            
        } catch ( Exception e) {
            logger.error("Error:Did not match any Language and Encoding");
            throw e;
        } finally {
        }
    }
    
    public String getTextFromFile(String sBodyTempPath) throws IOException
	{
		if(sBodyTempPath == null || sBodyTempPath.trim().length() < 1) return "";
		
		File oArticle = new File ( sBodyTempPath );
		
		if(!oArticle.exists()) return "";
		
		StringBuffer sbTemp = new StringBuffer("");
		
		FileInputStream oFis = new FileInputStream( sBodyTempPath );
		InputStreamReader isr = new InputStreamReader(oFis, "UTF-8");
		BufferedReader in = new BufferedReader(isr);
		
		String readLine = "";
		while((readLine = in.readLine()) != null) {
			sbTemp.append(readLine + "\n");
		}
		
		if(in != null) in.close();
		if(isr != null) isr.close();
		if(oFis != null) oFis.close();
		
		return sbTemp.toString();
	}
    
    public String ChangeData(String sModel) {
	    sModel = sModel == null ? "" : sModel;
	    sModel = sModel.replaceAll("null", "");
		String [] sArrayModelCode = splitKeyword(sModel);
		String sSplitData = "";	// 추출된 키워드
		StringBuffer sMergeTotalTerm = new StringBuffer(); // Merge 된 키워드
		
		if (sArrayModelCode != null && sArrayModelCode.length > 0){
			
			for (int i=0; i < sArrayModelCode.length; i++){
				// Split 된 데이터가 3Charater 이상일 경우만 Term 분리
				if (!"null".equals(sArrayModelCode[i]) && sArrayModelCode[i].length() > 2)
					sSplitData = charaterExtract(sArrayModelCode[i]);
				
				// 3Charater 이상인 데이터에서 분리된 Term 이 존재할 경우 Merge
				if (sSplitData.length() > 0){
					if (sMergeTotalTerm.length() > 0)
						sMergeTotalTerm.append(",").append(sSplitData);
					else
						sMergeTotalTerm.append(sSplitData);
				}
				sSplitData = "";
			}
		}
		return sMergeTotalTerm.toString();
	}
    
    /**
	 * Related Accessory - MDL_CD, MDL_NM TERM 분리
	 */
	public String [] splitKeyword(String sModelcode){
		
		StringBuffer sStringAscii = new StringBuffer(); 
		String [] sArraySplitString = null;
		
		if (sModelcode.length() > 0){
			for ( int i = 0; i < sModelcode.length(); i++ ) {
		    	char c = sModelcode.charAt( i );
		    	int j = (int) c;
		    	if ((j > 96 && j < 123) || (j > 64 && j < 91) || (j > 47 && j < 58))
		    		sStringAscii.append((char)j);
		    	else 
		    		sStringAscii.append(",");
			}  
		}
		
		if (sStringAscii.toString().length() > 0){
			sArraySplitString = sStringAscii.toString().split(",");
		}
		return sArraySplitString;
	}
	
	public String charaterExtract(String splitstring){
		char check;
		if(splitstring.equals("")) {
			//문자열이 공백인지 확인
			return splitstring;
		}
		// 문자열, 숫자형 체크
		boolean checkInt = false;
		StringBuffer sStringInt = new StringBuffer();
		
		for(int i = 0; i<splitstring.length(); i++){
			check = splitstring.charAt(i);
			
			if( check < 48 || check > 58){
				if (checkInt){
					sStringInt.append(",").append(splitstring.charAt(i));
				}else{
					sStringInt.append(splitstring.charAt(i));
				}
				checkInt = false;
			} else{
				if (checkInt){
					sStringInt.append(splitstring.charAt(i));
				}else{
					sStringInt.append(",").append(splitstring.charAt(i));
				}
				checkInt = true;
			}
		}
		
		String [] arrResult = sStringInt.toString().split(",");
		StringBuffer sGubun = new StringBuffer(); 
		for (int j =0; j < arrResult.length; j++){
			if (arrResult[j].length() > 2){
				if (sGubun.length() > 0)
					sGubun.append(",").append(arrResult[j]);
				else 
					sGubun.append(arrResult[j]);
			}
		}
		return sGubun.toString();
	}
	
	public String SpecialCharater(String sModel) {
		
		sModel = sModel == null ? "" : sModel;
		
        String sTerm = sModel;
        
        // 특수 문자 제거후 Term 분리 모듈 수행한 결과를 가지고 재 조합한 경우
        String aTerm[] = sTerm.split(",");
        String s3Term = null;
        if ( aTerm.length >= 2) {
            s3Term = aTerm[0] +  aTerm[1];
        }
        
        // 특수 문자 처리
        Pattern p = Pattern.compile("-|/");
        Matcher m = p.matcher(sTerm);
        
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, " ");
        }
        m.appendTail(sb);
        
        s3Term = sb.toString();
        
        // 특수 문자를 기준으로 자른 문자열을 첫번째와 두번째를 더한 문자열
        String aTerm1[] = s3Term.split(" ");
        s3Term = s3Term.replace(" ","");
        if ( aTerm1.length >= 2 ) {
            String sChangeData = ChangeData(aTerm1[1]);
            
            if ( sChangeData == null || sChangeData.equalsIgnoreCase(aTerm1[1])) {
                sChangeData = aTerm1[0] + aTerm1[1];
            } else {
                sChangeData = aTerm1[0] + sChangeData + "," + aTerm1[0] + aTerm1[1];
            }
            
            if (s3Term != null && s3Term.length() > 0) {
                //s3Term = s3Term + "," + aTerm1[0] + aTerm1[1];
                s3Term = s3Term + "," + sChangeData;
            } else {
                s3Term = sChangeData;
            }
        }
        return s3Term;
    }
	
	public String SpecialCharater2(String sMode1) {
		String sMode2 = ChangeData(sMode1.replace("-","").replace("/","").replace("(", "").replace(")", ""));
    	String aTerm2[] = sMode2.split(",");
    	String s4Term = null;
    	
    	if(aTerm2.length > 3){
    		s4Term = aTerm2[0] + aTerm2[1] + "," + aTerm2[1] + aTerm2[2] + "," + aTerm2[2] + aTerm2[3];
    	} else if (aTerm2.length > 2){
    		s4Term = aTerm2[0] + aTerm2[1] + "," + aTerm2[1] + aTerm2[2];
    	} else if (aTerm2.length > 1){
    		s4Term = aTerm2[0] + aTerm2[1];
    	} else {
    		s4Term = aTerm2[0];
    	}
    	
        return s4Term;
    }
	
	public  String SpecialCharater3(String sModel) {
    	String sMode3 = ChangeData(sModel);
    	String aTerm3[] = sMode3.split(",");
    	String s5Term = null;
    	
    	if(aTerm3.length > 3){
    		s5Term = aTerm3[0] + aTerm3[1] + "," + aTerm3[1] + aTerm3[2] + "," + aTerm3[2] + aTerm3[3];
    	} else if (aTerm3.length > 2){
    		s5Term = aTerm3[0] + aTerm3[1] + "," + aTerm3[1] + aTerm3[2];
    	} else if (aTerm3.length > 1){
    		s5Term = aTerm3[0] + aTerm3[1];
    	} else {
    		s5Term = aTerm3[0];
    	}
    	
        return s5Term;
    }
	
	public String getUniqueString(String dupString) {
    	dupString = dupString == null ? "" : dupString;
    	dupString = dupString.replaceAll("null", "");
		String [] tokens = dupString.split(",");
		String uniqueString = "";
		TreeSet<String> uniqueTree = new TreeSet<String>();
		  for ( int i=0; i < tokens.length; i++ ) {
		      if (tokens[i] !=null && !"null".equals(tokens[i]))
		          uniqueTree.add(tokens[i]);
		  }
		  Iterator<String> itor = uniqueTree.iterator();
		  while(itor.hasNext()){
			  if (uniqueString.length() > 0)
			 	uniqueString += " "+itor.next();
			  else
				uniqueString += itor.next();
		  }
		  return uniqueString;
	}
	
	public static void bubbleSort(int arr[], int limit, int type) {
		
		int tmp;
		int cnt=0;
		
		// 오름차순 정렬
		if ( type == 1 ) {
			for(int i=0; i<(limit-1); i++) {
				for(int j=0; j<(limit-1)-i; j++) {
					if(arr[j] > arr[j+1]) {
						tmp = arr[j];
						arr[j] = arr[j+1];
						arr[j+1] = tmp;
						cnt++;
					}
				}
			}
		// 내림차순 정렬
		} else { 
			for(int i=0; i<(limit-1); i++) {
				for(int j=0; j<(limit-1)-i; j++) {
					if(arr[j] < arr[j+1]) {
						tmp = arr[j];
						arr[j] = arr[j+1];
						arr[j+1] = tmp;
						cnt++;
					}
				}
			}
		}
	}
	
	public void sellectionSort(int arr[], int limit, int type) {
		
		int tmp;
		int cnt=0;
		
		// 오름차순 정렬
		if ( type == 1 ) {
			for(int i=0; i<limit-1; i++) {
				for(int j=0; j<limit-1; j++) {
					if(arr[j] > arr[j+1]) {
						tmp=arr[j+1];
						arr[j+1]=arr[j];
						arr[j]=tmp;
						cnt++;
					}
				}
			}
		// 내림차순 정렬
		} else {
			for(int i=0; i<limit-1; i++) {
				for(int j=0; j<limit-1; j++) {
					if(arr[j] < arr[j+1]) {
						tmp=arr[j+1];
						arr[j+1]=arr[j];
						arr[j]=tmp;
						cnt++;
					}
				}
			}
		}
	}
	
	public static void printListDataKeySort(List<DataKeySort> l) {
		for (int i=0; i<l.size(); i++) {
			DataKeySort m = (DataKeySort)l.get(i);
			System.out.println(m.getDataKeys() + "/" + m.getKeyOrder());
		}
	}
	
	/**
	 * DB에서 조회한 데이타가 중복된 문자열을 가지고 있을 경우 중복 제거하기 위한
	 * 
	 * @param dataMap
	 * @param resultMap
	 * @param sSourceColumn
	 * @param sTargetColumn
	 * @throws Exception
	 */
	public void removeDuplicateData( 
			Map<String, Object> dataMap, 
			Map<String, Object> resultMap, 
			String sSourceColumn, 
			String sTargetColumn, 
			String sSplitStr,
			String sDataSaveType) 
	throws Exception {
		
		String sInData = null, sOutData = null;
		
		if ( "FILE".equalsIgnoreCase(sDataSaveType) ) {
			File fData = null;
			
			sInData = (String)dataMap.get(sSourceColumn);	// 컬럼명 대문자임에 주의
			
			if ( sInData != null && sInData.trim().length() > 0 ) {
				fData = new File(sInData);
			}
			
			if ( fData != null && fData.isFile() ) {
				sInData = getTextFromFile(sInData);
			}
			
		} else {
			sInData = (String)dataMap.get(sSourceColumn);	// 컬럼명 대문자임에 주의
		}
		
		if ( ( sSplitStr != null && sSplitStr.trim().length() > 0 ) && ( sInData != null && sInData.trim().length() > 0 ) ) {
		
			String aInData[] = sInData.split(sSplitStr);
			
			Map<String,String> mapData = new HashMap<String,String>();
			
			for(int i=0; i<aInData.length; i++) {
				mapData.put(aInData[i],aInData[i]);
			}
			
			StringBuffer sb = new StringBuffer("");
			
			Set<String> setData = mapData.keySet();
			Object[] Keys = setData.toArray();
			
			for (int i = 0, n = Keys.length; i < n; i++) {
				sb.append(Keys[i].toString()).append(sSplitStr);
			}
			
			if ( sb.length() > sSplitStr.length() ) {
				sOutData = sb.substring(1,sb.length()-sSplitStr.length());
				if ( sOutData != null && sOutData.length() > 0 ) {
					sOutData = sOutData.replace("\n","");
				}
			}
			
		}
		
		if ( "FILE".equalsIgnoreCase(sDataSaveType) ) {
			writeContentInFile(resultMap, sTargetColumn, sOutData);
		} else {
			resultMap.put( sTargetColumn, sOutData );
		}
	}
	
	public void writeContentInFile(Map<String, Object> resultMap, String sTargetColumn, String sWriteContents)
	{		
		String sBodyFile = (String)resultMap.get(sTargetColumn);
		
		if (sBodyFile == null) {
			resultMap.put(sTargetColumn, sWriteContents);
		} else {
			File oBodyFile = new File(sBodyFile);
			
			if ( oBodyFile.exists() ) {
				FileAppender oFileAppender = new FileAppender( sBodyFile );
				boolean bError = false;
				try {
					oFileAppender.write ( sWriteContents );
				} catch (FileNotFoundException e) {
					logger.error("writeContentInFile Error(FileNotFoundException) : " + e.toString() );
					bError = true;
				} catch (IOException e) {
					logger.error("writeContentInFile Error(IOException) : " + e.toString() );
					bError = true;
				}
				
				if ( bError ) {
					logger.info(">>>> Error append File Description at Body File");
					resultMap.put(sTargetColumn, sWriteContents);
				}
				
			} else {
				resultMap.put(sTargetColumn, sWriteContents);
			}
		}
		
	}
}

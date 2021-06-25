/**
 *******************************************************************
 * 파일명 : YessLogger.java
 * 파일설명 : apache의 log4j클래스를 이용해 로그를 기록한다.
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/15   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.logging;

import java.io.*;
import org.apache.log4j.*;

import com.rayful.localize.Localization;

/**
*	apache의 log4j클래스를 이용해 로그를 기록하는 클래스
*/
public class RayfulLogger
{
		
	public static Level LEVEL = null;
	
	/**
	*	log4j의 logger클래스를 리턴한다.
	* @param	sClassName	로그를 기록할 클래스
	* @param	sFileName	로그파일명
	* @return	logger클래스
	*/	
	public static Logger getLogger ( String sClassName, String sFileName ) {
		
		// 날짜 패턴에 따라 추가될 파일 이름
		Logger oLogger = Logger.getLogger( sClassName );
		FileAppender oFileAppendar = null;
		Appender oStdAppender = Logger.getRootLogger().getAppender( "stdout" );
		PatternLayout oPatternlayout = null;
		
		
		if ( LEVEL != null ) {
			oLogger.setLevel ( LEVEL );
		}
		
		if ( oStdAppender != null ) {
			try {
				oPatternlayout = ( PatternLayout )oStdAppender.getLayout();
				oFileAppendar = new FileAppender(oPatternlayout, sFileName );
				oLogger.addAppender( oFileAppendar );
			} catch (IOException ioe) {
				System.out.println ( Localization.getMessage( RayfulLogger.class.getName() + ".Console.0001" ) );
			}			
		} else {
			System.out.println ( Localization.getMessage( RayfulLogger.class.getName() + ".Console.0001" ) );
		}
		return oLogger;
  }
  
}
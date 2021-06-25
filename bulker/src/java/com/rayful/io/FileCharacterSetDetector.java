package com.rayful.io;

import java.io.*;

import org.apache.log4j.Logger;
import org.mozilla.intl.chardet.nsDetector;
import org.mozilla.intl.chardet.nsICharsetDetectionObserver;
import org.mozilla.intl.chardet.nsPSMDetector;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.*;

public class FileCharacterSetDetector {

	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger(FileCharacterSetDetector.class.getName(), Config.LOG_PATH );
	
	/** Character-Set : 알수없음 */
	public static int UNKNOWN = 0;
	/** Character-Set : 한글ANSI */
	public static int EUCKR = 1;
	/** Character-Set : UTF-8 */
	public static int UTF8 = 2;
	
	/** Character-Set : UTF-8 */
	private int mi_characterSet;
	/** Character-Set : UTF-8 */
	private boolean mb_found;
	
	public FileCharacterSetDetector ( File oFile )
	throws FileNotFoundException, IOException
	{
		mi_characterSet = UNKNOWN;
		mb_found = false;
		detectCharacterSet( oFile );
	}
	
	private void detectCharacterSet( File oFile )
	throws FileNotFoundException, IOException
	{
		// Initalize the nsDetector() ;
		
		/*
		* nsPSMDetector.ALL로 하면 
		* ANSI파일을  GBXXXX로 인식하는 경우가 발생
		* nsPSMDetector.KOREAN로 설정하는 것이 안전할 것으로 추정
		*/
		int iLang = nsPSMDetector.KOREAN ;
		nsDetector det = new nsDetector(iLang) ;

		// Set an observer...
		// The Notify() will be called when a matching charset is found.
		det.Init(
			new nsICharsetDetectionObserver() {
				public void Notify(String sCharset) {
					mb_found = true ;
					
					if ( logger.isDebugEnabled() ) {
						logger.debug( "File Character Set =" +  sCharset );
					}
					if ( sCharset.equals("EUC-KR")) {
						mi_characterSet = EUCKR;
					} else if ( sCharset.equals("UTF-8")) {
						mi_characterSet = UTF8;
					} else if ( sCharset.equals("windows-1252")) {
						// windows-1252는 EUCKR로 간주
						mi_characterSet = EUCKR;
					} else {
						// 그외 CharacterSet은 Unknown으로 인식하게 한다.
						mi_characterSet = UNKNOWN;
					}					
				}
	    	}
		);

		// File을 Open...
		FileInputStream oFis = new FileInputStream( oFile );
		BufferedInputStream oBis = new BufferedInputStream(oFis);
		
		byte[] buf = new byte[1024] ;
		int len;
		boolean bDone = false ;
		boolean bAscii = true ;
		   
		while( (len=oBis.read(buf,0,buf.length)) != -1) {
	
			// Check if the stream is only ascii.
			if (bAscii)
				bAscii = det.isAscii(buf,len);
	
			// DoIt if non-ascii and not done yet.
			if (!bAscii && !bDone)
				bDone = det.DoIt(buf,len, false);
		}
		det.DataEnd();
		oBis.close();
	
		if (bAscii) {
			mb_found = true ;
			mi_characterSet = EUCKR;
		}
	}
	
	public int getCharacterSet() {
		if ( mb_found ) {
			return mi_characterSet;
		} else {
			//찾지 못햇다면 EUCKR로 간주한다.
			//삼성전자 P2 DB:Oracle america.american.utf8환경에서 디폴드 변경.
			//logger.warn( "	File Character Set Not Found! and set euc-kr" );
			//return EUCKR;
			logger.warn( "	File Character Set Not Found! and set UTF8" );
			return UTF8;
		}
	}
	
	public String getCharaterSetName() {
		String sCharSetName = null;
		
		if ( mb_found ) {
			if ( mi_characterSet == UTF8 ) {
				sCharSetName = "utf-8";
			} else if  ( mi_characterSet == UTF8 ) {
				sCharSetName = "euc-kr";
			} else {
				sCharSetName = "euc-kr";
			}
		} else {
			//찾지 못햇다면 EUCKR로 간주한다.
			//삼성전자 P2 DB:Oracle america.american.utf8환경에서 디폴드 변경.
			//logger.warn( "	File Character Set Not Found! and set euc-kr" );	
			//sCharSetName = "euc-kr";
			logger.warn( "	File Character Set Not Found! and set UTF8" );	
			sCharSetName = "utf-8";
		}
		
		return sCharSetName;
	}
}

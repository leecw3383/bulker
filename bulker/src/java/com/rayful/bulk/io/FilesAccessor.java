/**
 *******************************************************************
 * 파일명 : FileAccessor.java
 * 파일설명 : 외부첨부파일을 처리하는 클래스들의 Base Interface를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
/**
 * 외부첨부파일을 처리하는 클래스들의 Base Interface<p>
 * 이 Interface를 implemets하는 클래스는 다음 메서드를 반드시 구현해야 한다.<p>
*/
public abstract class FilesAccessor 
{
	/**
	* 원격지에 저장된 파일을 다운로드한다.
	* <p>
	* @param	sFilePath	다운로드할 파일경로
	* @param	sFileName	다운로드할 파일이름
	* @param	sDestFile	저장할 파일경로 및 이름
	*/	
	abstract public  void downloadFile( String sFilePath, String sFileName, String sDestFile )
	throws MalformedURLException, IOException;
	
	
	abstract public  String getURL( String sFilePath, String sFileName )
	throws UnsupportedEncodingException;

	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/	
	abstract public String toString();
	
	/**
	* 서버에 연결한다.
	*/	
	abstract public boolean connect();
	
	/**
	* 서버와 연결을 해제한다.
	*/	
	abstract public void disconnect();
	
	/**
	* PATH정보에 포함된 \\ 디렉토리 경로를 URL디렉토리 경로로 변경한다.
	* <p>
	* @param	sPath	PATH정보
	* @return	URL형태의 PATH정보
	*/	
	public String getUrlPath( String sPath ) {
		String sUrlPath = sPath;
		String sTempUrlpath = sPath;
		
		if ( sTempUrlpath.indexOf( '\\' ) >= 0 ) {
			//-----------------------------------------------------
			// ReplaceAll시 에러를 일으키는 문자를 먼저 치환시킴
			//-----------------------------------------------------
			if ( sTempUrlpath != null ) {
				//치환문자열에 $가 포함된 경우 \$로 변경한다.
				if ( sTempUrlpath.indexOf ( "$" ) >= 0 ) {
					sTempUrlpath = sTempUrlpath.replaceAll( "[$]", "\\\\\\$" );
				}
			} else {
				sTempUrlpath = "";
			}
			
			//-----------------------------------------------------
			// ReplaceAll로 "\\" 문자를 "/"로 치환시킨다.
			//-----------------------------------------------------		
			sUrlPath = sTempUrlpath.replaceAll( "\\\\", "/" );
		}
		return sUrlPath;
	}

	/**
	* 두 PATH정보를 합쳐서 완성된 PATH정보를 구성한다.
	* 단 URL에서 사용하는 경로문자("/")를 사용한다.
	* <p>
	* @param	sParentPath	상위 PATH정보
	* @param	sChildPath 하위 PATH정보
	* @return	두 PATH정보를 합친 PATH정보
	*/	
	public String buildUrlPath( String sParentPath,  String sChildPath )
	{
		String sUrl = "";
		String sPPath = "";
		String sCPath = "";
		boolean bParhentEndSlash;
		boolean bChildStartSlash;
		
		if ( sParentPath != null ) {
			sPPath = sParentPath;
		}
		
		if ( sChildPath != null ) {
			sCPath = sChildPath;
		}
		
		if ( sPPath.length() == 0 ) {
			return sCPath;
		} else if ( sCPath.length() == 0 ) {
			return sPPath;
		} else if ( sPPath.length() > 0 &&  sCPath.length() > 0 ) {
			bParhentEndSlash = sPPath.endsWith("/");
			bChildStartSlash = sCPath.startsWith("/");
			
			if ( bParhentEndSlash ) {
				if ( bChildStartSlash ) {
					sUrl = sPPath + sCPath.substring(1);
				} else {
					sUrl = sPPath + sCPath;
				}
			} else {
				if ( bChildStartSlash ) {
					sUrl = sPPath + sCPath;
				} else {
					sUrl = sPPath + "/" +  sCPath;
				}			
			}
		}
		
		return sUrl;
	}
}
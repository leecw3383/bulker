/**
 *******************************************************************
 * 파일명 : HttpAccessor.java
 * 파일설명 : HTTP를 통해 외부첨부파일을 가져오는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.io;

import java.net.*;
import java.io.*;
/**
 * HTTP를 통해 외부첨부파일을 가져오는 클래스 <p>
 * FilesAccessor를 상속해서 정의된 클래스 
*/
public class HttpAccessor extends FilesAccessor
{
	String ms_hostName;			// 호스트명
	String ms_portNumber;		// 포트번호
	String ms_basePath;			// basepath 경로
	
	/**
		* @param	sHostName 호스트명 
		* @param	sPortNumber 포트번호 
		* @param	sBasePath basepath 경로 
	*/
	public HttpAccessor ( String sHostName, 
										String sPortNumber, 
										String sBasePath )
	{
		ms_hostName =  sHostName;
		ms_portNumber = sPortNumber;
		ms_basePath = sBasePath;
	}
	
	/**
	* 첨부파일경로를 얻어온다.
	* <p>
	* @return	첨부파일경로
	*/	
	private String getURL()
	{
		StringBuffer osb = new StringBuffer();

		osb.append( "http://");
		osb.append( ms_hostName );
		
		if ( ms_portNumber != null ) {
			if ( ms_portNumber.length() > 0 ) {
				osb.append( ":" );
				osb.append( ms_portNumber );
			}
		}
		
		if ( ms_basePath.length() > 0 ) {
			if ( ! ms_basePath.startsWith( "/") ) {
				osb.append ("/");
			}
				
			osb.append( ms_basePath );
			
			if ( ! ms_basePath.endsWith( "/") ) {
				osb.append ("/");
			}					
		} else {
			osb.append ("/");
		}
		return osb.toString();
	}
	
	/**
	* 첨부파일경로를 얻어온다.
	* <p>
	* @param	sFilePath	첨부파일경로
	* @param	sFileName	첨부파일명
	* @return	첨부파일경로
	*/	
	public String getURL( String sFilePath, String sFileName )
	throws UnsupportedEncodingException
	{
		String sUrl, sBaseUrl;
		String sEncodeFileName = ""; 
		String sUrlFilePath;
		String sUrlFileName;			
		
		sBaseUrl = this.getURL();

		// 공백을 => %20으로 강제변환
		if ( sFileName != null ) {
			//sEncodeFileName =sFileName.replaceAll ( " ", "%20" );
			sEncodeFileName = URLEncoder.encode ( sFileName, "UTF8" );
			sEncodeFileName = sEncodeFileName.replaceAll ( "[+]", "%20" );			
		}
		
		// 경로명 또는 파일에 포함된 (\)문자를 (/)로 변경한다.
		sUrlFilePath = this.getUrlPath( sFilePath );
		sUrlFileName = this.getUrlPath( sEncodeFileName);		
		
		sBaseUrl = buildUrlPath( sBaseUrl, sUrlFilePath );
		sUrl = buildUrlPath( sBaseUrl, sUrlFileName );
		
		return sUrl;
	}	

	/**
	* 서버에 연결한다.
	*/	
	public boolean connect() { return true; }
	
	/**
	* 서버와 연결을 해제한다.
	*/	
	public void disconnect() {}
	
	/**
	* 첨부파일을 HTTP를 통해 로컬로 가져온다.
	* <p>
	* @param	sSourceFilePath	다운로드할 파일경로
	* @param	sSourceFileName	다운로드할 파일명
	* @param	sDestFile	저장할 파일경로 및 이름
	*/	
	public void downloadFile( String sSourceFilePath, String sSourceFileName, String sDestFile )
	throws UnsupportedEncodingException, MalformedURLException, IOException
	{
		String sUrl = this.getURL( sSourceFilePath, sSourceFileName );
		//System.out.print ("[URL]=" + sUrl );
		URL ocu = new URL(sUrl);
		BufferedInputStream obi = new BufferedInputStream ( ocu.openStream() );
		FileOutputStream ofo = new FileOutputStream( sDestFile );
		BufferedOutputStream obo = new BufferedOutputStream( ofo );

		int buf;
		int count = 0;
		while( ( buf = obi.read() ) != -1 ) {
			obo.write( buf );
			count ++;
		}
		obi.close();
		obo.close();
	}
	

		
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다
	* <p>
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer( "ClassName:HttpAccessor {\r\n" );
		
		osb.append( "ms_hostName=" );
		osb.append( ms_hostName + "\r\n" );		
		
		osb.append( "ms_portNumber=" );
		osb.append( ms_portNumber + "\r\n" );
		
		osb.append( "ms_basePath=" );
		osb.append( ms_basePath + "\r\n" );
		
		return 	osb.toString();	 
	}	
}
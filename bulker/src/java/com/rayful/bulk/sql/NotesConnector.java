/**
 *******************************************************************
 * 파일명 : NotesConnector.java
 * 파일설명 : NOTES의 Connection 처리를 하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/22   주현필	최초작성 
 * 2005/07/20	정충열	RDBMS색인모듈과 통합
 * 2010/12/10	주현필	Log 메세지 LogBundle를 사용하도록 수정           
 *******************************************************************
*/

package com.rayful.bulk.sql;


import com.rayful.localize.Localization;

import lotus.domino.*;

/**
 * NOTES의 Connection 처리를 하는 클래스<p>
 * JdbcConnector를 implements하지 않는다.
*/
public class NotesConnector
{
	lotus.domino.Session mo_session = null;		// NOTES Session 객체
	
	String ms_hostName; 	//접속하려는 NOTES의 호스트명
	String ms_portNumber; 	//접속하려는 NOTES의 포트번호
	String ms_dbName;		//접속하려는 NOTES의 DB명
	String ms_nsfName;		//접속하려는 NOTES의 nsf명 (DB명의 일부)
	String ms_dbAccount;	//접속하려는 NOTES의 계정 ID
	String ms_dbPassword;	//접속하려는 NOTES의 패스워드
	
	
	/** 
	* 생성자
	* @param	sHostName	Key 	NOTES의 호스트명
	* @param	sPortNumber			RDBM의 port 번호
	* @param	sDbName					NOTES의 DB명
	* @param	sAccount				NOTES의 계정 ID
	* @param	sPassword				NOTES의 패스워드
	*/
	public NotesConnector(
		String sHostName,		
		String sPortNumber,
		String sDbName,
		String sAccount,
		String sPassword ) 
	{
		ms_hostName = sHostName;
		ms_portNumber = sPortNumber;
		ms_dbName = sDbName;
		ms_nsfName = NotesConnector.getNsfName( sDbName );
		ms_dbAccount = sAccount;
		ms_dbPassword = sPassword;
	}
	
	/**
	* NOTES의 DrvierName을 얻는다.
	* <p>
	* @return	커넥션 스트링
	*/	
	public String getDriverName() 
	{
		return "lotus.domino.Session";
	}
	
	
	/**
	* NOTES의 Host Name을 얻어 온다.
	* <p>
	* @return	멤버변수에 저장된 Host Name을 리턴
	*/	
	public String getHostName() 
	{
		if ( ms_hostName != null) {
			return ms_hostName;
		}
		else {
			return "";
		}
	}
	
	/**
	* NOTES의 Host Name을 얻어 온다.
	* <p>
	* @param	pHost	LDAP style의 경로명
	* @return	멤버변수에 저장된 Host Name을 리턴
	*/	
	public static String getHostName( String pHost ) 
	{
	    String sHostName;
			
	    sHostName = pHost;
	    if ((sHostName.equals("")) || (sHostName == null)) {
	      return "";
	    }
		
		sHostName = sHostName.toUpperCase();
	    sHostName = sHostName.replaceAll("CN=","");
	    sHostName = sHostName.replaceAll("\\.","/");
	    
	    if (sHostName.indexOf("\\") > 0) {
	    	sHostName = sHostName.replaceAll("\\","/");
	    }
	    
	    if (sHostName.indexOf("/") > 0) {
	      return sHostName.substring(0,sHostName.indexOf("/")).trim(); 
	    }
	    else {
	      return sHostName;
	    }
	}	
	
	/**
	* NOTES의 DB Name을 얻어 온다.
	* <p>
	* @return	DB Name
	*/	
	public String getDbName() 
	{
		if ( ms_dbName != null) {
			return ms_dbName;
		}
		else {
			return "";
		}
	}
	
	/**
	* NOTES의 Nsf Name을 얻어 온다. 멤버변수에 저장된 Nsf Name을 리턴
	* <p>
	* @return	멤버변수에 저장된 Nsf Name을 리턴
	*/	
	public String getNsfName() 
	{
		return ms_nsfName;
	}
	
	/**
	* NOTES의 DBPath로 부터  Nsf Name만을 추출한다.
	* <p>
	* @param	sDBPath	Nsf 경로명
	* @return	Nsf Name
	*/	
	public static String getNsfName( String sDBPath ) 
	{
		String sNsfName = null;
		String sNotesDBPath = null;
		
		if ( sDBPath != null ) {
			// DBPath에 "\"를 사용한 문자는 "/"로 변경한다.
			sNotesDBPath = sDBPath.replaceAll( "\\\\", "/" );
			
			String [] aDBPath = sNotesDBPath.split ( "/" );
			if ( aDBPath.length > 0 ) {
				String sNsfNameWithExt = aDBPath [ aDBPath.length - 1 ];
				String [] aNsfName = sNsfNameWithExt.split ( "[.]" );
				if ( aNsfName.length > 0 ) {
					sNsfName = aNsfName [0];
				}
			}
		}
		return sNsfName;
	}	
	
	
	/**
	* NOTES의 계정이름을 얻는다.
	* <p>
	* @return	계정명
	*/	
	public String getAccount() 
	{
		if ( ms_dbAccount != null ) {
			return ms_dbAccount;
		} else {
			return "";
		}
	}
	
	/**
	* NOTES의 계정 패스워드를 얻는다.
	* <p>
	* @return	패스워드
	*/	
	public String getPassword() 
	{
		if ( ms_dbPassword != null ) {
			return ms_dbPassword;
		} else {
			return "";
		}
	}
	
	/**
	* NOTES의 Session객체를 생성한다.
	* <p>
	*/		
	public void setSessionConnection()
	throws Exception
	{
		String sUrl = null;
		
	    try {  
	    	mo_session = NotesSessionPool.getSession( this.getHostName() );
    	
	    	if ( mo_session == null ) {
		    	sUrl = this.getHostName();
		    	if ( ms_portNumber != null ) {
		    		sUrl += ":" + ms_portNumber;
		    	}
	    	
				mo_session = NotesFactory.createSession( sUrl,
														this.getAccount(),
														this.getPassword()
														);
				if ( mo_session == null ) {
					throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0001") );
				}
																					
				NotesSessionPool.putSession( this.getHostName(), mo_session );
			}
		} catch (Exception e) {
			throw e;
		}													
	}	
	
	/**
	* NOTES의 Session객체를 Close한다.
	*/		
	public void closeSessionConnection() 
	throws  NotesException
	{
		// 현재는 NotesConnctionPool을 사용하므로 세션객체를 clear하지 않는다.
		//mo_session.recycle();
	}
	
	
	/**
	* NOTES의 세션객체를 생성한다.
	* <p>
	* @return	세션객체
	*/	
	public Session getSession() {
		return mo_session;
	}
	
	/**
	* NOTES의 커넥션객체를 생성한다.
	* <p>
	* @return	커넥션객체
	*/		
	public Database getDbConnection()
	throws Exception
	{
		Database oDb = null;
		try {  
    	
	    	if ( this.getDbName().equals("")) {
	    		throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0002") );
	    	}
	    
	    	if ( mo_session == null ) {
	    		throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0001") );
	    	}
	    	
	    	// Namo로 저장된 Body를 읽어올 때 MIME Type으로 읽어오기 위해서는 false
	    	// 설정해 주어야 한다.
	    	this.mo_session.setConvertMIME(false);
	    	
			oDb = this.mo_session.getDatabase("",getDbName());	
			if ( oDb == null ) {
				throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0003") );
			}
			
			if ( oDb.isOpen() == false ) {
				throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0004") );
			}

			return oDb;
    	
		} catch (Exception e) {
			throw e;		
		}
		
	}
	
	/**
	* NOTES의 커넥션객체를 생성한다.
	* <p>
	* @return	커넥션객체
	*/		
	public Database getDbConnection(String sNSFName)
	throws Exception
	{
		Database oDb = null;
		try {  
    	
	    	if ( (sNSFName == null) || ("".equalsIgnoreCase(sNSFName)) ) {
	    		throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0002") );
	    	}
	    
	    	if ( mo_session == null ) {
	    		throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0001") );
	    	}
	    			
				oDb = this.mo_session.getDatabase("",sNSFName);	
				if ( oDb == null ) {
					throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0003") );
				}
				
				if ( oDb.isOpen() == false ) {
					throw new Exception ( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0004") );
				}

			return oDb;
    	
		} catch (Exception e) {
			throw e;		
		}
		
	}
		
	public DateTime getCreateDateTime(String pLastIndexDate)
	throws Exception 
	{
		DateTime dt = null;
		
		try {
			if (mo_session == null) {
				throw new Exception( Localization.getMessage( NotesConnector.class.getName() + ".Exception.0001") );
			}
				//dt = s.createDateTime("2000-01-01 00:00:00 AM");
			dt = mo_session.createDateTime(pLastIndexDate);
			return dt;
		} catch (Exception e) {
			throw e;
		}
		
		
		
	}
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer( "ClassName:NotesConnector {\r\n" );
		
		osb.append( "ms_hostName=" );
		osb.append( ms_hostName + "\r\n" );
		
		osb.append( "ms_portNumber=" );
		osb.append( ms_portNumber + "\r\n" );
		
		osb.append( "ms_dbName=" );
		osb.append( ms_dbName + "\r\n" );
		
		osb.append( "ms_nsfName=" );
		osb.append( ms_nsfName + "\r\n" );		
		
		osb.append( "ms_dbAccount=" );
		osb.append( ms_dbAccount + "\r\n" );
		
		osb.append( "ms_dbPassword=" );
		osb.append( ms_dbPassword + " }\r\n" );		
		
		return 	osb.toString();	 
	}		
	
}
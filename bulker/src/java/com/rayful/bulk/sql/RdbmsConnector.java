/**
 *******************************************************************
 * 파일명 : RdbmsConnector.java
 * 파일설명 : RDBMS의 Connection 처리를 하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성
 * 2005/11/10   정충열    MYSQL, DB2 로직추가           
 *******************************************************************
*/

package com.rayful.bulk.sql;


import java.sql.*;

/**
 * RDBMS의 Connection 처리를 담당하는 클래스<p>
 * JdbcConnector를 implements해서 정의된 클래스
 * 현재 ORACLE, MS-SQL이 구현되어 있고, 만일 JDBC를 이용할 수 있는 데이터소스를 추가해야 한다면
 * 여기 부분에 해당접속방법을 구현해야 한다. <p>
 * [특이사항]MSSQL의 경우 microsoft에서 제공하는 드라이버가 버그가 많고, 많은 제약이 있어 jtds드라이버 사용
*/
public class RdbmsConnector implements JdbcConnector
{
	public static final int RDBMSTYPE_UNKNOWN = 0;
	public static final int RDBMSTYPE_ORACLE = 1;
	public static final int RDBMSTYPE_MSSQL = 2;
	public static final int RDBMSTYPE_MYSQL = 3;
	public static final int RDBMSTYPE_DB2 = 4;
	
	int rdbmsType; 		//접속하려는 RDBMS의 종류
	String driverClass; 	//접속하려는 RDBMS의 driever class 
	String url; 			//접속하려는 RDBMS의 url
	String username;	//접속하려는 RDBMS의 username
	String password;	//접속하려는 RDBMS의 password
	
	/** 
	* @param	driverClass		//접속하려는 RDBMS의 driever class 
	* @param	url	
	* @param	username
	* @param	password
	* 
	*  지원 드라이버 종류
	*	oracle.jdbc.driver.OracleDriver
	*	com.microsoft.jdbc.sqlserver.SQLServerDriver
	*	net.sourceforge.jtds.jdbc.Driver
	*	com.mysql.jdbc.Driver
	*	com.ibm.db2.jcc.DB2Driver
	*	com.ibm.db2.jdbc.net.DB2Driver
	*/
	public RdbmsConnector(
		String driverClass,
		String url,
		String username,
		String password ) 
	{
		this.driverClass = driverClass;
		this.url = url;
		this.username = username;
		this.password = password;
		
		if ( driverClass.indexOf( "oracle" ) >= 0) {
			this.rdbmsType = RdbmsConnector.RDBMSTYPE_ORACLE;
		} else if ( driverClass.indexOf( "microsoft" ) >= 0 || driverClass.indexOf( "jtds" ) >= 0) {
			this.rdbmsType = RdbmsConnector.RDBMSTYPE_MSSQL;
		// driverClass mariadb 도 인식하도록 추가
		} else if ( driverClass.indexOf( "mysql" ) >= 0 || driverClass.indexOf( "mariadb" ) >= 0 ) {
			this.rdbmsType = RdbmsConnector.RDBMSTYPE_MYSQL;
		} else if ( driverClass.indexOf( "db2" ) >= 0) {
			this.rdbmsType = RdbmsConnector.RDBMSTYPE_DB2;
		} else {
			this.rdbmsType = RdbmsConnector.RDBMSTYPE_UNKNOWN;
			// Unknown DB Type 일 경우 오류 추가
			throw new IllegalArgumentException("Unknown DB Type");
		}
	}
	
	/**
	* RDBMS의 종류를 알아낸다.
	* <p>
	* @return	커넥션 스트링
	*/	
	public int getRdbmsType() 
	{
		return this.rdbmsType;
	}	
	
	/**
	* RDBMS의 DrvierClass 명을 얻는다.
	* <p>
	* @return	커넥션 스트링
	*/	
	public String getDriverClass() 
	{
		if ( this.driverClass != null ) {
			return this.driverClass;
		} else {
			return "";
		}
	}
	
	/**
	* RDBMS의 커넥션 스트링을 얻는다.
	* <p>
	* 
	* url 종류
	*jdbc:oracle:thin:@HOSTNAME[:PORT]:SID
	*jdbc:microsoft:sqlserver://HOSTNAME[:PORT];DatabaseName=DBNAME
	*jdbc:jtds:sqlserver://HOSTNAME[:PORT];DatabaseName=DBNAME
	*	jdbc:jtds:sqlserver://HOSTNAME[:PORT];DatabaseName=DBNAME[;tds=4.2] (mssql 6.5+)
	*	jdbc:jtds:sqlserver://HOSTNAME[:PORT];DatabaseName=DBNAME[;tds=7.0] (mssql 7.0+)
	*	jdbc:jtds:sqlserver://HOSTNAME[:PORT];DatabaseName=DBNAME[;tds=8.0] (mssql 2000 (default))
	*jdbc:mysql://HOSTNAME[:PORT]/DBNAME
	*jdbc:db2://HOSTNAME[:PORT]/DBNAME
	* @return	커넥션 스트링
	*/	
	public String getUrl() 
	{
		if ( this.url != null ) {
			return this.url;
		} else {
			return "";
		}
	}
	
	/**
	* RDBMS의 계정이름을 얻는다.
	* <p>
	* @return	계정명
	*/	
	public String getUsername() 
	{
		if ( this.username != null ) {
			return this.username;
		} else {
			return "";
		}
	}
	
	/**
	* RDBMS의 계정 패스워드를 얻는다.
	* <p>
	* @return	패스워드
	*/	
	public String getPassword() 
	{
		if ( this.password != null ) {
			return this.password;
		} else {
			return "";
		}
	}
	
	public String getHostName() {
		return "N/A";
	}
	
	public String getDbName() {
		return "N/A";
	}
	
	/**
	* RDBMS의 커넥션객체를 생성한다.
	* <p>
	* @return	커넥션객체
	*/		
	public Connection getConnection()
	throws ClassNotFoundException, SQLException
	{
		Connection oCon = null;
		
		Class.forName( this.getDriverClass() );
		
		oCon = DriverManager.getConnection( this.getUrl(),
				this.getUsername(),
				this.getPassword()
				);
		return oCon;
	}

	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer( "ClassName:RdbmsConnector {\r\n" );
		
		osb.append( "rdbmsType=" );
		osb.append( this.rdbmsType + "\r\n" );
				
		osb.append( "driverClass=" );
		osb.append( this.driverClass + "\r\n" );
		
		osb.append( "url=" );
		osb.append( this.url + "\r\n" );
		
		osb.append( "userame=" );
		osb.append( this.username + "\r\n" );		
		
		osb.append( "password=" );
		osb.append( this.password + "\r\n" );
		
		return 	osb.toString();	 
	}
}
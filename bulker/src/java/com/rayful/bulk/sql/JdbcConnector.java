/**
 *******************************************************************
 * 파일명 : JdbcConnector.java
 * 파일설명 : Jdbc를 통해 커넥션을 맺는 클래스들의 Base Interface를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.sql;

import java.sql.*;

/**
 * Jdbc를 통해 커넥션을 맺는 클래스들의 Base Interface<p>
 * 이 Interface를 implemets하는 클래스는 다음 메서드를 반드시 구현해야 한다.<p>
*/
public interface JdbcConnector 
{
	
	/**
	* 스토리지의 Driver명을 얻는다.
	* <p>
	* @return	Driver명
	*/	
	public String getDriverClass();
		
	/**
	* 스토리지의 접속정보(URL)을 얻는다.
	* <p>
	* @return	커넥션 스트링
	*/	
	public String getUrl();
	
	/**
	* 스토리지의 접속계정을 얻는다.
	* <p>
	* @return	계정명
	*/		
	public String getUsername();
	
	/**
	* 스토리지의 접속패스워드을 얻는다.
	* <p>
	* @return	접속패스워드
	*/	
	public String getPassword();
	
	/**
	 * 호스트명을 알아낸다.
	 * @return "N/A"를 리턴 
	 */
	public String getHostName();
	
	/**
	 * 디비명을 알아낸다.
	 * @return "N/A"를 리턴 
	 */
	public String getDbName();
	
	/**
	* 스토리지의 커넥션객체를 생성한다.
	* <p>
	* @return	커넥션객체
	*/			
	public Connection getConnection()
	throws ClassNotFoundException, SQLException;
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/	
	public String toString();		
}
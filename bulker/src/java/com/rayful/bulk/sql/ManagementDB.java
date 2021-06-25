/**
 *******************************************************************
 * 파일명 : ManagementDB
 * 파일설명 : 관리DB의 조회/수정을 담당하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/07/15   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.sql;

import org.apache.log4j.*;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.RayfulLogger;

import java.sql.*;

public class ManagementDB 
{
	/** logger 지정 */	
	static Logger logger = RayfulLogger.getLogger(ManagementDB.class.getName(), Config.LOG_PATH );
		
	Connection mo_connection;
	
	public final static int INDEX_STATE_NONE = 0;
	public final static int INDEX_STATE_NORMAL = 1;
	public final static int INDEX_STATE_INDEXING = 2;
	public final static int INDEX_STATE_ERROR = 3;
	
	public ManagementDB ( Connection oMetaDBConnection ) 
	{
		mo_connection = oMetaDBConnection;
	}
	
	public int getIndexState ( String sSSTableName )
	throws SQLException
	{
		
		ResultSet oRset = null;
		String sSql = null;
		PreparedStatement oPstmt = null;
		
		StringBuffer oSbSql = new StringBuffer();
		String sTableName = sSSTableName.toLowerCase();
		String sIndexState = null;
		int iIndexState ;
		
				
		if ( mo_connection == null ) {
			throw new SQLException( "관리DB커넥션이 NULL입니다.");
		}
		
		oSbSql.append ( "SELECT INDEX_STATUS_CODE FROM TB_ESP_INDEXTABLIST " );
		oSbSql.append ( "WHERE INDEX_TABLE_NAME = ? " );
		sSql = oSbSql.toString();

  	try {
			
	  	oPstmt = mo_connection.prepareStatement( sSql );
			oPstmt.setString ( 1, sTableName );
			// 조회
  		oRset= oPstmt.executeQuery();
  		
  		if ( oRset.next() ) {
  			sIndexState = oRset.getString( 1 );
  		} else {
  			throw new SQLException ( "색인목록에 색인테이블명(" + sTableName + ")을 조회할 수 없습니다. ");
  		}
  		
  		if ( sIndexState != null ) {
  			try {
  				iIndexState = Integer.parseInt( sIndexState );
  			} catch ( NumberFormatException nfe ) {
  				throw new SQLException ( "색인상태의 숫자값이 아닙니다.");
  			}
  		} else {
  			throw new SQLException ( "색인파일명(" + sTableName + ")의 상태값이 null입니다. ");
  		}
  		
  		return iIndexState;
  		
  	} catch ( SQLException se ) {
  		throw se; 
  	} finally {
	  	// 자원해제
			if ( oRset != null ) { try { oRset.close(); } catch ( SQLException se ) {} };
			if ( oPstmt != null ) { try { oPstmt.close();} catch ( SQLException se ) {} };
		}		
	}
	
	
	public boolean setIndexState ( String sSSTableName, int iIndexState )
	{
		String sSql = null;
		PreparedStatement oPstmt = null;
		
		StringBuffer oSbSql = new StringBuffer();
		String sTableName = sSSTableName.toLowerCase();
			
		if ( mo_connection == null ) {
			return false;
		}
		
		oSbSql.append ( "UPDATE TB_ESP_INDEXTABLIST " );
		oSbSql.append ( "SET INDEX_STATUS_CODE = ? " );
		oSbSql.append ( "WHERE INDEX_TABLE_NAME = ? " );
		sSql = oSbSql.toString();

  	try {
  		mo_connection.setAutoCommit( true );
	  	oPstmt = mo_connection.prepareStatement( sSql );
			oPstmt.setString ( 1, ( new Integer(iIndexState)).toString() );
			oPstmt.setString ( 2, sTableName );
			
			// 수정
  		oPstmt.executeUpdate();
  		return true;
  		
  	} catch ( SQLException se ) {
  		return false;
  	} finally {
			try { mo_connection.setAutoCommit( false ); } catch ( SQLException se ) {};
			
			// PreparedStatement 자원해제
			if ( oPstmt != null ) { try {  oPstmt.close(); } catch ( SQLException se ) {} };
		}		
	}	

}
	

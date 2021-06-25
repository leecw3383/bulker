/**
 *******************************************************************
 * 파일명 : RdbmsDataReader.java
 * 파일설명 : 색인대상의 조회를 수행하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.sql;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.KeyData;
import com.rayful.bulk.index.SourceColumn;
import com.rayful.bulk.index.TimestampColumn;
import com.rayful.bulk.io.Attachment;
import com.rayful.bulk.io.Attachments;
import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.util.DataKeySort;
import com.rayful.bulk.util.HangulConversion;
import com.rayful.bulk.util.Utils;
import com.rayful.localize.Localization;



/**
 * 색인대상(RDBMS)의 조회를 수행하는 클래스  색인대상으로 부터 색인할 색인데이터의 대상을 찾거나(key Query), 색인데이터를 조회하거나(data Query), 첨부파일정보를 조회(file query)를 담당한다. <p>
 * @author  	정충열
 * @version  1.1
 * @see  NotesDataReader
 */
public class RdbmsDataReader
{
	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger( RdbmsDataReader.class.getName(), Config.LOG_PATH );
	
	/**
	 * 색인대상 접속객체
	 * @uml.property  name="mo_connector"
	 * @uml.associationEnd  
	 */
	RdbmsConnector mo_connector;
	/** 색인대상 Connection 객체 */
	Connection mo_connection;	
	/**
	 * 컬럼매칭정보를 위한 객체
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	ColumnMatching mo_columnMatching;
	/** Timestamp 컬럼정보  */
	List<TimestampColumn> mo_timestampColumnList;
	
	/** Previous Query SQL문 */
	String ms_preSql;
	/** Key Query SQL문 */
	String ms_keySql;
	/** Incremental Key Query SQL문 */
	String ms_keyIncSql;
	/** Data Query SQL문 */
	String ms_dataSql;
	/** 첨부파일정보 Query SQL문 */
	String ms_fileSql;
	/** Etc Query SQL 문 */
	Map<String, String> map_EtcQuery;
	
	/** Key Duplicate Remove */
	String ms_DuplicateRemove;
	
	/** Key Query Duplicate Remove */
	String ms_keyDuplicateRemove;
	
	/** Key Incremental Query Duplicate Remove */
	String ms_keyIncDuplicateRemove;
		
	/** 문서테이블의 별칭 */
	String ms_docTableAlias;
	/** 파일쿼리의 종류 ( 파일목록, 파일내용 ) */
	int mi_fileQueryType;
	/** 파일쿼리의 종류가 파일내용일때 그 컬럼의 Type */
	int mi_fileContentType;

	/** 데이터소스ID(DF에 기술) */
	String ms_dataSourceId = null;
	/** 최종색인일(DF에 기술) */
	String ms_lastDocIndexDate = null;
	
	/** PreCondition에서 읽어온 값 */
	String ms_preConditionValue = null;
	
	/** 
	* Previous Query Key를 저장하는 배열 
	* RdbmsIndexer.initDataReader()에서 참조한다.
	*/
	public String [] PreKeys;
	/** 
	* key Query Key를 저장하는 배열 
	* RdbmsIndexer.initDataReader()에서 참조한다.
	*/
	public String [] Keys;
	/** 
	* data Query Key를 저장하는 배열 
	* RdbmsIndexer.initDataReader()에서 참조한다.
	*/
	public String [] dataKeys;
	/** Data Query Multi Value 처리 읽어온 값 */
	public int dataMulti = 1;
	
	/** 
	* file Query Key를 저장하는 배열 
	* RdbmsIndexer.initDataReader()에서 참조한다.
	*/	
	public String [] fileKeys;
	
	/** FILE_QUERY 종류 : UNKNOWN */
	public static final int FQUERY_UNKNOWN = 0;
	/**  FILE_QUERY 종류 : 파일목록쿼리 */
	public static final int FQUERY_FILELIST = 1;
	/** FILE_QUERY 종류 : 파일내용쿼리 */
	public static final int FQUERY_FILECONTENT = 2;
	
	private final String INDEXDATE_STR = "#data_source@last_doc_indexdate#";
	private final String DATASOURCE_STR = "#data_source_id#";
	private final String PRECONDITION_STR = "#pre_condition#";
	
	/** 
	* 생성자
	* 색인대상컬럼정보가 설정되면서 변경된 건만을 조회하기 위한 조건문이 
	* 키쿼리에 추가된다. ( 색인모드가 INDEX_MODIFIED일 경우 )
	* <p>
	* @param	sKeyQuery 색인대상들의 Key쿼리 SQL문
	* @param	sKeyIncQuery 증분용 색인대상들의 Key쿼리 SQL문
	* @param	sDataQuery 색인데이터 쿼리 SQL문
	* @param	sDataKeyCols 색인데이터 쿼리 Parameter
	* @param	sFileQuery 색인파일정보 쿼리 SQL문
	* @param	sFileKeyCols 색인파일정보 쿼리 Parameter
	* @param	sLastDocIndexDate 최종(마지막) 색인일
	* @throws SQLException
	*/
	public RdbmsDataReader( String sPreQuery,
								String sPreKeyCols,
								String sKeyQuery,
								String sKeyCols,
								String sKeyIncQuery,
								String sKeyIncCols,
								String sDataQuery, 
								String sDataKeyCols,
								String sDataMultiCols,
								String sFileQuery,
								String sFileKeyCols,
								String sLastDocIndexDate,
								String sKeyDuplicateRemove,
								String sKeyIncDuplicateRemove,
								Map<String,String> mapEtcQuery)
	throws SQLException
	{
		if ( sPreQuery != null ) {
			ms_preSql = sPreQuery;
		} else {
			ms_preSql = null;
		}
		
		if ( sKeyQuery != null ) {
			ms_keySql = sKeyQuery;	//대문자로 변경하지 않음
		} else {
			ms_keySql = null;
		}
		
		if ( sKeyDuplicateRemove != null ) {
			ms_keyDuplicateRemove = sKeyDuplicateRemove;
		} else {
			ms_keyDuplicateRemove = "false";
		}
	
		if ( sKeyIncQuery != null ) {
			ms_keyIncSql = sKeyIncQuery;	//대문자로 변경하지 않음
		} else {
			ms_keyIncSql = null;
		}
		
		if ( sKeyIncDuplicateRemove != null ) {
			ms_keyIncDuplicateRemove = sKeyIncDuplicateRemove;
		} else {
			ms_keyIncDuplicateRemove = "false";
		}
		
		if ( sDataQuery != null ) {
			ms_dataSql = sDataQuery;	//대문자로 변경하지 않음
		} else {
			ms_dataSql = null;
		}
		
		if ( sFileQuery != null ) {
			ms_fileSql = sFileQuery;	//대문자로 변경하지 않음
		} else {
			ms_fileSql = null;
		}
		
		if ( mapEtcQuery != null ) {
			map_EtcQuery = mapEtcQuery;
		} else {
			map_EtcQuery = new HashMap<String, String>();
		}

		mo_connector = null;
		mo_connection =null;
		mo_columnMatching = null;
		
		
		// Key Query 의 Key Column을 구성
		if ( sKeyCols != null ) {
			Keys = sKeyCols.split ( "\\|" );
		}
		
		// Data Query 의 Key Column을 구성
		if ( sDataKeyCols != null ) {
			dataKeys = sDataKeyCols.split ( "\\|" );
		} else {
			logger.error( "data query 의 키컬럼이 정의되지 않았습니다" );
			throw new SQLException ( "data query의 키컬럼이 정의되지 않았습니다" );
		}
		
		if ( sDataMultiCols != null ) {
			try {
				dataMulti = Integer.parseInt(sDataMultiCols);
			} catch(Exception e) {
				dataMulti = 1;
			}
		}
		
		// File Query 의 Key Column을 구성
		if ( sFileKeyCols != null ) {
			fileKeys = sFileKeyCols.split ( "\\|" );
		}
		
		ms_lastDocIndexDate = sLastDocIndexDate;
		ms_docTableAlias = null;
		

		// timestamp_column 목록 ....
		//mo_timestampColumnList = oTimestmapColumnList;
		
	}
	
	
	/** 
	* 파일쿼리가 파일경로목록인지, 파일내용목록인지 종류를 세팅한다.
	* <p>
	* @param	iFileQueryType	파일쿼리종류
	**/	
	public void setFileQueryType ( int iFileQueryType ) 
	{
		mi_fileQueryType = iFileQueryType;
	}
	
	/** 
	* 파일쿼리가 파일경로목록인지, 파일내용목록인지를 알아낸다.
	* <p>
	* @return	파일쿼리종류
	**/	
	public int getFileQueryType () 
	{
		return mi_fileQueryType;
	}	
	
	
	/** 
	* 파일내용컬럼의 타입을 지정한다.
	* <p>
	* @param	iFileContentType	파일내용컬럼의 타입
	**/	
	public void setFileContentType ( int iFileContentType ) 
	{
		if ( mi_fileQueryType == RdbmsDataReader.FQUERY_FILECONTENT ) {
			mi_fileContentType = iFileContentType;
		}
	}		
	
	/** 
	* 파일내용컬럼의 타입을 알아낸다.
	* <p>
	* @return	mi_fileContentType	파일내용컬럼의 타입
	**/	
	public int getFileContentType () 
	{
		return mi_fileContentType;
	}		

		
	/** 
	* 색인대상 Connection객체를 클래스외부에서 설정한다.
	* <p>
	* @param	oConnection 색인대상 커텍션객체
	**/
	public boolean setConnection ( Connection oConnection , 
																	RdbmsConnector oConnector,
																	String sDataSourceId ) 
	{
		if ( oConnection == null ) {
			//에러처리
			return false;
		} 
		
		if ( oConnector.getRdbmsType() == RdbmsConnector.RDBMSTYPE_UNKNOWN ) {
			//에러처리
			return false;
		}		
		
		mo_connection = oConnection;
		mo_connector = oConnector;
		ms_dataSourceId = sDataSourceId;
		
		
		
//		// 수정된건만을 대상으로 하는 조건 추가 (날짜조건 추가)
//		if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED ) {
//			// 변경된 건만을 조회하기 위한 SQL문을 알아낸다.
//
//			String sModifiedSql = this.getModifedWhereSql();
//			if ( sModifiedSql.length() > 0 ) {
//				//ms_keySql을 변경시킴.
//				addModifiedWhereSql( sModifiedSql );
//			}
//		}//if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED )	
		
		
		return true;
	}
		
	/** 
	* 컬럼매칭객체를 설정한다.
	* <p>
	* @param	oColumnMatching 컬럼매칭정보 객체
	**/		
	public void setColumnMatching ( ColumnMatching oColumnMatching )
	{
		if ( oColumnMatching == null ) {
			//에러처리
			return;
		}
				
		mo_columnMatching = oColumnMatching;
	}


//	/** 
//	* Key SQL문에 modified건을 찾는 SQL조건문을 합친다.
//	* <p>
//	* @param	modified건을 찾는 SQL조건문
//	* @return	true: WHERE문 있음 / false : WHERE문 없음
//	**/			
//	private void addModifiedWhereSql ( String sModifiedSql ) 
//	{
//		String sUpperSql = ms_keySql;
//		StringBuffer oSb = new StringBuffer();
//		StringTokenizer oSt = new StringTokenizer( sUpperSql, " ()\r\n\t", true );
//		
//		boolean bExistWhere = false;
//		boolean bAddedModifiedSql = false;
//		
//		String sToken;
//		int iBraceCount = 0;
//		
//		while ( oSt.hasMoreTokens() ) {
//			sToken = oSt.nextToken();
//			
//			// "("를 만나면   ")"를 만날때 까지 무조건 SQL문 StringBuffer에 append.
//			// subquery sql문 절은 parsing 하지 않고 무조건 SQL문에 포함시킴.
//			if ( sToken.equals( "(" ) ) {
//				oSb.append( sToken );
//				iBraceCount ++;
//				
//				while ( oSt.hasMoreTokens() ) {
//					// "(" 를 만날때 까지 pop - ( ) 내 스트링을 모두 비움.
//					sToken = ( String )	oSt.nextToken();
//					oSb.append( sToken );
//					
//					if ( sToken.equals( "(" ) ) {
//						iBraceCount ++;
//					}
//					
//					if ( sToken.equals( ")" ) ) {
//						iBraceCount --;
//						
//						if ( iBraceCount == 0) 	break;
//					}
//				}	// while 
//				
//				if ( ! oSt.hasMoreTokens() ) {
//					break;
//				}
//				
//			// main sql문의 "WHERE"를 만났을 때 
//			} else if ( sToken.equalsIgnoreCase( "WHERE" ) ) {
//				oSb.append( sToken );
//				bExistWhere = true;		// where절이 있음을 표시
//				
//			// main sql문의 "ORDER"를 만났을 때 
//			} else if ( sToken.equalsIgnoreCase( "ORDER" ) ) {
//				//System.out.println(" in order ");
//				if ( bExistWhere ) {
//					oSb.append( "\r\nAND " );
//				} else {
//					oSb.append( "\r\nWHERE " );
//				}
//				oSb.append( sModifiedSql );
//				oSb.append( "\r\n" );
//				
//				bAddedModifiedSql = true; // modified조건을 추가함.
//				
//				oSb.append( sToken );
//			
//			// main sql문의 "GROUP"를 만났을 때
//			} else if ( sToken.equalsIgnoreCase( "GROUP" ) ) {
//				if ( bExistWhere ) {
//					oSb.append( "\r\nAND " );
//				} else {
//					oSb.append( "\r\nWHERE " );
//				}
//				oSb.append( sModifiedSql );
//				oSb.append( "\r\n" );
//				
//				bAddedModifiedSql = true; // modified조건을 추가함.
//				
//				oSb.append( sToken );
//			
//			// 그외...
//			} else {
//				oSb.append( sToken );
//			}
//		
//		} // while
//		
//		if ( bAddedModifiedSql == false ) {
//		// modified조건이 추가되지 않았다면 ( ORDER, GROUP절이 없을 때)
//		// 이곳에서 추가함.
//			if ( bExistWhere ) {
//				oSb.append( "\r\nAND " );
//			} else {
//				oSb.append( "\r\nWHERE " );
//			}
//			oSb.append( sModifiedSql );
//		}
//		
//		// keySql문을 변경함(변경건 조회조건 추가된 keySql문으로 변경)
//		ms_keySql = oSb.toString();
//	}
//	
//	
//	/** 
//	* 색인대상쿼리의 변경여부조건을 알아낸다.
//	* <p>
//	* @return	색인대상쿼리 변경여부조 SQL문
//	**/		
//	private String getModifedWhereSql ()
//	{
//		StringBuffer osbSql = new StringBuffer();
//		
//		String sSql = null;
//		String sWhereSql = "";
//		boolean bExistPrevSql = false;
//		TimestampColumn oTimestampColumn = null;
//		
//		for ( int i=0; i< mo_timestampColumnList.size(); i++ ) {
//			oTimestampColumn = (TimestampColumn) mo_timestampColumnList.get(i);
//			sWhereSql = this.getDateWhereSql ( oTimestampColumn );
//
//			if ( sWhereSql.length() > 0 ) {
//				if ( bExistPrevSql ) {
//					osbSql.append ( " or " );
//				}
//				osbSql.append ( sWhereSql );
//				bExistPrevSql = true;
//			}
//		}
//		
//		sSql = osbSql.toString();
//		if ( sSql.length() > 0 ) {
//			return " ( " + osbSql.toString() + " ) ";
//		} else {
//			return "";
//		}
//	}
//		
//	/** 
//	* 수정된건만을 색인시에 변경된건을 찾기위해 
//	* 색인대상쿼리의 날짜조건을 생성한다.
//	* <p>
//	* @return	날짜조건 SQL문
//	**/		
//	private String getDateWhereSql( TimestampColumn oTimestampColumn )
//	{
//		String sLastDocIndexDate = ms_lastDocIndexDate;
//		StringBuffer osbSql = new StringBuffer();
//	
//		// 최종색인일이 없을때 NullPointException 나는것을 막음
//		if ( sLastDocIndexDate == null ) {
//			sLastDocIndexDate = "";
//		}
//
//		if ( sLastDocIndexDate.length() > 0 ) {
//  	
//			if ( mo_connector.getRdbmsType() == RdbmsConnector.RDBMSTYPE_ORACLE ) {
//
//				if ( oTimestampColumn.getColumnType() == TimestampColumn.TIMESTMP_DATETIME ) {
//					osbSql.append ( oTimestampColumn.getColumnName() );
//				  	
//				} else {
//					osbSql.append ( " TO_DATE( " );
//					osbSql.append ( oTimestampColumn.getColumnName() );
//					osbSql.append ( ", 'yyyy-mm-dd HH24:MI:SS')" );
//				}
//				
//				osbSql.append ( " >= TO_DATE('" );
//				osbSql.append ( sLastDocIndexDate );
//				osbSql.append ( "', 'yyyy-mm-dd HH24:MI:SS')" );
//			  			  
//			} else if ( mo_connector.getRdbmsType() == RdbmsConnector.RDBMSTYPE_MSSQL ) {
//				/*
//				if ( oTimestampColumn.getColumnType() == Types.DATE ) {
//				  	osbSql.append ( oTimestampColumn.getColumnName() );
//				}*/
//				
//				osbSql.append ( oTimestampColumn.getColumnName() );
//			  	osbSql.append ( " >= '" );
//			  	osbSql.append ( sLastDocIndexDate );
//			  	osbSql.append ( "' " );				
//			} else {
//				logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0020" ) );
//			}
//		}
//
//		return osbSql.toString();
//	}

	/**
	 * PreCondition에서 사용하는 값을 설정
	 */
	public void setPreConditionValue(String preConditionValue)
	{
		ms_preConditionValue = preConditionValue;
	}
	
	/** 
	* 색인대상의 키 쿼리 수행 조건 SQL문을 알아낸다.
	* <p>
	* @return	색인대상 데이터쿼리 SQL문
	**/
	public String getPreQuerySql ()
	{
		return ms_preSql;
	}
	
	/** 
	* 색인대상의 데이터쿼리 SQL문을 알아낸다.
	* <p>
	* @return	색인대상 데이터쿼리 SQL문
	**/
	public String getDataQuerySql ()
	{
		return ms_dataSql;
	}
	
	/** 
	* 색인대상의 Etc 쿼리 SQL문을 알아낸다.
	* <p>
	* @return	색인대상 Etc 쿼리 SQL문
	**/
	public Map<String, String> getEtcQuerySql ()
	{
		return map_EtcQuery;
	}
	
	/**
	* Key Query를 수행하기 위한 조건이 존재하는 경우 그 값을 조회하여 결과를 리턴
	* <p>
	* @param 	sPreQuery	키 쿼리에 해당하는 조건을 조회하기 위한 쿼리
	* @throws	SQLException
	*/	
	public List<String> executePreQuery() 
	throws SQLException 
	{
		PreparedStatement oPStmt = null;
		ResultSet oRset = null;
		ResultSetMetaData oRsetmdata = null;
		String sSql = ms_preSql;
		
		int iKeyColumnCnt = 0;
		String [] arrKeyColumnNames = null;
		int iCnt = 0;
		
		List<String> lPreData = new ArrayList<String>();
		
	  	try {
			
			// PreparedStatement 생성
			oPStmt = mo_connection.prepareStatement( sSql );
				
			// 조회
	  		oRset = oPStmt.executeQuery();
	  		
	  		// 컬럼정보
	  		oRsetmdata = oRset.getMetaData();
	  		
	  		// 컬럼명을  알아낸다.
	  		iKeyColumnCnt = oRsetmdata.getColumnCount();
	  		arrKeyColumnNames = new String [iKeyColumnCnt];
	  		for ( int i=0; i< iKeyColumnCnt ; i++) {
	  			arrKeyColumnNames[i] = oRsetmdata.getColumnName(i+1);
	  		}
	  		
	  		// logger에서 사용하는 로그 번호 다시 생성하도록 한다.
	  		logger.info( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0022" ) );
	  		
	  		// 컬럼값을 맵에 넣는다.
	  		// PreQuery에서 리턴하는 값은 1개로 한정한다.
	  		// 여러개의 값을 받아서 처리해야 하는 경우는 필요하면 다시 로직을 구성하도록 한다.
	  		while ( oRset.next() ) {
	  			
	  			lPreData.add( (String)HangulConversion.fromDB( oRset.getString(1) ) ); 
	  			
	  			System.out.print( Localization.getMessage( RdbmsDataReader.class.getName() + ".Console.0001" ) );
	  			
	  			iCnt++;
		  	}
	  		System.out.println( Localization.getMessage( RdbmsDataReader.class.getName() + ".Console.0003" ) );
	  		logger.info( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0023", iCnt ) );

	  	} catch ( SQLException se ) {
	  		logger.error ( sSql, se );
	  		throw se; // SQLException을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
	  	} finally {
	  		try { if ( oPStmt != null ) { oPStmt.close(); }} catch ( SQLException se ) {}
	  		try { if ( oRset != null ) { oRset.close(); }} catch ( SQLException se ) {}	  		
	  	}
	  	
	  	return lPreData;
	}
	
	/**
	* 색인대상의 키들을 쿼리하여 결과를 리턴
	* <p>
	* @param	iMaxRows	색인제한수
	* @param 	KeyData 객체 ( 조회결과를 리턴)
	* @throws	SQLException
	*/	
	public void  executeKeyQuery( KeyData oKeyData, int iMaxRows ) 
	throws SQLException 
	{
		PreparedStatement oPStmt = null;
		ResultSet oRset = null;
		ResultSetMetaData oRsetmdata = null;
		String sSql = null;
		
		int iKeyColumnCnt = 0;
		String [] arrKeyColumnNames = null;
		int iCnt = 0;
		int iBind = 0;
		String aBindParam[] = null;
		
		List<Map<String,String>> lPreData = null;
		
	  	try {
		
			// Query문 생성
			//sSql = ms_keySql;
			//logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0001", sSql ) );
	  		
	  		// Query문 생성
			//---------------------------------------------------
			// 2010.11.11 정충열 [증분색인쿼리관련]
			//---------------------------------------------------
	  		if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED ) {	// 증분색인일 경우
	  			sSql = ms_keyIncSql;
	  			ms_DuplicateRemove = ms_keyIncDuplicateRemove;
	  		} else {	// 전체색인일 경우 
	  			sSql = ms_keySql;
	  			ms_DuplicateRemove = ms_keyDuplicateRemove;
	  		}
	  		
	  		HashMap<String, Object> mapData = getBindQuery(sSql);
	  		
	  		if( mapData != null ) {
	  			sSql = (String)mapData.get("SQL");
	  			aBindParam = (String[])mapData.get("PARAM");
	  		}

	  		logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0001", sSql ) );

			// DBConnection 체크
			if ( mo_connection == null ) {
				throw new SQLException ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Exception.0001" ) );
			}
			
			// PreparedStatement 생성
			oPStmt = mo_connection.prepareStatement( sSql );
			
			// 아래 aBindParam 로직하고 어떻게 잘 조화를 이룰 수 있을까?
			// pre_query가 정의되어 있다면 항상 첫번째 bindParam으로 사용하도록 정의!!!
//			if ( Keys != null && Keys.length > 0 ) {
//				oPStmt.setString(1, Keys[0]);
//				iBind = 1;
//			} else {
//				iBind = 0;
//			}
			
//			if ( ms_preSql != null && ms_preSql.length() > 0 ) {
//				lPreData = executePreQuery( ms_preSql );
//				iBind = 1;
//			}
			
//			String sPreKeyID = null;
			
			// 여기서 다시 수정을 하는데
			// Data Query처럼 Key Param을 사용하여 처리할 수 있도록 함.
			// 만약, 필요하다면 전체적인 구조 변경도 생각해 보자
//			for( int iPreCnt=0; iPreCnt<lPreData.size(); iPreCnt++ ) {
				
//				sPreKeyID = lPreData.get(iPreCnt).get("ID");
				
//				oPStmt.setString(1, sPreKeyID);
				
				//if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED ) {	// 증분색인일 경우
				if ( aBindParam != null && aBindParam.length > 0 ) {
					for ( int i=0; i<aBindParam.length; i++ ) {
						if( DATASOURCE_STR.equalsIgnoreCase(aBindParam[i])) {
							oPStmt.setString ( iBind + i + 1, ms_dataSourceId );
						} else if ( INDEXDATE_STR.equalsIgnoreCase(aBindParam[i])) {
							oPStmt.setString ( iBind + i + 1, ms_lastDocIndexDate );
						} else if ( PRECONDITION_STR.equalsIgnoreCase(aBindParam[i])) {
							oPStmt.setString ( iBind + i + 1, ms_preConditionValue );
						}
				  	}
				}
				
				// 조회
		  		oRset = oPStmt.executeQuery();
		  		
		  		// 컬럼정보
		  		oRsetmdata = oRset.getMetaData();
		  		
		  		// 컬럼명을  알아낸다.
		  		iKeyColumnCnt = oRsetmdata.getColumnCount();
		  		arrKeyColumnNames = new String [iKeyColumnCnt];
		  		for ( int i=0; i< iKeyColumnCnt ; i++) {
		  			// mariadb 에서 컬럼 Alias 인식이 불가하여 수정
		  			//arrKeyColumnNames[i] = oRsetmdata.getColumnName(i+1);
		  			arrKeyColumnNames[i] = oRsetmdata.getColumnLabel(i+1);
		  		}
		  		
		  		logger.info( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0002" ) );
		  		// 컬럼값을 맵에 넣는다.
		  		while ( oRset.next() ) {
		  				  			
		  			// 색인수를 제한한다면 제한한 수만큼만 메모리에 담는다.
		  			if ( iMaxRows == iCnt) break;
		  			
		  			Map<String, String> oKeyDataMap = new LinkedHashMap<String, String>();
		  			for ( int i=0; i<iKeyColumnCnt; i++) {
		  				oKeyDataMap.put( arrKeyColumnNames[i], HangulConversion.fromDB( oRset.getString(arrKeyColumnNames[i])) );
		  			}
		  			//oKeyData.add( oKeyDataMap );
		  			
		  			// 2012.02.13 Key Query Data 중복 제거 - HP.JOO
		  			if ( ms_DuplicateRemove != null && "true".equalsIgnoreCase(ms_DuplicateRemove) ) {
		  				if ( oKeyData.add( oKeyDataMap, true ) == false )
		  					iCnt ++;
		  			}
		  			else {
		  				oKeyData.add( oKeyDataMap ); 
		  				iCnt ++;
		  			}
		  			
		  			//iCnt ++;
		  			System.out.print( Localization.getMessage( RdbmsDataReader.class.getName() + ".Console.0001" ) );
					if ( iCnt % 1000 == 0 ) {
						System.out.println ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Console.0002", iCnt ) );
					}
			  	}
		  		System.out.println( Localization.getMessage( RdbmsDataReader.class.getName() + ".Console.0003" ) );
		  		logger.info( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0003", iCnt ) );
		  		
		  		try { if ( oRset != null ) { oRset.close(); }} catch ( SQLException se ) {}	
//			}
	  		 		
	  	} catch ( SQLException se ) {
	  		logger.error ( sSql, se );
	  		throw se; // SQLException을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
	  	} finally {
	  		try { if ( oPStmt != null ) { oPStmt.close(); }} catch ( SQLException se ) {}
	  		try { if ( oRset != null ) { oRset.close(); }} catch ( SQLException se ) {}	  		
	  	}
	}

	/**
	* 색인할 데이터를 쿼리하여 결과를 리턴
	* 본문관련 컬럼은 oFileAppender를 이용 내용파일에 기록한다.
	* 첨부파일정보는 oAttachments에 추가한다.
	* <p>
	* @param	aParams	색인대상의 Key값들
	* @param	oFileAppender	본문파일 Appender(OUTPUT)
	* @param	oAttachments	첨부파일정보 목록(OUTPUT)
	* @return	문서정보 쿼리결과
	* @throws Excetpion
	*/
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> executeDataQuery( List<Map<String,String>> listKey, String [][] aParams, int iMultiVal )  
	throws Exception
	{
		PreparedStatement oPstmt = null;
		ResultSet oRset = null;		
		Map<String, Object> oReaderMap = new LinkedHashMap<String, Object>();
		List<Map<String, Object>> listMap = new ArrayList<Map<String,Object>>();
		
	  	// 조회한 값을 읽어서 저장.
	  	String sReadValue = null;
	  	int iColumnType = 0;
  		int iSystemInfoType = 0;	  	
	  	//int iFileInfoType = -1;
	  	SourceColumn oSourceColumn = null;
	  	
	  	File tmpFile = null;
	  	int bodyFileCnt = 0;
	  	boolean bNotFound = true;
	  	String sDataSql = "";
	  	
		try {
			
			// PreparedStatement 생성
			// 기존 방식대로 ? 으로 설정하도록 함.
			// 단, data_query keys="ID1|ID2|ID3" 으로 정의되어 있으면
			// ? 도 순서대로 매핑이 되어야 함.
			
//			// 기존에는 ? 으로 data_query 에서 key 정보를 설정했으나
//			// 변경된 방식에서는 keys 정보를 매핑하도록 한다.
//			// 즉, data_query keys="ID1|ID2|ID3" 으로 되어 있었으면
//			// 기존에는 WHERE SITE_CD = ? 으로 되어 있었음.
//			// 이를 WHERE SITE_CD = #ID1# 으로 변경함.
//			// 여기서 #ID1#, #ID2#, #ID3# 으로 되어 있는 정보를 ? 으로 변환 처리하도록 한다.
//			for(int i=0; i<dataKeys.length; i++) {
//				sDataSql = ms_dataSql.replace("#"+dataKeys[i]+"#", "?");
//			}
				
			oPstmt = mo_connection.prepareStatement( ms_dataSql );
			
			if ( listKey != null ) {
				
				//System.out.println("loop ");
				int iStmtCnt = 1;
				for( int j=0; j<listKey.size(); j++) {
					for ( int i=0; i<aParams.length; i++ ) {
						//System.out.print(" [" + i + "][" + j + "] : " + aParams[i][j]);
						oPstmt.setString ( iStmtCnt++, aParams[i][j] );
					}
			  	}
				//System.out.println("");
				
//				List<DataKeySort> listKeySort = new ArrayList<DataKeySort>();
//				
//				//int iBindPos[] = new int[aParams.length*aParams[0].length];
//				
//				//int iCnt=0;
//				String sTmpSql = ms_dataSql;
//				for ( int i=0; i<dataKeys.length; i++ ) {
//					boolean bLoop = true;
//					int iPos = -1;
//					while(bLoop) {
//						iPos = sTmpSql.indexOf("#"+dataKeys[i]+"#");
//						if ( iPos == -1 ) {
//							bLoop = false;
//						}
//						else {
//							sTmpSql = sTmpSql.replaceFirst("#"+dataKeys[i]+"#", "@"+dataKeys[i]+"@");
//							//iBindPos[iCnt] = iPos;
//							//iCnt++;
//							listKeySort.add(new DataKeySort(dataKeys[i],iPos));
//						}
//					}
//				}
//				
//				//System.out.println("##### 정렬하기 전 #####");
//				//Utils.printListDataKeySort(listKeySort);
//				// 리스트를 Navi순으로 정렬
//			    Collections.sort(listKeySort, new DataKeySortComparator());
//			    //System.out.println("##### 정렬한 후 #####");
//			    //Utils.printListDataKeySort(listKeySort);
			    
			} else {
				
			  	for ( int i=0; i< aParams.length; i++ ) {
			  		oPstmt.setString ( i+1, aParams[i][0] );
			  	}
			}
		  	
	  		// 조회
			//System.out.println(new java.util.Date().getTime());
			//long startDate = new java.util.Date().getTime();
	  		oRset = oPstmt.executeQuery();
	  		//long endDate = new java.util.Date().getTime();
	  		//System.out.println("time : " + String.valueOf(endDate-startDate));
	
	  		if ( logger.isDebugEnabled() ) {
	  			logger.debug ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0004", ms_dataSql ) );
	  		}
	  		
	  		if ( listKey != null ) {
	  			int iStmtCnt = 1;
	  			for( int j=0; j<listKey.size(); j++) {
					for ( int i=0; i<aParams.length; i++ ) {
						//if ( logger.isDebugEnabled() ) {
							logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", iStmtCnt++, aParams[i][j].toString() ) );
						//}
					}
	  			}
	  		} else {
			  	for ( int i=0; i< aParams.length; i++ ) {
			  		if ( logger.isDebugEnabled() ) {
				  		// aParams[i].toString() 수정해야 함.
				  		logger.debug ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", (i+1), aParams[i][0].toString() ) );
			  		}
			  	}
	  		}
	  		
		  	//if ( oRset.next() ) {
		  	//while( oRset.next() ) {
		  	for ( int iRsetCnt=0; iRsetCnt<this.dataMulti; iRsetCnt++) {
		  		
		  		if ( oRset.next() == false ) {
		  			break;
		  		}
		  		
		  		bNotFound = false;
		  		
		  		// * 주의사항 *
				// LONGVARCHAR, LONGVARBINARY컬럼이 배열의 처음부분에 오도록 조정된 컬럼목록배열을 받아온다.
				// LONGVARCHAR, LONGVARBINARY컬럼을 먼저 조회하지 않으면[스트림이 닫혀져있다]는 에러가 발생
				Object [] aKey = mo_columnMatching.getSouceColumnNames();
				for ( int i=0; i< aKey.length; i++ ) {
					sReadValue = null;
					oSourceColumn = ( SourceColumn ) mo_columnMatching.source.get( aKey[i].toString() );
					iColumnType = oSourceColumn.getColumnType();
					//iFileInfoType = oSourceColumn.getFileInfoType();
					iSystemInfoType = oSourceColumn.getSystemInfoType();
					
					// ================================================================
					// 1. LONGVARCHAR (본문으로 인식)
					// ================================================================					
					if ( iColumnType == Types.LONGVARCHAR ) {
						/*
						bodyFileCnt ++;
						tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , ".tmp", new File(Config.DOWNLOAD_PATH) );
						FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
						sReadValue = null;
						*/
						Reader oReader =  oRset.getCharacterStream( aKey[i].toString() );
						if ( oReader != null ) {

							bodyFileCnt ++;
							tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , ".tmp", new File(Config.DOWNLOAD_PATH) );
							FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
							sReadValue = null;

							try {
								oFileAppender.append ( oReader );
								sReadValue = tmpFile.getPath();
							} catch ( FileNotFoundException fnfe ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0006" ) );
								throw fnfe;
							} catch ( IOException ie ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0007" ) );
								throw ie;
							}
						}
						
					// ================================================================
					// 2. LONGVARBINARY (본문 혹은 첨부파일로 인식)
					// ================================================================							
					} else if ( iColumnType == Types.LONGVARBINARY ) {
						/*
						bodyFileCnt ++;
						tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , "tmp", new File(Config.DOWNLOAD_PATH) );
						FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
						sReadValue = null;
						*/
						
						InputStream oInputStream =  oRset.getBinaryStream( aKey[i].toString() );
						if ( oInputStream != null ) {
							
							bodyFileCnt ++;
							tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , ".tmp", new File(Config.DOWNLOAD_PATH) );
							FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
							sReadValue = null;
							
							try {
								oFileAppender.append ( oInputStream );
								sReadValue = tmpFile.getPath();
							} catch ( FileNotFoundException fnfe ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0008" ) );
								throw fnfe;
							} catch ( IOException ie ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0009" ) );
								throw ie;
							}
						} else {
							logger.info( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0010" ) );
						}
						
	
					// ================================================================
					// 3. CLOB (본문으로 인식)
					// ================================================================		
					} else if ( iColumnType == Types.CLOB ) {
						/*
						bodyFileCnt ++;
						tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , "tmp", new File(Config.DOWNLOAD_PATH) );
						FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
						sReadValue = null;
						*/
						
						Clob oClob = oRset.getClob( aKey[i].toString() );
						if ( oClob != null ) {
							
							bodyFileCnt ++;
							tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , ".tmp", new File(Config.DOWNLOAD_PATH) );
							FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
							sReadValue = null;
							
							Reader oReader = oClob.getCharacterStream();
							
							try {
								oFileAppender.append ( oReader );
								sReadValue = tmpFile.getPath();
							} catch ( FileNotFoundException fnfe ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0011" ) );
								throw fnfe;
							} catch ( IOException ie ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0012" ) );
								throw ie;
							}						
						}
							
					// ================================================================
					// 4. BLOB (본문 혹은 첨부파일로 인식)
					// ================================================================							
					} else if ( iColumnType == Types.BLOB ) {
						/*
						bodyFileCnt ++;
						tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , "tmp", new File(Config.DOWNLOAD_PATH) );
						FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
						sReadValue = null;
						*/
						
						Blob oBlob = oRset.getBlob( aKey[i].toString() );
						if ( oBlob != null ) {
							
							bodyFileCnt ++;
							tmpFile = File.createTempFile("rdbms" + "_" + bodyFileCnt , ".tmp", new File(Config.DOWNLOAD_PATH) );
							FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
							sReadValue = null;

							InputStream oInputStream = oBlob.getBinaryStream();
							try { 
								// --------------------------------------------
								// 문제발생가능성 있음 (테스트 필요)
								// 2006/12/26	정충열
								// --------------------------------------------
								oFileAppender.append ( oInputStream );	// UTF-8으로 저장
								sReadValue = tmpFile.getPath();
							} catch ( FileNotFoundException fnfe ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0013" ) );
								throw fnfe;
							} catch ( IOException ie ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0014" ) );
								throw ie;
							}
						}
						
					// ================================================================
					// 5. 그외 타입 ( 스트링으로 인식)
					// ================================================================						
					} else {
						// ================================================================
						//	5-1. DF정의값
						// ================================================================								
						if ( iSystemInfoType == SourceColumn.SINFOYPE_HOSTNAME ) {
							sReadValue = mo_connector.getHostName();
						} else if ( iSystemInfoType == SourceColumn.SINFOYPE_DATASURCE_ID ) {
							sReadValue = ms_dataSourceId;
						} else if ( iSystemInfoType == SourceColumn.SINFOYPE_DBNAME ) {
							sReadValue = mo_connector.getDbName();
						} else {
						// ================================================================
						//	5-2. 일반스트링
						// ================================================================								
							// DB로 부터 읽는다.
							sReadValue = HangulConversion.fromDB( oRset.getString( aKey[i].toString() ));
						}

						if ( sReadValue != null ) {
							if (sReadValue.length() == 0 ) {
								sReadValue = null;
							}
						}						
				
						if ( sReadValue == null ) {
							// ================================================================
							// 5-2-1. NOT NULL 처리
							// ================================================================			
							if ( oSourceColumn.isNotNull() ) {
								throw new Exception ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Exception.0002", aKey[i].toString() ) );
							}
							
							// ================================================================
							// 5-2-2. Default(기본) 값처리
							// ================================================================									
							sReadValue = oSourceColumn.getDefaultValue();
						}						
						
					} // else
					
					oReaderMap.put( aKey[i].toString(), sReadValue );
				} // for
				
				listMap.add(oReaderMap);
				
				oReaderMap = new LinkedHashMap<String, Object>();
				
		  	} // while( oRset.next() )
		  	
		  	if ( bNotFound ) {
			  	throw new Exception ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Exception.0003" ) );
		  	} // if
		  	
	  	} catch ( Exception e ) {
	  		logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0004", ms_dataSql ), e );
		  	for ( int i=0; i< aParams.length; i++ ) {
		  		// aParams[i].toString() 수정해야 함.
		  		if ( aParams[i].length > 1 ) {
		  			StringBuffer sb = new StringBuffer("");
		  			for(int j=0; j<aParams[i].length; j++) {
		  				sb.append(" ").append(aParams[i][j]);
		  			}
		  			logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", (i+1), sb.toString() ) );
		  			
		  		} else {
		  			logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", (i+1), aParams[i].toString() ) );
		  		}
		  	}
		  	
		  	if ( oSourceColumn != null ) {
		  		logger.info ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0015", oSourceColumn.getColumnName() ) );
		  	}
		  	
	  		throw e; 
  		} finally {
	  	// ResultSet 자원해제
			try { if ( oRset != null ) { oRset.close(); } } catch ( SQLException se ) {}
			// PreparedStatement 자원해제
			try { if ( oPstmt != null ) { oPstmt.close(); } } catch ( SQLException se ) { }
		}
		
		return listMap;
	}

	/**
	* 첨부파일정보를 쿼리하여 결과를 oAttachments에 추가
	* @param	aParams 쿼리시 필요한 조건값들을 가진 배열
	* @param	oAttachments 첨부파일목록정보를 저장할 객체(OUTPUT)
	* @throws SQLException
	*/
	public void executeFileInfoQuery( String [] aParams, Attachments oAttachments ) 
	throws SQLException 
	{
		PreparedStatement oPstmt = null;
		ResultSet oRset = null;
		Attachment oAttachment = null;
		int iCnt;
		String sFileId = null;
		String sOrgFileName = null;
  	
		try {
	  		// PreparedStatement 생성
	  		oPstmt = mo_connection.prepareStatement( ms_fileSql );
	
				// 쿼리의 Key값 세팅
		  	for ( int i=0; i< aParams.length; i++ ) {
		  		oPstmt.setString ( i+ 1, aParams[i] );
		  	}
		  	
		  	if ( logger.isDebugEnabled() ) {
		  		logger.debug ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0016", ms_fileSql ) );
		  	}
		  	
		  	for ( int i=0; i< aParams.length; i++ ) {
		  		if ( logger.isDebugEnabled() ) {
		  			logger.debug ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", (i+1), aParams[i] ) );
		  		}
		  	}    	
	  	
	  		// 조회
	  		oRset= oPstmt.executeQuery();  		
	  	
	  		// 조회한 값을 읽어서 저장.
	  		iCnt = 0;
		  	while ( oRset.next() ) {
		  		iCnt ++;
		  		
		  		if ( mi_fileQueryType == RdbmsDataReader.FQUERY_FILELIST ) {
		  		// ================================================================
		  		// 1.파일목록을 조회한경우 
		  		// ================================================================	  			
		  			String [] aColumnValue = new String [4];
					aColumnValue[0] = HangulConversion.fromDB( oRset.getString("FILE_ID") );
					aColumnValue[1] = HangulConversion.fromDB( oRset.getString("FILE_SAVPATH") );
					aColumnValue[2] = HangulConversion.fromDB( oRset.getString("FILE_SAVNAME") );
					aColumnValue[3] = HangulConversion.fromDB( oRset.getString("FILE_ORGNAME") );
					
					// 별도 파일저장이 필요 없을 경우 사용
					oAttachment = new Attachment ( aColumnValue );
					oAttachments.addAttachment ( oAttachment );
				} else {
				// ================================================================
		  		// 2.파일내용 및 파일정보를 조회한 경우
		  		// ================================================================
						
					if ( mi_fileContentType == Types.LONGVARBINARY ) {
					// ================================================================
					// 2-1. File Content가 LONGVARBINARY 일 경우 
					// 가장 먼저 그 컬럼을 읽어야 에러가 발생하지 않는다.
					// ================================================================
						
						// Temp 파일이름, 파일 생성
						File oTempFile = new File ( Config.DOWNLOAD_PATH, "filequery_temp" + iCnt );
						FileAppender oAttFileAppender = new FileAppender ( oTempFile.getPath() );
						
						// Temp 파일에 기록
						InputStream oInputStream = oRset.getBinaryStream( "FILE_CONTENT" );
						if ( oInputStream != null ) {
							try { 
								oAttFileAppender.append ( oInputStream );
							} catch ( FileNotFoundException fnfe ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0017" ), fnfe);
							} catch ( IOException ie ) {
								logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0018" ), ie);
							}
						} else {
							continue;
						}
						
						// 파일ID를 얻는다.
						sFileId = HangulConversion.fromDB( oRset.getString ("FILE_ID") );
						// 다운로드 파일명을 얻는다.
						sOrgFileName = HangulConversion.fromDB( oRset.getString ("FILE_ORGNAME") );
						
						oAttachment = new Attachment( sFileId, sOrgFileName, null );
						// 첨부파일 정보를 기록
						oAttachments.addAttachment( oAttachment );
																
						File oSaveFileName = new File ( Config.DOWNLOAD_PATH, oAttachment.getTargetFileName() );
						// ******** 중요 *********
						// Temp 파일이름을 변경한다. 
						// ******** 중요 *********						
						if ( ! oTempFile.renameTo( oSaveFileName ) ) {
							logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0019" ));
							continue;
						} else {
							// 저장됨을 표시
							oAttachment.setSaved( true );							
						}
						
					} else if ( mi_fileContentType == Types.BLOB ) {
					// ================================================================
					// 2-2. File Content가 BLOB 일 경우 
					// ================================================================
						
						// 파일ID를 얻는다.
						sFileId = HangulConversion.fromDB( oRset.getString ("FILE_ID") );						
						// 다운로드 파일명을 얻는다.
						sOrgFileName = HangulConversion.fromDB( oRset.getString( "FILE_ORGNAME" ) );
						
						// 첨부파일 정보를 기록						
						oAttachment = new Attachment( sFileId, sOrgFileName, null );
						oAttachments.addAttachment( oAttachment );												
						
						File oSaveFileName = new File ( Config.DOWNLOAD_PATH, oAttachment.getTargetFileName() );						
						FileAppender oAttFileAppender = new FileAppender ( oSaveFileName.getPath() );
						
						// 다운로드 파일내용을 기록
						Blob oBlob = oRset.getBlob( "FILE_CONTENT" );
						if ( oBlob != null ) {
							InputStream oInputStream = oBlob.getBinaryStream();
							if ( oInputStream != null ) {
								try { 
									oAttFileAppender.append ( oInputStream );
									oInputStream.close();
								} catch ( FileNotFoundException fnfe ) {
									logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0017" ), fnfe);
								} catch ( IOException ie ) {
									logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0018" ), ie);
								}
							} else {
								continue;
							}
						} else {
							continue;
						}
						
						// 저장됨을 표시
						oAttachment.setSaved( true );							
					} // else if ( mi_fileContentType == Types.BLOB )
				} // else
		  	}	// while
	  	
	  	} catch ( SQLException se ) {
	  		logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0016", ms_fileSql ), se );
		  	for ( int i=0; i< aParams.length; i++ ) {
		  		logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0005", (i+1), aParams[i] ) );
		  	}  		
	  		throw se; // SQLException을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
	  	} finally {	
		  	// ResultSet 자원해제 
			try { if ( oRset != null ) { oRset.close(); } } catch ( SQLException se ) {}
			// PreparedStatement 자원해제 
			try { if ( oPstmt != null ) { oPstmt.close(); } } catch ( SQLException se ) { }
		}
	}
	
	
	/**
	* 오라클의 Product Version 정보를 알아낸다.
	* <p>
	* @return	오라클 버전정보
	* @throws SQLException
	*/		
	public String getOracleVersion()
	throws SQLException
	{
		Statement oStmt = null;
		ResultSet oRset = null;
		String sSql = "SELECT version FROM PRODUCT_COMPONENT_VERSION where PRODUCT LIKE 'Oracle%'";
		String sVersion = null;
		
		try {
			if ( mo_connector.getRdbmsType() != RdbmsConnector.RDBMSTYPE_ORACLE ) {
				throw new SQLException ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Exception.0004" ) );
			}
					
			oStmt = mo_connection.createStatement();
			oRset = oStmt.executeQuery( sSql );
			
			if ( oRset.next() ) {
				sVersion = oRset.getString ( 1 );
			}
			
			if ( sVersion == null ) {
				throw new SQLException ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Exception.0005" ) );
			}
			
			return sVersion;
			
		} catch ( SQLException se ) {
			logger.error( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0021" ), se );
			throw se;
		} finally {
	  	// ResultSet 자원해제 
			try { if ( oRset != null ) { oRset.close(); } } catch ( SQLException se ) {}
			// PreparedStatement 자원해제 
			try { if ( oStmt != null ) { oStmt.close(); } } catch ( SQLException se ) {}
		}
	}
		

	private HashMap<String,Object> getBindQuery(String sSql) 
	{
		String [] aBindParam = null;
		String sIndexDateParam[] = null, sDataSourceParam[] = null;
		
		String sInSql = null;
		
		int iIndexDateBind = 0, iDataSourceBind = 0, iPreConditionBind = 0;
		
		iIndexDateBind = sSql.split(INDEXDATE_STR).length;
		
		iIndexDateBind = iIndexDateBind > 0 ? iIndexDateBind - 1 : 0;
		
		iDataSourceBind = sSql.split(DATASOURCE_STR).length;
		
		iDataSourceBind = iDataSourceBind > 0 ? iDataSourceBind - 1 : 0;
		
		iPreConditionBind = sSql.split(PRECONDITION_STR).length;
		
		iPreConditionBind = iPreConditionBind > 0 ? iPreConditionBind - 1 : 0;
		
		aBindParam = new String[iIndexDateBind+iDataSourceBind+iPreConditionBind];
		
		if ( iIndexDateBind+iDataSourceBind+iPreConditionBind > 0 )
			aBindParam = bindParam(sSql, iIndexDateBind, iDataSourceBind, iPreConditionBind);
		
//		if ( iDataSourceBind > 0 )
//			sDataSourceParam = bindParam(aBindParam, sSql, iDataSourceBind, iIndexDateBind);
		
		//sSql = sSql.replace("#data_source@last_doc_indexdate#", "'" + ms_lastDocIndexDate + "'");
		sInSql = sSql.replace(INDEXDATE_STR, "?").replace(DATASOURCE_STR, "?").replace(PRECONDITION_STR, "?");
		
		logger.info("[Bind : " + iIndexDateBind + iDataSourceBind + iPreConditionBind + "] Increment SQL : " + sInSql);
		
		HashMap<String, Object> mapData = new HashMap<String, Object>();
		
		mapData.put("SQL", sInSql);
		mapData.put("PARAM", aBindParam);
//		mapData.put("INDEXDATE_PARAM", sIndexDateParam);
//		mapData.put("DATASOURCE_PARAM", sDataSourceParam);
		
		return mapData;
	}
	
	private String[] bindParam(String sSql, int iIndexDateBind, int iDataSourceBind, int iPreConditionBind)
	{
		// "#data_source@last_doc_indexdate#", iBind, "#data_source_id#", iDataSourceBind
		String aParam[] = new String[iIndexDateBind+iDataSourceBind+iPreConditionBind];
		int iPos1 = 0, iPos2 = 0, iPos3 = 0;
		boolean bBind1 = true, bBind2 = true, bBind3 = true;
		Object aBind[][] = new Object[iIndexDateBind+iDataSourceBind+iPreConditionBind][2];
		int iCnt = 0;
		
		if ( iIndexDateBind > 0 ) {
			while(bBind1) {
				iPos1 = sSql.indexOf(INDEXDATE_STR,iPos1+1);
				if(iPos1 > 0 ) {
					aBind[iCnt][0] = INDEXDATE_STR;
					aBind[iCnt][1] = iPos1;
					iCnt++;
				} else {
					bBind1 = false;
				}
			}
		}
		
		if ( iDataSourceBind > 0 ) {
			while(bBind2) {
				iPos2 = sSql.indexOf(DATASOURCE_STR,iPos2+1);
				if(iPos2 > 0 ) {
					aBind[iCnt][0] = DATASOURCE_STR;
					aBind[iCnt][1] = iPos2;
					iCnt++;
				} else {
					bBind2 = false;
				}
			}
		}
		
		if ( iPreConditionBind > 0 ) {
			while(bBind3) {
				iPos3 = sSql.indexOf(PRECONDITION_STR,iPos3+1);
				if(iPos3 > 0 ) {
					aBind[iCnt][0] = PRECONDITION_STR;
					aBind[iCnt][1] = iPos3;
					iCnt++;
				} else {
					bBind3 = false;
				}
			}
		}
		
		sortArray(aBind);
		
		//printArray(aBind);
		
		iCnt = 0;
		//String aParam[] = new String[aBind.length];
		
		for(int iLoop=0; iLoop<aBind.length; iLoop++) {
			aParam[iLoop] = (String)aBind[iLoop][0];
		}
		
		return aParam;
	}
	
	private static void sortArray(Object[][] arr) 
	{ 
		Arrays.sort(arr, new Comparator<Object[]>() { 
			@SuppressWarnings("unchecked")
			public int compare(Object[] arr1, Object[] arr2) { 
				if( ((Comparable)arr1[1]).compareTo(arr2[1]) > 0 ) 
					return 1; 
				else
					return -1; 
			} 
		}); 
	}
	
	public static void printArray(Object[][] arr) 
	{ 
		for( int i = 0; i < arr.length; i++ ) 
		{ 
			for( int j = 0; j < arr[i].length; j++ ) 
				System.out.print(arr[i][j] + "\t"); 
				System.out.println(); 
			} 
		System.out.println(); 
	} 
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer();
		osb.append( "ClassName: RdbmsDataReader \n");
		
		osb.append( "ms_keySql=" );
		osb.append( ms_keySql + "\n" );
		
		osb.append( "ms_dataSql=" );
		osb.append( ms_dataSql + "\n" );
		
		osb.append( "ms_fileSql=" );
		osb.append( ms_fileSql + "\n" );
		
		osb.append( "map_EtcQuery=" );
		osb.append( map_EtcQuery.toString() + "\n" );
		
		return 	osb.toString();
	}		
}

//젊은 순서대로 정렬하기 위해 Comparator 인터페이스를 구현
@SuppressWarnings("unchecked")
class DataKeySortComparator implements Comparator { 
  public int compare(Object o1, Object o2) {
    int by1 = ((DataKeySort)o1).keyOrder;
    int by2 = ((DataKeySort)o2).keyOrder;
    //return by1 > by2 ? -1 : (by1 == by2 ? 0 : 1); // descending 정렬.....
    return by1 < by2 ? -1 : (by1 == by2 ? 0 : 1); // ascending 정렬.....
  }
}	
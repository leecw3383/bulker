/**
 *******************************************************************
 * 파일명 : ColumnMatching.java
 * 파일설명 : 색인대상컬럼과 색인테이블 컬럼의 매칭정보를 유지하는
 * 						클래스 정의 파일
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/24   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.index;

import java.util.*;
import java.sql.*;

import com.rayful.bulk.ESPTypes;
import com.rayful.localize.Localization;

/**
 * 색인대상컬럼과 색인테이블 컬럼의 매칭정보를 기억하는 클래스
*/
public class ColumnMatching {
	/** 색인대상의 컬럼정보를 유지 */
	public Map<String, SourceColumn> source;	
	/** 색인테이블의 컬럼정보를 유지 */
	public Map<String, TargetColumn> target;	
	
	/** RDBMS 색인대상의 컬럼중 LONG TYPE이 컬럼명의 목록을 유지*/
	Object []  ma_sortedSourceColumnNames;
	
	// NAMO Content Type을 위해서 20100622 추가
	/** RDBMS 색인대상 컬럼중 본문 내용이 NAMO Type인지 정의 */
	public final static String CONTENT_TYPE_NAMO = "namo";
	/** RDBMS 색인대상 컬럼중 내용이 HTML 정의 */
	public final static String CONTENT_TYPE_HTML = "html";
	
	/**
	* 생성자<p>
	* 컬럼들을 관리하는 Map을 생성
	*/	
	public ColumnMatching() {
		source = new LinkedHashMap<String, SourceColumn>();
		target = new LinkedHashMap<String, TargetColumn>();
		
		ma_sortedSourceColumnNames = null;
	}
	
	/**
	* 색인대상 컬럼목록을 TYPE이 LONGVARCHAR인 컬럼이 먼저오도록 정렬한다.
	* 색인대상이 RDBMS인 경우만 호출된다.
	* 원래는 DF에 정의된 순서로 컬럼목록이 구성되지만 
	* longvarchr컬럼은 가장 먼저읽도록 컬럼목록의 순서를 조정한다.
	* longvarchar컬럼을 가장 먼저 읽지 않으면 에러가 발생하기 때문이다.
	* <p>
	*/		
	public void sortSoucreColumnNames()
	{
		ArrayList<Object> oLongvarcharColumns = new ArrayList<Object>();
		ArrayList<Object> oNonLongvarcharColumns = new ArrayList<Object>();
		
		int iLongvarcharCnt;
		int iNonLongvarcharCnt;
		int iCurPos =0;
		
		Object [] aKey = this.source.keySet().toArray();
		int iKeyCnt = aKey.length;
		
		for ( int i=0; i< iKeyCnt; i++ ) {
			SourceColumn oSourceColumn = ( SourceColumn )this.source.get( aKey[i] );
			if ( oSourceColumn.getColumnType() == Types.LONGVARCHAR ||
						 oSourceColumn.getColumnType() == Types.LONGVARBINARY ) {
				oLongvarcharColumns.add( aKey[i] );
			} else {
				oNonLongvarcharColumns.add( aKey[i] );
			}
		}
		
		iLongvarcharCnt = oLongvarcharColumns.size();
		iNonLongvarcharCnt = oNonLongvarcharColumns.size();
		
		if ( iLongvarcharCnt > 0 ) {
			ma_sortedSourceColumnNames = new Object [ iKeyCnt ];
			// LONGVARCHAR타입의 컬럼명을 담는다.
			for ( int i=0; i< iLongvarcharCnt; i++ ) {
				ma_sortedSourceColumnNames[iCurPos] = oLongvarcharColumns.get(i);
				iCurPos ++;
			}
			
			// 그외타입의 컬럼명을 담는다.
			for ( int i=0; i< iNonLongvarcharCnt; i++ ) {
				ma_sortedSourceColumnNames[iCurPos] = oNonLongvarcharColumns.get(i);
				iCurPos ++;
			}
		}
	}	
	
	/**
	* 컬럼TYPE이 LONGVARCHAR인 컬럼이 처음으로 위치한 컬럼목록을 알아낸다.
	* 색인대상이 RDBMS인 경우만 호출된다.
	* longvarchar컬럼을 가장 먼저 읽지 않으면 에러가 발생하기 때문이다.
	* <p>
	*/	
	public Object[] getSouceColumnNames() {
		
		if ( ma_sortedSourceColumnNames != null ) {
			return ma_sortedSourceColumnNames;
		} else {
			return this.source.keySet().toArray();
		}
	}
	
	
	/**
	* 색인대상(RDBMS)에 접근하여 source 컬럼정보를 읽어온다.
	* <p>
	* @param	oCon	RDBMS 커넥션객체
	* @param	sDataQuerySql	데이터쿼리 SQL문
	* @param	iParamCnt	데이터 쿼리 SQL문의 Parameter수 (?문자 갯수)
	*/		
	public void loadSourceColumnInfo( Connection oCon, String sDataQuerySql, int iParamCnt )
	throws SQLException
	{
		PreparedStatement oPstmt = null;
		ResultSet oRset = null;
		ResultSetMetaData oRsetmdata = null;
		boolean bExistsLongvarcharColumn = false;
		
		String sTargetColumnName = null;
		TargetColumn oTargetColumn = null;
		
		if ( iParamCnt < 1 ) {
			throw new SQLException ( Localization.getMessage( ColumnMatching.class.getName() + ".Exception.0001" ) );
		}
		
		try {
			oPstmt = oCon.prepareStatement( sDataQuerySql );
			for ( int i=1; i<=iParamCnt; i++ ) {
				oPstmt.setString( i, "");
			}
			oPstmt.executeQuery();
			
			oRset = oPstmt.executeQuery();
			oRsetmdata = oRset.getMetaData();
			
			for (int i=1; i<= oRsetmdata.getColumnCount(); i++) {
				String sColumnName;
				int iColumnType;
				SourceColumn oSourceColumn = null;
				
				// 컬럼명은 항상 대문자임
				// mariadb 에서 컬럼 Alias 인식이 불가하여 수정
				//sColumnName = oRsetmdata.getColumnName(i);
				sColumnName = oRsetmdata.getColumnLabel(i);
				iColumnType = oRsetmdata.getColumnType(i);
				
				//System.out.println ( "*******" + sColumnName + "=" + iColumnType );
				
				oSourceColumn = (SourceColumn) this.source.get( sColumnName );
				if ( oSourceColumn != null ) {
					// 타입이 LONGVARCHAR인 컬럼명을 별도로 보관
					if ( iColumnType == Types.LONGVARCHAR ) {
						bExistsLongvarcharColumn = true;
					}
					oSourceColumn.setColumnType( iColumnType );
				}
				
				// -----------------------------------------------
				// 이 로직이 올바르게 수행되기 위해서는
				// 소스컬럼과 관계된 대상컬럼은 오직 하나여야 한다.
				// 이 가정이 무너지면 아래 로직은 올바로 동작하지 않을 것이다.
				// -----------------------------------------------
				if ( iColumnType == Types.LONGVARCHAR ||
						iColumnType == Types.LONGVARBINARY || 
						iColumnType == Types.CLOB || 
						iColumnType == Types.BLOB 
					) {
					
					sTargetColumnName = oSourceColumn.getTargetColumnName();
					if ( sTargetColumnName != null ) {
						oTargetColumn = ( TargetColumn ) this.target.get( sTargetColumnName ) ;
						
						// 색인대상컬럼의 타입을  LONGTEXT로  강제로 설정
						if ( oTargetColumn != null ) {
							oTargetColumn.setColumnType( ESPTypes.LONGTEXT  );
						} else {
						// 색인대상컬럼의 타입을  LONGTEXT로  강제로 설정
						// 매칭되는 타겟컬럼이 없다면 에러를 발생
							throw new SQLException ( Localization.getMessage( ColumnMatching.class.getName() + ".Exception.0002" ) );
						}
					} else {
						throw new SQLException ( Localization.getMessage( ColumnMatching.class.getName() + ".Exception.0002" ) );
					}
				}
				// -----------------------------------------------			
			}
			
			if ( bExistsLongvarcharColumn ) {
				// longvarchar 컬럼이 컬럼목록의 처음에 위치하도록 컬럼목록정렬
				this.sortSoucreColumnNames();
			}
			
		} catch ( SQLException se ) {
			System.out.println ( Localization.getMessage( ColumnMatching.class.getName() + ".Console.0001", sDataQuerySql ) );
			throw se;
		} finally {
			// Resultset 자원해제
			try { if ( oRset != null ) { oRset.close();} } catch ( SQLException se ) {}
			// PreparedStatement 자원해제
			try { if ( oPstmt != null ) { oPstmt.close();} } catch ( SQLException se ) {}		
		}
	}
	
	
	/**
	* 객체의 멤버변수 내용을 하나의 String으로 만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/			
	public String toString() 
	{
		StringBuffer oSb = new StringBuffer("ClassName:ColumnMatching{\r\n");
		
		oSb.append ( "source={\r\n");
		if ( source != null ) {
			Object [] objKey = source.keySet().toArray();
			for ( int i=0; i<objKey.length; i++ ) {
				oSb.append( source.get(objKey[i]) );
			}
		}
		oSb.append ( " }\r\n");
		oSb.append ( "\r\n");
		oSb.append ( "target={\r\n");
		if ( target != null ) {
			Object [] objKey = target.keySet().toArray();
			for ( int i=0; i<objKey.length; i++ ) {
				oSb.append( target.get(objKey[i]) );
			}
		}
		oSb.append ( " }\r\n");
		
		return oSb.toString();
	}
	
}
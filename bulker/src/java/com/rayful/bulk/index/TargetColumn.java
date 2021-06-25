/**
 *******************************************************************
 * 파일명 : TargetColumn.java
 * 파일설명 : 색인테이블의 컬럼정보를 담는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/06   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.index;

import com.rayful.bulk.ESPTypes;

/**
 * 색인테이블의 컬럼정보를 담는 클래스 <p>
*/
public class TargetColumn 
{
	String ms_columnName;			// 컬럼명
	int mi_columnType;				// 컬럼 Type
	String ms_sourceColumnNames;	// 저장될 색인테이블 컬럼
	String ms_columnText;			// 텍스트부분
	boolean mb_key;					// PK여부
	boolean mb_notNull;				// notnull 여부
	String ms_ContentType;			// Content Type

	/**
	* 생성자 
	* <p>
	* @param sColumnName	색인컬럼명
	*/	
	public TargetColumn( String sColumnName) 
	{
		ms_columnName = sColumnName;
		ms_sourceColumnNames = "";
		ms_columnText = null;
		mb_key = false;
		mi_columnType = ESPTypes.NONE;
		mb_notNull = false;	
		ms_ContentType = "";
	}
	
	/**
	* 컬럼명을 알아낸다. 
	* <p>
	* @return	컬럼명
	*/		
	public String getColumnName ()
	{
		return ms_columnName;
	}
	
	/**
	* 컬럼 Type을 알아낸다. 
	* <p>
	* @return	컬럼 Type
	*/		
	public int getColumnType ()
	{
		return mi_columnType;
	}	
	
	
	/**
	* 색인대상 컬럼명을 알아낸다. 
	* <p>
	* @return	컬럼명
	*/		
	public String getSourceColumnNames ()
	{
		return ms_sourceColumnNames;
	}
	
	/**
	* 색인대상 컬럼Text 부분을 알아낸다.
	* <p>
	* @return	컬럼Text
	*/		
	public String getColumnText ()
	{
		return ms_columnText;
	}
	
	/**
	 * 현재 본문을 구성하는 Content의 종류를 알아낸다.
	 * <p>
	 * @return Content Type( ex : NAMO)
	 */
	public String getContentType() {
		return ms_ContentType;
	}
	
	/**
	* 현재컬럼이 색인테이블의 Primary Key Column인지 알아낸다. 
	* <p>
	* @return	true: PK column / false : normal column
	*/		
	public boolean isKey ()
	{
		return mb_key;
	}	
		
	
	/**
	* 컬럼 Type을 설정한다. 
	* <p>
	* @param	sColumnType	컬럼 Type
	*/		
	public void setColumnType ( int sColumnType )
	{
		//if ( mi_columnType == ESPTypes.NONE ) 
		//{
		mi_columnType = sColumnType;
		//}
	}

	/**
	* 색인대상 컬럼Text 부분을 설정한다.
	* <p>
	* @param	sColumnText	컬럼Text
	*/		
	public void setColumnText ( String sColumnText )
	{
		ms_columnText = sColumnText;
	}
		
	/**
	* 색인테이블의 PrimaryKey라고 설정한다.
	*/	
	public void setKey() 
	{
		mb_key = true;
	}
	
	
	/**
	 * 현재 본문을 구성하는 Content의 종류를 설정한다.
	 * <p>
	 * @param sContentType
	 */
	public void setContentType(String sContentType) {
		ms_ContentType = sContentType;
	}
	
	/**
	* 컬럼의 notnull 여부를 설정한다. 
	* notnull이 설정되면 데이터 조회시 값이 null일때 에러를 발생시켜 
	* 그 데이터를 색인하지 않는다. 
	* <p>
	*/		
	public void setNotNull ()
	{
		mb_notNull = true;
	}
	
	/**
	* 컬럼이 notnull이 설정된 컬럼인지 알아낸다. <p>
	* @return	true : notnull컬럼 / false : nullable 컬럼
	*/	
	public boolean isNotNull ()
	{
		return mb_notNull;
	}

	
	/**
	* 매칭되는 소스컬럼의 컬럼명을 추가한다.
	* <p>
	* @param	sSourceColumnName	매칭되는 소스컬럼명
	*/		
	public void addSourceColumnNames( String sSourceColumnName )
	{
		if ( sSourceColumnName != null ) {
			if ( ms_sourceColumnNames.length() > 0 ) {
				ms_sourceColumnNames += "||";
			}
			ms_sourceColumnNames += sSourceColumnName;
		}
	}
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* <p>
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer("ClassName:targetColumn {\r\n");
			
		osb.append( "ms_columnName=" );
		osb.append( ms_columnName + "\r\n" );
		
		osb.append( "mi_columnType=" );
		osb.append( mi_columnType + "\r\n" );
		
		osb.append( "ms_ContentType=" );
		osb.append( ms_ContentType + "\r\n" );
		
		osb.append( "ms_columnText=" );
		osb.append( ms_columnText + "\r\n" );		
		
		osb.append( "ms_sourceColumnNames=" );
		osb.append( ms_sourceColumnNames + "\r\n" );
		
		osb.append( "mb_key=" );
		osb.append( mb_key + " }\r\n" );		
		
		return 	osb.toString();	 
	}	

}
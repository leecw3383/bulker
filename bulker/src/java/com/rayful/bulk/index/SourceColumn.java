/**
 *******************************************************************
 * 파일명 : SourceColumn.java
 * 파일설명 : 색인대상 스토리지의 컬럼정보를 담는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/06   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.index;

/**
 * 색인대상 스토리지의 컬럼정보를 담는 클래스 <p>
*/
public class SourceColumn 
{
	String ms_columnName;				// 컬럼명
	int mi_columnType;					// 컬럼 Type
	
	int mi_fileInfoType;				// 파일정보 Type을 정의
	int mi_systemInfoType;			// 시스템정보 Type을 정의
	
	String ms_targetColumnName;	// 저장될 색인테이블 컬럼
	boolean mb_notNull;					// notnull 여부
	String ms_defaultValue;			// default 값

	// 파일정보 Type의 종류
	public static final int FINFOTYPE_UNKNOWN = 0;
	public static final int FINFOTYPE_FILELIST = 1;
	public static final int FINFOTYPE_FILECONTENT = 2;
	public static final int FINFOTYPE_FILENAME = 3;
	
	
	// 시스템정보 Type의 종류
	public static final int SINFOYPE_UNKNOWN = 0;
	public static final int SINFOYPE_HOSTNAME = 1;
	public static final int SINFOYPE_DATASURCE_ID = 2;
	public static final int SINFOYPE_DBNAME = 3;
	public static final int SINFOYPE_FILEID = 4;	
	public static final int SINFOYPE_NOTES_NSFNAME = 10;
	public static final int SINFOYPE_NOTES_UNVERSALID = 11;
	public static final int SINFOYPE_NOTES_MODIFIED = 12;
	public static final int SINFOYPE_NOTES_CREATED = 13;

	
	/**
	* 생성자
	* @param sColumnName	색인대상 컬럼명
	* @param sTargetColumnName	색인파일에 저장될 컬럼명
	*/	
	public SourceColumn( String sColumnName, String sTargetColumnName ) 
	{
		ms_columnName = sColumnName;
		ms_targetColumnName = sTargetColumnName;
		mi_fileInfoType = FINFOTYPE_UNKNOWN;
		mi_systemInfoType = SINFOYPE_UNKNOWN;
		mb_notNull = false;
		ms_defaultValue = null;
	}
	
	/**
	* 생성자
	* @param sColumnName	색인대상 컬럼명
	* @param sTargetColumnName	색인파일에 저장될 컬럼명
	* @param iFileInfoType	파일정보타입
	* @param iSystemInfoType	시스템정보타입
	*/	
	public SourceColumn( String sColumnName, 
												String sTargetColumnName, 
												int iFileInfoType, 
												int iSystemInfoType ) 
	{
		ms_columnName = sColumnName;
		ms_targetColumnName = sTargetColumnName;
		mi_fileInfoType = iFileInfoType;
		mi_systemInfoType = iSystemInfoType;		
		mb_notNull = false;
		ms_defaultValue = null;		
	}
	
	
	/**
	* 컬럼명을 알아낸다. <p>
	* @return	컬럼명
	*/		
	public String getColumnName ()
	{
		return ms_columnName;
	}
	
	/**
	* 컬럼 Type을 알아낸다. <p>
	* @return	컬럼 Type
	*/		
	public int getColumnType ()
	{
		return mi_columnType;
	}	
	
	/**
	* 파일InfoType을 알아낸다. <p>
	* @return	파일InfoType
	*/
	public int getFileInfoType ()
	{
		return mi_fileInfoType;
	}		
	
	
	/**
	* 저장할 색인테이블 컬럼명을 알아낸다. <p>
	* @return	컬럼명
	*/		
	public String getTargetColumnName ()
	{
		return ms_targetColumnName;
	}				
	
	/**
	* 컬럼 Type을 설정한다. <p>
	* @param	iColumnType	컬럼 Type
	*/		
	public void setColumnType ( int iColumnType )
	{
		mi_columnType = iColumnType;
	}
	
	
	/**
	* 컬럼의 default값을 설정한다. 
	* default값이 설정되면 데이터 조회시 값이 null일때 default값으로 대체시킨다.
	* <p>
	* @param	sDefaultValue 	컬럼 default값
	*/		
	public void setDefaultValue ( String sDefaultValue )
	{
		ms_defaultValue = sDefaultValue;
	}
	
	/**
	* 컬럼의 default값을 알아낸다. <p>
	* @return	컬럼 default값
	*/	
	public String getDefaultValue ()
	{
		return ms_defaultValue;
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
	* 시스템정보 타입을 설정한다.
	* 시스템정보가 설정되면 데이터소스의 컬럼이라고 인식하지 않는다.
	* 조회된 데이터라고 생각하지 않는다.
	* <p>
	* @param	iSystemInfoType 	시스템정보의 종류
	*/		
	public void setSystemInfoType ( int iSystemInfoType )
	{
		mi_systemInfoType = iSystemInfoType;
	}
	
	/**
	* 컬럼이 notnull이 설정된 컬럼인지 알아낸다. <p>
	* @return	true : notnull컬럼 / false : nullable 컬럼
	*/	
	public int getSystemInfoType ()
	{
		return mi_systemInfoType;
	}
	
	
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer("ClassName:SourceColumn {\r\n");
		
		osb.append( "ms_columnName=" );
		osb.append( ms_columnName + "\r\n" );
		
		osb.append( "mi_columnType=" );
		osb.append( mi_columnType + "\r\n" );
		
		osb.append( "ms_targetColumnName=" );
		osb.append( ms_targetColumnName + "\r\n" );
		
		osb.append( "mb_notNull=" );
		osb.append( mb_notNull + "\r\n" );
		
		osb.append( "ms_defaultValue=" );
		osb.append( ms_defaultValue + "\r\n" );		
		
		osb.append( "mi_fileInfoType=" );
		osb.append( mi_fileInfoType + "\r\n" );	
		
		osb.append( "mi_systemInfoType=" );
		osb.append( mi_systemInfoType + " }\r\n" );				
				
		
		return 	osb.toString();	 
	}	
}

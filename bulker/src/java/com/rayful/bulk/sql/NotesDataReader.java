/**
 *******************************************************************
 * 파일명 : NotesDataReader.java
 * 파일설명 : 색인대상(Notes)으로 부터 색인할 색인데이터를 추출하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/23   주현필    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.sql;

// FileAppender 중복될수 있음
import java.io.File;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import lotus.domino.Database;
import lotus.domino.DateTime;
import lotus.domino.Document;
import lotus.domino.DocumentCollection;
import lotus.domino.Item;
import lotus.domino.MIMEEntity;
import lotus.domino.NotesException;
import lotus.domino.RichTextItem;

import org.apache.log4j.Logger;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.SourceColumn;
import com.rayful.bulk.index.TargetColumn;
import com.rayful.bulk.index.indexer.NotesIndexer;
import com.rayful.bulk.io.Attachment;
import com.rayful.bulk.io.Attachments;
import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.localize.Localization;

/**
 * 색인대상으로 부터 색인할 색인데이터를 추출하는 클래스
 */
public class NotesDataReader
{
	//static Logger logger = YessLogger.getLogger( NotesDataReader.class.getName(), Config.LOG_PATH );
	static Logger logger = null;
	
	/**
	 * Connector 객체
	 * @uml.property  name="mo_connector"
	 * @uml.associationEnd  
	 */
	NotesConnector mo_connector;	
	/** Connection 객체 */
	Database mo_database;	 		
	/**
	 * 컬럼매칭정보
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	ColumnMatching mo_columnMatching;
	
	/** Data Query SQL */
	String ms_dataSql;
	
	/** DataSource Id */
	String ms_dataSourceId;
	/** 특정로우를 가리키는 PK */
	String ms_keys;
	/** 문서 최종색인일	*/
	String ms_lastDocIndexDate;
	/** 조회수 최종색인일	*/
	String ms_lastDoccountIndexdate;
	
	/** 조회수 컬럼명 */
	String ms_doccountColumnName;		
	/** 조회수변경일 컬럼명	*/
	String ms_doccountModifiedTimeColumnName;
	
	/** 
	* data Query Key를 저장하는 배열 
	* NotesIndexer.initDataReader()에서 참조한다.
	*/
	public String [] dataKeys;
	/** 
	* file Query Key를 저장하는 배열 
	* NotesIndexer.initDataReader()에서 참조한다.
	*/	
	public String [] fileKeys;
	
	
	// 현재사용안하는 상수들.
	//int LEN_ABSTRACT = 500;
  //int MAX_ORIGINAL_FILE_SIZE = 10000000;
  //int MAX_ERROR_FILE_SIZE = 9000000; 
	
	
	/** 
	* 생성자
	* <p>
	* @param	sDataQuery 색인데이터 쿼리 SQL문
	* @param	sLastDocIndexDate 문서 최종색인일	
	* @param	sLastDoccountIndexDate 조회수 최종색인일
	* @param	sDocCountColumnName 조회수 컬럼명
	* @param	sModifiedTimeColumnName 조회수변경일 컬럼명
	*/
	public NotesDataReader( String sDataQuery,
							String sLastDocIndexDate,
							String sLastDoccountIndexDate,
							String sDocCountColumnName,
							String sModifiedTimeColumnName )
	{
		ms_dataSql = sDataQuery;
		
		mo_database = null;
		mo_connector = null;
		mo_columnMatching = null;
		ms_dataSourceId = null;
		ms_keys = null;
		ms_lastDocIndexDate = sLastDocIndexDate;
		ms_lastDoccountIndexdate = sLastDoccountIndexDate;
		
		ms_doccountColumnName = sDocCountColumnName;
		ms_doccountModifiedTimeColumnName = sModifiedTimeColumnName;
		
		//opensky
		dataKeys = "PK".split ( "\\|" );
	}
	
	public void setLogger(String sLogPath)
	{
		//logger = YessLogger.getLogger( NotesDataReader.class.getName(), Config.LOG_PATH );
		logger = RayfulLogger.getLogger( NotesDataReader.class.getName(), sLogPath );
	}
	
	/** 
	* 색인대상쿼리 SQL문을 알아낸다.
	* <p>
	* @return	색인대상쿼리 SQL문
	**/
	public String getDataQuerySql () 
	{
		return ms_dataSql;
	}

		
	/** 
	* 커넥션객체를 설정한다.
	* <p>
	* @param	oDataBase 노츠데이터베이스객체
	* @param	oConnector 노츠커넥터객체
	* @param	sDataSourceId 데이터소스ID
	**/
	public void setConnection( Database oDataBase, 
															NotesConnector oConnector,
															String sDataSourceId ) 
	throws Exception 
	{
		if ( oDataBase == null ) {
			String sMsg = Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0022");
			// Exception을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
			logger.error ( sMsg );
			new Exception ( sMsg );
		}
		
		mo_database = oDataBase;
		mo_connector = oConnector;
		ms_dataSourceId = sDataSourceId;
		
	}
	
	/** 
	* 색인대상컬럼정보들을 설정한다.
	* 내부적으로 색인대상컬럼정보가 설정되면서 변경된 건만을 조회하기 위한 조건문이 
	* 키쿼리에 추가된다. ( 색인모드가 INDEX_MODIFIED일 경우 )
	* <p>
	* @param	oColumnMatching 컬럼매칭객체
	**/		
	public void setColumnMatching ( ColumnMatching oColumnMatching )
	{
		if ( oColumnMatching == null ) {
			//에러처리
			return;
		}
				
		mo_columnMatching = oColumnMatching;
	}//function
		
	/**
	* 특정 row만 조회하도록 PK값을 설정한다.
	* <p>
	* @param	sKeys	색인하려는 데이터의 키값
	*/		
	public void setKey ( String sKeys )
	{
		ms_keys = sKeys;
	}		

	/**
	* 시스템정보를 알아낸다.
	* @param	iSystemInfoType	시스템정보종류
	* @return	조건문 SQL, ms_dataSql이 null이면 ""스트링을 리턴.
	*/
	private String getSystemInfo( Document oDocument, int iSystemInfoType, String sTimeZone ) 
	throws NotesException
	{
		String sSystemValue = null;
		
		if ( iSystemInfoType == SourceColumn.SINFOYPE_HOSTNAME ) {
			sSystemValue = mo_connector.getHostName();
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_DATASURCE_ID ) {
			sSystemValue = ms_dataSourceId;
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_DBNAME ) {
			sSystemValue = mo_connector.getDbName();
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_NOTES_NSFNAME ) {
			sSystemValue = mo_connector.getNsfName();
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_NOTES_UNVERSALID ) {
			sSystemValue = oDocument.getUniversalID();
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_NOTES_CREATED) {
			sSystemValue = ESPTypes.dateFormatString(oDocument.getCreated().toJavaDate(), "yyyy-MM-dd HH:mm:ss", sTimeZone);
		} else if ( iSystemInfoType == SourceColumn.SINFOYPE_NOTES_MODIFIED) {
			sSystemValue = ESPTypes.dateFormatString(oDocument.getLastModified().toJavaDate(), "yyyy-MM-dd HH:mm:ss", sTimeZone);
			
//			System.out.println("sSystemValue : " + sSystemValue);
//			System.out.println("getDateOnly : " + oDocument.getLastModified().getDateOnly());
//			System.out.println("getGMTTime : " + oDocument.getLastModified().getGMTTime());
//			System.out.println("getLocalTime : " + oDocument.getLastModified().getLocalTime());
//			System.out.println("getTimeOnly : " + oDocument.getLastModified().getTimeOnly());
//			System.out.println("getZoneTime : " + oDocument.getLastModified().getZoneTime());
//			System.out.println("TimeZone : " + oDocument.getLastModified().getTimeZone() );
		}
		
		return sSystemValue;
	}



	/**
	* DF에 정의된 DataQuery SQL문에서 조건부분만을 추출한다.
	* @return	조건문 SQL, ms_dataSql이 null이면 ""스트링을 리턴.
	*/
	private String getDFQuerySql() {
		String sDFQuerySql = "";
		if ( ms_dataSql != null ) {
			sDFQuerySql = ms_dataSql.replaceFirst( "SELECT", "" );
			sDFQuerySql = sDFQuerySql.replaceFirst( "@ALL", "" );
			sDFQuerySql = sDFQuerySql.trim();
		}
		
		return sDFQuerySql;
	}

	/**
	* 색인할 데이터를 쿼리하여 결과를 리턴
	* INDEX MODE가 INDEX_MODIFIED일 경우 생성일, 수정일을 최종색인일과 비교하는 
	* 조건문을 자동으로 추가한다.
	* @return	노츠문서컬렉션객체
	*/
	public DocumentCollection executeDataQuery()
	throws Exception
	{		
		DocumentCollection oDocumentCollection = null;
		String sQuerySql = null;
		String sNotesFormatDate = null;
		String sDFQuerySql = null;
	  		
  	try {
			if ( ms_dataSql == null ) {
				throw new Exception ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0012") );
			}

			if ( ms_keys == null ) {
				sQuerySql = ms_dataSql;
				
					// 수정된건만을 대상으로 하는 조건 추가 (날짜조건 추가)
			  if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED ) {
	
					// 색인컬럼 MODIFIEDTIME의 정보를 읽는다.
					TargetColumn oTaregetColumn = ( TargetColumn ) mo_columnMatching.target.get( "MODIFIEDTIME" );
					if ( oTaregetColumn == null ) {
						// opensky
						//new Exception ( "MODIFIEDTIME 컬럼정보를 찾을 수 없습니다.");
						sQuerySql = "SELECT @ALL";
					} else {
						String sSourceColumnNames = oTaregetColumn.getSourceColumnNames();
						StringTokenizer oSt = new StringTokenizer ( sSourceColumnNames, "||" );
						String sModifiedTimeColumnName = null;
						
						// 데이터소스의 수정시간컬럼을 알아낸다.
						if (  oSt.hasMoreElements() ) {
							sModifiedTimeColumnName = oSt.nextToken();
						}
						
						if ( sModifiedTimeColumnName == null || sModifiedTimeColumnName.length() == 0 ) {
							new Exception ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0013") );
						}
						
		
						if ( ms_lastDocIndexDate != null ) {
							StringBuffer oSb = new StringBuffer();	
							//sNotesFormatDate = ms_lastDocIndexDate.replaceAll ( "-", ";" ) + ";00;00;01";
							sNotesFormatDate = ESPTypes.dateFormatString( ms_lastDoccountIndexdate, "yyyy;MM;dd;HH;mm;ss");
							oSb.append( "SELECT " );	
								
							sDFQuerySql = this.getDFQuerySql();	
							if ( sDFQuerySql.length() > 0 ) {	
								oSb.append ( " ( " );	
								oSb.append ( sDFQuerySql );	
								oSb.append ( " ) " );	
								oSb.append ( " & " );	
							}
								
							oSb.append( " ( " );	
							oSb.append( sModifiedTimeColumnName );	
							oSb.append( " >= @Date( " + sNotesFormatDate + " ) " );	
							oSb.append( " ) " );	
								
							sQuerySql = oSb.toString();
						}
					}
				} // if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED )
			} else { 
				// ++ 특정Row만을 조회하는 쿼리 ++
				sQuerySql = "SELECT @Text(@DocumentUniqueID) = @Uppercase(\"" + ms_keys + "\") ";
			} // if ( ms_keys == null ) 
			
			
			logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0023") + sQuerySql );
			oDocumentCollection = mo_database.search(sQuerySql, null, 0);
			return oDocumentCollection;
			
  	} catch ( Exception e ) {
  		logger.error ( sQuerySql, e );
  		throw e; // Exception을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
  	} 
	}
	
	/**
	* 조회수가 수정된 문서만을 조회한다.
	* @return	노츠문서컬렉션객체
	*/
	public DocumentCollection executeDoccountQuery()
	throws Exception
	{		
		DocumentCollection oDocumentCollection = null;
		String sQuerySql = null;
		String sNotesFormatDate = null;
		String sDFQuerySql = null;
		
  	try {
  		
			if ( ms_dataSql == null ) {
				throw new Exception ( Localization.getMessage(NotesDataReader.class.getName() + ".Exception.0012") );
			}
			
			if ( ms_doccountColumnName == null ) {
				// SQLException을 사용하여 Exception과 구분
				throw new SQLException ( Localization.getMessage(NotesDataReader.class.getName() + ".Exception.0014") );
			}
			
			if ( ms_doccountModifiedTimeColumnName == null ) {
				// SQLException을 사용하여 Exception과 구분
				throw new SQLException ( Localization.getMessage(NotesDataReader.class.getName() + ".Exception.0015") );
			}
			
			if ( ms_lastDoccountIndexdate	== null || ms_lastDoccountIndexdate.length() == 0) {
				throw new Exception ( Localization.getMessage(NotesDataReader.class.getName() + ".Exception.0016") );
			}
			
			
			StringBuffer oSb = new StringBuffer();
			//sNotesFormatDate = ms_lastDoccountIndexdate.replaceAll ( "-", ";" ) + ";00;00;01";
			sNotesFormatDate = ESPTypes.dateFormatString( ms_lastDoccountIndexdate, "yyyyMMdd;HH;mm;ss");
			oSb.append( "SELECT " );
			
			sDFQuerySql = this.getDFQuerySql();
			if ( sDFQuerySql.length() > 0 ) {
				oSb.append ( " ( " );
				oSb.append ( sDFQuerySql );
				oSb.append ( " ) " );
				oSb.append ( " & " );
			}
			
			oSb.append( " ( " );
			oSb.append( ms_doccountModifiedTimeColumnName );
			oSb.append( " >= @Date( " + sNotesFormatDate + " ) " );
			oSb.append( " ) " );
			
			sQuerySql = oSb.toString();
			
			logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0024") + sQuerySql );
			oDocumentCollection = mo_database.search(sQuerySql, null, 0);
			return oDocumentCollection;
		} catch ( SQLException se ) {
			// 실제로 에러가 아님 : 로그를 기록하지 않음
			throw se;			
  	} catch ( Exception e ) {
  		logger.error ( sQuerySql, e );
  		throw e; // Exception을 던져서 호출한 부분에서 에러를 감지할 수 있도록 한다.
  	} 
	}
	
	/**
	* 조회수변경된 건을 색인하기 위한 데이터를 읽어 결과를 리턴
	* @param	oDocument	노츠문서객체
	* @param	oReaderMap	조회된 데이터 Map
	* @return	성공여부 ( true: 조회성공/  false: 조회실패)
	*/	
	public boolean getDoccountData ( Document oDocument, Map<String, String> oReaderMap ) 
	{
	
		String sNotesId = null;
		Item oItem = null;
		boolean bReturn = true;
		String sPK = null;
		String sDoccount = null;		

		try {
			// 외부파일명을 구성 : Notes의 UniversalID
			sNotesId = oDocument.getUniversalID();
			logger.info( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0025") + sNotesId );
			
			if ( ( sNotesId.equals(" ") ) ||
					 ( oDocument.hasItem("$Conflict") ) )
			{		
				logger.warn (Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0026"));
				throw new SQLException ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0006") );
			}
			
			if ( ms_dataSourceId == null ) {
				String sMsg = Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0027");
				logger.error (sMsg);
				throw new SQLException (sMsg);
			}
			
			//OUTPUT변수
			sPK = ms_dataSourceId + "_" + sNotesId;
			
			if ( oDocument.hasItem( ms_doccountColumnName ) ) {
				oItem = oDocument.getFirstItem( ms_doccountColumnName );
				//OUTPUT변수
				sDoccount = oItem.getText();
			}	else {
				throw new SQLException ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0017", ms_doccountColumnName ) );
			}
			
			if ( sDoccount == null ) {
				sDoccount = "0";
			}
			
			oReaderMap.put ( "PK", sPK );
			oReaderMap.put ( "DOCCOUNT", sDoccount );
			
		} catch ( Exception e ) {
			logger.error ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0007"), e );
			bReturn = false;
		} 
		
		return bReturn;
	}
	
	/**
	* 색인할 데이터를 쿼리하여 결과를 리턴
	* @param	oDocument	노츠문서객체
	* @param	oFileAppender 본문 FileAppender객체
	* @param	oAttachments 첨부파일목록 객체
	* @return	조회결과 Map
	*/
	@SuppressWarnings("unchecked")
	public Map<String, Object> getDocIndexData ( Document oDocument, FileAppender oFileAppender, Attachments oAttachments, String sDataSourceID, String sTimeZone )
	throws Exception 
	{
		Map<String, Object> oReaderMap = new LinkedHashMap<String, Object>();

	  	// 조회한 값을 읽어서 저장.
	  	String sReadValue = null;
	  	String sAttachName = null;
	  	String sExtensionName = null;
	  	int iColumnType = 0;
	  	int iSystemInfoType = 0;
	  	SourceColumn oSourceColumn = null;
	  	String sSourceColumnName = null;
	  	Item oItem = null;
	
	  	try {
		  	
		  	if ( oDocument.hasItem( "$conflict" ) ) {
		  		throw new Exception ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0018" ) );	
		  	}
	  	
			Object [] aKey = mo_columnMatching.source.keySet().toArray();
			for ( int i=0; i< aKey.length; i++ ) {
				sReadValue = null;
				sAttachName = null;
				sExtensionName = null;
				sSourceColumnName = aKey[i].toString();
				oSourceColumn = ( SourceColumn ) mo_columnMatching.source.get( sSourceColumnName );
				
				//System.out.println("Column Name : " + oSourceColumn.getColumnName() + " Type : " + oSourceColumn.getColumnType());
				iSystemInfoType = oSourceColumn.getSystemInfoType();
				//oDocument.getLastModified()
				
				if ( oDocument.hasItem( sSourceColumnName ) ) {
					
					oItem = oDocument.getFirstItem( sSourceColumnName );
					iColumnType = oItem.getType();
					
					if ( iColumnType == Item.DATETIMES ) {
					//날짜TYPE
						DateTime oDateTime = oItem.getDateTimeValue();
						if ( oDateTime != null ) {
							java.util.Date oDate = oDateTime.toJavaDate();
							sReadValue = ESPTypes.dateFormatString ( oDate );
						} else {
						sReadValue = null;
						}
					} else if  ( iColumnType == Item.TEXT ||
												iColumnType == Item.AUTHORS ||
												iColumnType == Item.NAMES ||
												iColumnType == Item.READERS ||
												iColumnType == 1282) {
						// TEXT TYPE
						sReadValue = oItem.getText();
					} else if  ( iColumnType == Item.NUMBERS ) {
						sReadValue = oItem.getText();						
					} else if  ( iColumnType == Item.RICHTEXT ) { 
					// RICHTEXT TYPE
						RichTextItem oRTItem = (RichTextItem)oItem;
						sReadValue = oRTItem.getFormattedText(false,0,0);
					} else if ( iColumnType == Item.MIME_PART ) {
						
						MIMEEntity mime = oDocument.getMIMEEntity();
						
						if ( mime != null ) {
							sReadValue = mime.getContentAsText();
							
							MIMEEntity child = mime.getFirstChildEntity();
							
							while (child != null) {
					        	mime = child;
					        	child = mime.getFirstChildEntity();
							}
					        
							if (mime.getContentType().equals("text") &&
					          mime.getContentSubType().equals("html")) {
								
								// 1727 : MIMEEntity.ENC_BASE64
								if (mime.getEncoding() == MIMEEntity.ENC_BASE64) 
									mime.decodeContent();
								
								sReadValue = mime.getContentAsText();
									
					            //System.out.println("mime : " + mime.getContentAsText() );
							}
							//else 
							//	System.out.println("not mime");
							
						}
						
						//System.out.println("Mime Value : " + sReadValue);
						
					} else if  ( iColumnType == Item.ATTACHMENT ) {
					// ATTACHMENT TYPE ($FILE이기를 빌며...)
						Vector oValues = null;
						String sOrgFileName = null;
						String sCnvFileName = null;
						String sExtension = null;
						int iFileExtType  = 0;
						File oDownloadFile = null;
						String sUniversalID = null;
						String sSaveFileName = null;
						
						sReadValue = "";
						sAttachName = "";
						sExtensionName = "";
						
						oValues = mo_connector.getSession().evaluate( "@AttachmentNames", oDocument );
						
						if ( oValues != null ) {
							for ( int j=0; j < oValues.size(); j++ ) {
								sOrgFileName = oValues.elementAt(j).toString();
								
								if ( sOrgFileName == null ) {
									continue;
								}
								
								sUniversalID = oDocument.getUniversalID();
								
								sSaveFileName = sDataSourceID + "_" + sUniversalID + "_" + (new Integer( j )).toString();
								
								Attachment oAttachment = new Attachment( sSaveFileName, sOrgFileName, null );
								oAttachments.addAttachment( oAttachment );
								sCnvFileName = 	oAttachment.getTargetFileName();
								
								sExtension = FileAppender.getFileExtentionName( sOrgFileName );
								iFileExtType = FileAppender.getFileExtentionType( sOrgFileName );
								if (iFileExtType != FileAppender.UNKNOWN  ) {
									if ( oDocument.getAttachment( sOrgFileName ) == null ) {
										logger.warn( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0028") );
										continue;
									}
						      
									oDownloadFile = new File ( Config.DOWNLOAD_PATH, sCnvFileName );
									logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0029") + oDownloadFile.getPath() );
									
									// NOTES API extractFile() 이용
									oDocument.getAttachment( sOrgFileName ).extractFile( oDownloadFile.getPath() );
						      
						     
									// InputStream이용
									/*
									FileAppender oAttFileAppender = new FileAppender ( oDownloadFile.getPath() );
									EmbeddedObject oEObj = oDocument.getAttachment( sOrgFileName );
									logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0030") + oEObj.getFileSize() );
									InputStream oInputStream = oEObj.getInputStream();
									try { 
										oAttFileAppender.append ( oInputStream );
										oInputStream.close();
									} catch ( FileNotFoundException fnfe ) {
										logger.error ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0008"), fnfe);
									} catch ( IOException ie ) {
										logger.error ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0009"), ie);
									}
									*/
									logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0031") );
						      
									// 저장됨을 표시
									oAttachment.setSaved( true );
									
									if ( sReadValue.length() < 1 ) {
										sReadValue = oDownloadFile.getPath();
									} else {
										sReadValue = sReadValue + ";" + oDownloadFile.getPath();
									}
									
									if ( sAttachName.length() < 1 ) {
										sAttachName = sOrgFileName;
									} else {
										sAttachName = sAttachName + ";" + sOrgFileName;
									}
									
									if ( sExtensionName.length() < 1 ) {
										sExtensionName = sExtension;
									} else {
										sExtensionName = sExtensionName + ";" + sExtension;
									}
									
				      
								}	else {
									logger.warn ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0032", sExtension) );
								}
							}						
						}					
						//continue;					
					} else {
						//throw new Exception ( "[" + sSourceColumnName  + " TYPE] = " + iColumnType + ", 지원할수 없는 컬럼타입입니다." );
						throw new Exception ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0010", sSourceColumnName, iColumnType) );
					}
		
				}	else {
					
					// ================================================================
					//	시스템변수인지 체크
					// ================================================================						
						sReadValue = this.getSystemInfo( oDocument, iSystemInfoType, sTimeZone );
				}
				
				if ( sReadValue != null ) {
					if (sReadValue.length() == 0 ) {
						sReadValue = null;
					}
				}
				
				// 조회한 값이 NULL일경우 처리
				if ( sReadValue == null ) {
					// NOT NULL 처리
					if ( oSourceColumn.isNotNull() ) {
						throw new Exception ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0002", sSourceColumnName ) );
					}
					// Default(기본) 값처리
					sReadValue = oSourceColumn.getDefaultValue();
				}
				
				
				// 조회결과맵에 추가한다.
				if ( "$FILE".equalsIgnoreCase(sSourceColumnName) ) {
					oReaderMap.put ( sSourceColumnName, sReadValue );
					oReaderMap.put ( "ATTACHNAME", sAttachName );
					oReaderMap.put ( "ATTACHEXTENSION", sExtensionName );
				} else if ( "ATTACHNAME".equalsIgnoreCase(sSourceColumnName) || "ATTACHEXTENSION".equalsIgnoreCase(sSourceColumnName) ) {
					// 아무 작업도 하지 않는다.
					// 위의 로직에서 값을 설정하므로 설정하면 안 된다.
				} else {
					oReaderMap.put ( sSourceColumnName, sReadValue );
				}
				
				if ( logger.isDebugEnabled() ) {
					logger.debug ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0033", sSourceColumnName, sReadValue) );
				}
			}	// for
	  	} catch( NotesException ne ) {
	  		logger.error ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0011"), ne );
	  		logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0034", sSourceColumnName, sReadValue, iColumnType ) );
	  		throw ne;
	  	} catch( Exception e ) {
	  		logger.error ( Localization.getMessage( NotesDataReader.class.getName() + ".Exception.0011"), e );
	  		logger.info ( Localization.getMessage( NotesDataReader.class.getName() + ".Logger.0034", sSourceColumnName, sReadValue, iColumnType ) );  		
	      throw e;
	    }
  	
		return oReaderMap;
	}

	
	/** 잡고있는 자원을 해제한다. */
	public void clear () {

	}

	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/		
	public String toString()
	{
		StringBuffer osb = new StringBuffer();
		osb.append( "ClassName: NotesDataReser \n");
		
		osb.append( "ms_dataSql=" );
		osb.append( ms_dataSql + "\n" );		
		
		osb.append( "mo_database=" );
		if ( mo_database != null ) {
			osb.append( "not null \n" );
		} else {
			osb.append( "null \n" );
		}
		
		osb.append( "mo_connector=" );
		if ( mo_connector != null ) {
			osb.append( "not null \n" );
		} else {
			osb.append( "null \n" );
		}
		
		osb.append( "mo_columnMatching=" );
		if ( mo_columnMatching != null ) {
			osb.append( "not null \n" );
		} else {
			osb.append( "null \n" );
		}			
		
		osb.append( "ms_dataSourceId=" );
		osb.append( ms_dataSourceId + "\n" );
		
		osb.append( "ms_doccountColumnName=" );
		osb.append( ms_doccountColumnName + "\n" );
		
		osb.append( "ms_doccountModifiedTimeColumnName=" );
		osb.append( ms_doccountModifiedTimeColumnName + "\n" );
		
		return 	osb.toString();	 
	}
}

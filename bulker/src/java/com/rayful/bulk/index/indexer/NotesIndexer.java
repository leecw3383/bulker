/**
 *******************************************************************
 * 파일명 : NotesIndexer.java
 * 파일설명 : 데이터 소스가 Notes인 경우 색인메인로직을 처리하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/23   주현필	최초작성  
 * 2005/08/09   정충열	ABSTRACT추출시 FileReader를 사용하도록 변경  
 * 2006/04/06	정충열	isUseMQ() 함수 추가
 * 2010/12/10	주현필	EtcReader 함수 호출하는 부분 수정
 *******************************************************************
*/

package com.rayful.bulk.index.indexer;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import lotus.domino.Database;
import lotus.domino.Document;
import lotus.domino.DocumentCollection;
import lotus.domino.NotesException;

import org.apache.log4j.Logger;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.DFLoaderException;
import com.rayful.bulk.index.DataConvertor;
import com.rayful.bulk.index.customizer.ConvertCustomizer;
import com.rayful.bulk.index.fastsearch.XmlWriter;
import com.rayful.bulk.io.Attachments;
import com.rayful.bulk.io.ErrorFile;
import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.io.FilesAccessor;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.NotesConnector;
import com.rayful.bulk.sql.NotesDataReader;
import com.rayful.bulk.sql.NotesEtcReader;
import com.rayful.localize.Localization;

/**
 * 데이터소스가 NOTES인 경우의 색인메인로직을 처리
*/
public class NotesIndexer implements BaseIndexer
{
	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger(NotesIndexer.class.getName(), Config.LOG_PATH );
	
	/** 색인대상 Connection 객체 */
	protected Connection mo_sourceConnection;
	
	/** Notes 색인대상 커넥터객체 **/
	protected NotesConnector mo_sourceConnector;
	/**
	 * Notes 데이터조회 객체
	 * @uml.property  name="mo_dataReader"
	 * @uml.associationEnd  
	 */
	protected NotesDataReader mo_dataReader;
	/**
	 * Notes 추가데이터조회 객체
	 * @uml.property  name="mo_etcReader"
	 * @uml.associationEnd  
	 */
	protected NotesEtcReader mo_etcReader;
	
	/** 색인데이터 컨버터 객체 **/
	protected DataConvertor mo_dataConvertor;	
	/** 컬럼매칭정보 객체 **/
	protected ColumnMatching mo_columnMatching;
	/** 파일접근자 객체 **/
	protected FilesAccessor mo_filesAccessor;
	/** 커스터마이징객체  **/
	protected ConvertCustomizer mo_convertCustomizer;
	
	/** 노츠 DataBase객체  **/
	Database mo_sourceDataBase = null;	
	
	/**
	 * 에러파일 객체
	 * @uml.property  name="mo_errorFile"
	 * @uml.associationEnd  
	 */
	protected ErrorFile mo_errorFile;
	/**
	 * Warning 파일 객체
	 * @uml.property  name="mo_warnFile"
	 * @uml.associationEnd  
	 */
	protected ErrorFile mo_warnFile;
	
	/** 커넥션정보가 전역적으로 선언되었는지 여부  **/
	protected boolean mb_isDefinedGlobalConnect;
	
	/** 색인대상 전체건수 **/
	protected int mi_totalCnt;
	/** 현재 색인시도 건수 **/
	protected int mi_tryCnt;	
	/** 성공된된 건수 **/
	protected int mi_successCnt;
	/** 실패한 건수 **/
	protected int mi_failCnt;
	/** 경고대상  건수 **/
	protected int mi_warnCnt;
	
	/** Deleted된 건수 **/
	protected int mi_deleteCnt;
	/** 조회수 수정 건수 **/
	protected int mi_doccountUpdateCnt;	
	
	/** 최대색인건수 : 이 수 만큼만 색인함 **/
	protected int mi_maxIndexRows;
	
	/** 데이터소스 특정건의 조회 Key값 **/
	protected String ms_keys;
	
	/** DataQuery Parameter배열 */
	protected String [] ma_dataKeyParams;
	/** FileQuery Parameter배열 */
	protected String [] ma_fileKeyParams;
	/** 현재 색인중인 키값들을 담은 맵 */
	protected Map<String, String> mo_curKeyMap;

	// 현재색인하는 문서의 NotesID
	protected String ms_curNotesId;
	
	/** 데이터 소스ID **/
	protected String ms_dataSourceId;
	/** Customizer클래스명 **/
	protected String ms_convertCustomizerClassName;
	/** 본문내용 Html Tag 삭제로직 수행여부 **/
	protected boolean mb_contentDelTag;
	
	/** 추가 데이터 조회 클래스명 **/	
	protected String ms_etcReaderClassName;
	
	protected String ms_resultFileName = null;
	protected int mi_resultMaxRows;
	
	/** 색인실행시간  **/
	protected java.util.Date mo_ExecDate;
	
	/** 
	* 생성자
	*/	
	public NotesIndexer ()
	{
		mo_sourceConnection = null;
		
		mo_sourceConnector = null;
		mo_dataReader = null;
		mo_dataConvertor = null;
		mo_convertCustomizer = null;
		mo_sourceDataBase = null;
		
		mo_columnMatching = null;
		mo_filesAccessor = null;
		mb_isDefinedGlobalConnect = false;
		
		// 카운트 초기화
		mi_totalCnt = 0;
		mi_tryCnt = 0;
		mi_successCnt = 0;
		mi_failCnt = 0;
		mi_warnCnt = 0;
		mi_deleteCnt = 0;
		mi_doccountUpdateCnt =0;
		
		mi_maxIndexRows = -1;
		
		ms_curNotesId = null;			
		
		ms_dataSourceId = null;
		ms_convertCustomizerClassName = null;

		mb_contentDelTag = false;
		ms_keys = null;
		mo_ExecDate = new java.util.Date();
		
		ms_etcReaderClassName = null;
		
		ms_resultFileName = null;
		mi_resultMaxRows = -1;
	}
	
	/** 
	* MQ사용여부 리턴	
	*	LG전자 전용 2006/04/06 추가
	* <p>
	* @return		true:MQ사용/ fasle:MQ사용하지 않음
	*/
	public boolean isUseMQ ()
	{
		return false; //MQ사용하지 않음
	}

	
	/** 
	* DF파일의 정보를 로드하여필요한 색인관련 객체들을 생성한다 
	* <p>
	* @param	oDFLoader	DFLoader객체
	*/
	protected void loadDF( DFLoader oDFLoader ) 
	throws Exception 
	{
			
		// 데이터소스의 종류를 알아낸다.
		int iDataSourceType = oDFLoader.getDataSourceType();
		
		ms_resultFileName = oDFLoader.getTargetResultFileName();
		mi_resultMaxRows = oDFLoader.getTargetResultMaxRows();
		
		mb_isDefinedGlobalConnect = oDFLoader.isDefinedGlobalConnect();
		
		// 접속관련객체 생성
		mo_sourceConnector = ( NotesConnector )oDFLoader.getDataSourceConnector( iDataSourceType );
		
		// 조회관련객체 생성
		mo_dataReader = ( NotesDataReader )oDFLoader.getDataSourceDataReader( iDataSourceType );
		
		// 추가조회객체 클래스명을 알아낸다.
		ms_etcReaderClassName = oDFLoader.getDataSourceEtcReaderClassName();
		
		// 컬럼매칭정보객체 생성
		mo_columnMatching = oDFLoader.getDataSourceColumnMatching();
		
		// 파일접근정보객체 생성
		mo_filesAccessor = oDFLoader.getDataSourceFilesAccessor();
		
		if ( logger.isDebugEnabled() ) {
			logger.debug( mo_filesAccessor );
		}
		
		// 컨버트 커스터마지저 클래스명을 알아낸다.
		ms_convertCustomizerClassName = oDFLoader.getDataSourceConvertCustomizerClassName();
			
		// 데이터소스 최대 색인건수 ( mi_maxIndexRows > 0 보다 크면 색인은 mi_maxIndexRows건수만 수행한다. )
		if ( mi_maxIndexRows < 0 ) {
			mi_maxIndexRows = oDFLoader.getDataSourceMaxRows();
		}
		if ( mi_maxIndexRows > 0 ) {
			logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0052", mi_maxIndexRows ) );
		}	else if ( mi_maxIndexRows == 0 ) {
			throw new Exception ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0053" ) );
		}
		
		// 데이터소스 ID를 알아낸다.
		ms_dataSourceId = oDFLoader.getDataSourceId();
		
		// 에러파일 객체를 생성한다.
		if ( Config.ERROR_FILE_PATH.length() > 0 ) {
			mo_errorFile = new ErrorFile(
					Config.ERROR_FILE_PATH, 
					ms_dataSourceId, 
					mo_dataReader.dataKeys, 
					mo_dataReader.fileKeys );
		}
		
		// 에러파일 객체를 생성한다.
		if ( Config.WARN_FILE_PATH.length() > 0 ) {
			mo_warnFile = new ErrorFile(
					Config.WARN_FILE_PATH, 
					ms_dataSourceId, 
					mo_dataReader.dataKeys, 
					mo_dataReader.fileKeys );
		}
		
	}	
	
	/** 
	 * 색인대상에 접속한다.
	 * @throws Exception
	 */
	protected void connectSourceConnection() 
	throws Exception
	{
		try {
			//=============================================================================
			// 원본(색인대상) 스토리지에 접속
			//=============================================================================
			// Session을 연결한다.
			mo_sourceConnector.setSessionConnection();
			// Database을 연결한다.
			mo_sourceDataBase = mo_sourceConnector.getDbConnection();
		} catch ( Exception e ) {
			logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0003" ), e);
			throw e;
			//에러가 발생하면 해당 데이터소스의 색인을 수행하지 않는다.				
		}
	}
		
	/** 
	* 데이터 소스와 연결을 끊는다.<p>
	* @param	oDFLoader	DFLoader객체
	*/		
	protected void closeSourceConnection () 
	{
			// 데이터소스 Connection정보가 전역적으로 선언되었다면... 커넥션을 해제하지 않는다.
			if ( mb_isDefinedGlobalConnect ) {
				return;
			}
			//=============================================================================
			// 데이터소스의 Connection 해제
			//=============================================================================
			try{
				if ( mo_sourceConnection != null ) {
					mo_sourceDataBase.recycle();
					
					// NotesSesiionPool이 메로리에서 내려갈때 커넥션을 닫도록함...
					//mo_sourceConnector.closeSessionConnection();
				}
			} catch ( NotesException ne ) {
				logger.warn( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0004" ) , ne);
			}
	}
	
	/** 
	* ColumnMatching객체의 Type정보를 설정한다.
	* <p>
	* @param	oSSWriter	SSWriter객체 
	*/		
	protected void initColumnMatching ()
	throws SQLException
	{
//			String sDataQuerySql = null;
			
			//=============================================================================
			// 컬럼정보 로드...
			//=============================================================================
			// 색인대상컬럼정보를 로드하여 세팅한다.
			
//			sDataQuerySql = mo_dataReader.getDataQuerySql();
//			logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0005" ) );
//			mo_columnMatching.loadSourceColumnInfo( mo_sourceConnection, sDataQuerySql, mo_dataReader.dataKeys.length );
			
			if ( logger.isDebugEnabled() ) {
				logger.debug ( mo_columnMatching );
			}
			// opensky
			// 색인테이블컬럼정보를 로드하여 세팅한다.
			//mo_columnMatching.loadTargetColumnInfo( mo_sourceConnection );		
			
	}
	
	
	/** 
	* DataReader객체를 설정한다.
	* 내부적으로 데이터베이스정보 및 컬럼정보를 설정한다.
	*/		
	protected void initDataReader()
	throws Exception
	{
			//=============================================================================
			// DataReader 객체 생성및 설정
			//=============================================================================
			// DataReader를 세팅한다.
			mo_dataReader.setConnection( mo_sourceDataBase, mo_sourceConnector, ms_dataSourceId );
			mo_dataReader.setColumnMatching( mo_columnMatching );	
			mo_dataReader.setKey( ms_keys );	 // PK값을 설정 : ms_keys가 null이 아니라면 특정로우만 조회될것임
			mo_dataReader.setLogger(Config.LOG_PATH);
	}
	
	/** 
	* EtcReader객체를 세팅한다.
	* ( mo_etcReader )
	*/		
	@SuppressWarnings("unchecked")
	protected void initEtcReader()
	throws Exception
	{
		if ( ms_etcReaderClassName == null ) {
			return ;
		}
		
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0006", ms_etcReaderClassName ) );		
		try {
			// 클래스 생성
			Class etcReaderClass = Class.forName( ms_etcReaderClassName );
			
			// Consturctor Parameter Value 값을 설정
			Object [] aConParamValues = new Object[1];
			Class[] aConParamTypes = new Class[1];

			// 아래 두 줄 변경됨 2010-12-10
			aConParamValues[0] = mo_sourceConnector;
			aConParamTypes[0] = Class.forName("com.rayful.bulk.sql.NotesConnector");
			
			// Constructor를 알아낸다.
			java.lang.reflect.Constructor oConstructor = etcReaderClass.getConstructor(aConParamTypes);
			// 알아낸 Constructor객체를 이용하여 동적객체를 생성
			mo_etcReader = (NotesEtcReader)oConstructor.newInstance(aConParamValues);

		} catch ( Exception  e ) {
			logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0007", ms_etcReaderClassName ), e );
			throw e;
		}
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0008", ms_etcReaderClassName ) );

	}
	
	/** 
	* DataConvertor 객체를  로드한다.
	*/		
	protected void initDataConvertor()
	throws Exception
	{
			mo_dataConvertor = new DataConvertor(ms_dataSourceId, mo_columnMatching);
	}


	/** 
	* ConvertCustomizer 객체를 동적으로 로드한다.
	*/
	protected void initConvertCustomizer()
	throws Exception
	{
			logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0054" ) );
			if ( ms_convertCustomizerClassName == null ) {
				logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0055" ) );
				return;
			}
		
			try {
				mo_convertCustomizer = ( ConvertCustomizer )Class.forName( ms_convertCustomizerClassName ).newInstance();
			}  catch ( ClassNotFoundException cnfe ) {
				logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0005" ), cnfe );
				//에러가 발생하면 해당 데이터소스의 색인을 수행하지 않는다.
				throw cnfe;
			} catch ( InstantiationException ie ) {
				logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0005" ), ie );
				//에러가 발생하면 해당 데이터소스의 색인을 수행하지 않는다.
				throw ie;
			} catch ( IllegalAccessException iae ) {
				logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0005" ), iae );
				//에러가 발생하면 해당 데이터소스의 색인을 수행하지 않는다.
				throw iae;
			}
			logger.info ( ">>>ConvertCustomizer 클래스로드 완료" );
	}	
	
	/** 
	* KeyQuery 결과로 부터 DataQuery의 Parameter값을 알아낸다.
	* Data Query의 키들을 이용하여  색인외부파일의  Appender객체를 생성하여 리턴한다.
	* <p>
	* @return	DataQuery의 키값으로 구성된 색인외부파일 Appender객체 
	* @throws SQLException
	*/
		
	/** 
	* KeyQuery 결과로 부터 DataQuery의 Parameter값을 알아낸다.
	* <p>
	* @param	oRs	KeyQuery결과
	* @return	DataQuery의 키값으로 구성된 색인외부파일 Appender객체 
	*/			
	protected FileAppender initDataQueryParameter( Document oDoc )
	{
		
		// [색인외부파일명] = Notes의 UniversalID
		StringBuffer oSb = new StringBuffer();
		
		File oExternalPath = null;
		File oExternalFile = null;
		FileAppender oExternalFileAppender = null;
		
		try {
			// 외부파일경로를 구성
//			oSb.append( File.separatorChar );
//			oSb.append( Config.ESP_COLLECTION_NAME );	//색인테이블명
//			oSb.append( File.separatorChar );
//			oSb.append( ms_dataSourceId );		//데이터소스명
//			oSb.append( File.separatorChar );		
//			oExternalPath = new File ( Config.DOWNLOAD_PATH, oSb.toString());
			oExternalPath = new File ( Config.DOWNLOAD_PATH );
	
			if ( ! oExternalPath.exists()  ) {
				if ( oExternalPath.mkdirs() == false ) {
					logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0056", oExternalPath.getPath() ) );
					return null;
				}
			}			

			// 외부파일명을 구성 : Notes의 UniversalID
			oSb = new StringBuffer();
			ms_curNotesId = oDoc.getUniversalID();
			logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0057", ms_curNotesId ) );
			oSb.append ( ms_curNotesId );
			
			if ( ( ms_curNotesId.equals(" ") ) ||
					 ( oDoc.hasItem("$Conflict") ) )
			{		
				logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0058" ) );
				return null;
			}
		} catch ( NotesException ne ) {
			logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0006" ) , ne );
			return null;	
		}
		oSb.append ( ".txt" );	// 확장자는 txt

		// 외부파일경로 + 외부파일명 
		oExternalFile = new File ( oExternalPath.getPath(), oSb.toString() );


		// 색인외부파일이 이미 존재하면 삭제
		if ( oExternalFile.exists() ) {
			if ( ! oExternalFile.delete() ) {
				logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0058" , oExternalFile.getPath() ) );
				return null;
			}
		}
		
		// 외부파일 Appender객체 생성
		oExternalFileAppender = new FileAppender ( oExternalFile.getPath() );
		
//		// -------------------------------------------------------------
//		// 본문의 HTML TAG를 제거해야 한다면 
//		// text-reader가 HTML로 인식할 수 있는 Meta태그를 먼저 추가한다.
//		// -------------------------------------------------------------
//		if ( mb_contentDelTag == true ) {
//			try { 
//				oExternalFileAppender.append( "<HTML><HEAD><META HTTP-EQUIV='Content-Type' CONTENT='text/html; charset=ks_c_5601-1987'></HEAD></HTML>" );
//			} catch ( FileNotFoundException fnfe ) {
//				logger.warn( "외부파일이 없어서 메타태그를 추가하지 못했습니다", fnfe);
//				return null;
//			}
//		}
		
		return oExternalFileAppender ;
	}
	
	
	/** 
	* 색인데이터를 쿼리한다.
	* <p>
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @param	oAttachments	색인목록정보 객체
	* @return	쿼리결과Map
	*/		
	protected Map<String, Object> getDocIndexData( Document oDocument, FileAppender oExternalFileAppender,
									Attachments oAttachments, DFLoader oDFLoader )
	{
		try {
			
			String sTimeZone = oDFLoader.getTimeZone();
			
			String sDataSourceID = oDFLoader.getDataSourceId();
			
			logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0024" ) );
			return mo_dataReader.getDocIndexData ( oDocument, oExternalFileAppender, oAttachments, sDataSourceID, sTimeZone);

		} catch ( Exception e ) {
			// 에러가 발생하면 현재 Row를 색인하지 않는다.
			return null;
		}
		
	}
	
	
	/** 
	* 조회된 색인데이터를 색인하기위한 형태로 Convert한다.
	* <p>
	* @param	oDataReaderMap	조회된 색인데이터 Map
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @return	Converted 데이터 Map
	*/		
	protected Map<String, Object> convertIndexData(Map<String, Object> oDataReaderMap)
	{
		Map<String, Object> oResultMap = null;
		try {
			oResultMap =  mo_dataConvertor.convertData( oDataReaderMap );

		} catch (Exception e ) {
			//아무작업도 하지 않는다....
		}		
		
		return oResultMap;
	}
	
	/** 
	* 하드코딩이 필요한 경우 하드코딩 구현된 클래스를 실행한다.
	* <p>
	* @param	oDataReaderMap	조회된 색인데이터 Map
	* @param	oResultMap	Converted 데이터 Map	
	* @return	true: 성공 / false: 에러발생
	*/		
	protected boolean customize ( Map<String, Object> oDataReaderMap, Map<String, Object> oResultMap )
	{
		// customizer가 동작					
		try {
			if ( mo_convertCustomizer != null ) {
				mo_convertCustomizer.convert( ms_dataSourceId, oDataReaderMap, oResultMap, mo_columnMatching );
			}
		} catch ( Exception e ) {
			logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0031" ), e );
			return false;
		}
		return true;
	}
	
	/** 
	* 색인테이블에 반영시킨다.
	* <p>
	* @param	oResultMap	Converted 데이터 Map
	* @param	oSSWriter	색인Writer객체 
	* @return	SSWriter.INSERTED:insert/SSWriter.UPDATED:update/SSWriter.FAILED:에러발생
	*/		
	protected int addXmlData( XmlWriter oXmlWriter, Map<String, Object> oResultMap )
	{
		int iState;
		//=============================================================================
		// 색인테이블에 반영 
		// Insert / Update
		//=============================================================================
		
		if ( oXmlWriter.addDocument(oResultMap) ) {
			iState = XmlWriter.SUCCESSED;
		} else {
			iState =  XmlWriter.FAILED;
		}
	
		return iState;
	}
	
	/** 
	* 색인요약결과를 로그에 기록한다.
	*/		
	protected void writeResultLog()
	{
		// 색인수행결과의 요약정보 로그로 남김
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0046" ) );
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0047", mi_totalCnt ) );
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0048", mi_tryCnt ) );
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0049", mi_successCnt ) );
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0050", mi_failCnt ) );
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0051", mi_warnCnt ) );			
		logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0046" ) );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	*/
	protected void writeIndexLog( int iState )
	{
		writeIndexLog( iState, false );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	* @param bWarning 경고대상여부
	*/	
	protected void writeIndexLog( int iState, boolean bWarning )
	{
		String sMessage = null; 
		
		if ( iState == XmlWriter.SUCCESSED ) {
			mi_successCnt ++;
			sMessage = ". success ";
		} else {
			mi_failCnt ++;
			sMessage = ". failed ";
		}

		
		//에러로그파일에 기록
		if ( iState == XmlWriter.FAILED  ) {
			
			if ( mo_errorFile != null ) {
				try { 
					mo_errorFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0026" ), e);
				}
			}			
		} else if ( bWarning ) {
			mi_warnCnt ++;
			sMessage += "but Warnning!!! ";
			
			if ( mo_warnFile != null ) {
				try { 
					mo_warnFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0027" ), e);
				}
			}			
		}		
		
		if ( ma_dataKeyParams != null ) {
			StringBuffer oKeys = new StringBuffer ( " Keys:" );
			for ( int i=0; i < ma_dataKeyParams.length; i++ )
			{
				oKeys.append( ma_dataKeyParams [i] );
				if ( i < ma_dataKeyParams.length -1 ) {
					oKeys.append( "," );
				}
			}
			sMessage += oKeys.toString();
		}
		
		logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0028", mi_tryCnt, sMessage ) );
	}
	
	/** 
	* DF에 문서 최종색인일을 기록한다.
	* @param oDFLoader DFLoader객체
	* @throws DFLoaderException
	*/		
	protected void writeIndexDate( DFLoader oDFLoader )
	throws DFLoaderException
	{
		// 부분색인이 아닐때 마지막 문서 색인일을 현재일로 변경
		if ( ms_keys == null && mi_maxIndexRows < 0 ) {
			if ( Config.KEYQUERY_METHOD != Config.KEY_ERROR ) {
				String sLastDocIndexDate = null, sTimeZone = null;
				
				sTimeZone = oDFLoader.getTimeZone();
				
				if ( sTimeZone == null || sTimeZone.trim().length() < 1 ) {
					sLastDocIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss" );
				}
				else {
					sLastDocIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss", sTimeZone );
				}
				
				oDFLoader.setDataSourceLastDocIndexdate( sLastDocIndexDate );
			} else {
				logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0037" ) );
			}
		} else {
			logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0038" ) );
		}
	}
	
	/** 
	* DF에 조회수 최종색인일을 기록한다.
	* @param oDFLoader DFLoader객체
	* @throws DFLoaderException	
	*/		
	protected void writeDoccountIndexDate( DFLoader oDFLoader )
	throws DFLoaderException
	{
		// 부분색인이 아닐때 마지막 조회수 색인일을 현재일로 변경
		if ( ms_keys == null && mi_maxIndexRows < 0  ) {
			if ( Config.KEYQUERY_METHOD != Config.KEY_ERROR ) {
				String sLastDoccountIndexDate = null, sTimeZone = null;
				
				sTimeZone = oDFLoader.getTimeZone();
				
				if ( sTimeZone == null || sTimeZone.trim().length() < 1 ) {
					sLastDoccountIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss" );
				}
				else {
					sLastDoccountIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss", sTimeZone );
				}
				
				oDFLoader.setDataSourceLastDoccountIndexdate( sLastDoccountIndexDate );
			} else {
				logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0039" ) );
			}			
		} else {
			logger.info( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0040" ) );
		}
	}

	/** 
	* 추가 쿼리가 있는 경우 이 클래스를 상속받은후 이 함수를 재정의한다.
	* 쿼리 결과를 oDataMap에 반영한다.
	* 쿼리 결과를 oResultMap에 반영한다.
	* @param ResultMap 쿼리데이터
	* @param ResultMap 색인데이터 
	* @return true:성공시 /false :에러 발생시
	*/		
	protected boolean queryEtcData ( Map<String, Object> oDataMap, Map<String, Object> oResultMap ) {
		boolean bReturn = true;
		
		if ( mo_etcReader != null ) {
			bReturn = mo_etcReader.executeEtcQuery(oDataMap, oResultMap);
		}
		return bReturn;
	}
	
	
	/** 색인대상 전체건수 리턴 (필수구현함수)	*/
	public int getTotalCount()
	{
		return mi_totalCnt;
	}
	
	/** 색인 시도건수 리턴  (필수구현함수)	*/
	public int getTryCount()
	{
		return mi_tryCnt;
	}	
	
	/** Inserted된 건수 리턴	(필수구현함수) */
	public int getSuccessCount()
	{
		return mi_successCnt;
	}
	
	/** 실패한 건수 리턴	(필수구현함수) */	
	public int getFailCount()
	{
		return mi_failCnt;
	}
	
	/** 경고대상 건수 리턴	(필수구현함수) */	
	public int getWarnCount()
	{
		return mi_warnCnt;
	}		
	
	/**
	* 특정 row만 색인을 수행한다.
	* <p>
	* @param	sKeys	색인하려는 데이터의 키값
	*/		
	public void setKey ( String sKeys )
	{
		ms_keys = sKeys;
	}
	
	/**
	* 색인수를 제한한다. (필수구현함수)
	* <p>
	* @param	iMaxRows	제한된색인수
	*/											
	public void setMaxRows( int iMaxRows ){
		mi_maxIndexRows = iMaxRows;
	}
		
		
	/**
	* Notes 색인메인로직을 구현한다. (필수구현함수)
	*	@param	oDFLoader	DFLoader객체
	*	@param	oSSWriter	SSWriter객체
	*	@param	oMetaDbConnection	관리DB커넥션
	*
	*/
	public void run( DFLoader oDFLoader )
	throws Exception
	{
		DocumentCollection oDocCollection = null;
		Document oDocument = null;
		
		// 키 데이터를 전체를  담은 클래스
		int iState =0;
		boolean bWarned = false;
		File oResultFile = null;
		String sCurrentTime = null;
		XmlWriter oXmlWriter = null;
		
		try {
			//----------------------------------------------------------------------------------------
			// ### 에러로직 ###
			// 에러가 발생하면 색인프로그램을 종료
			// 여기서 호출되는 함수는 내부에서 Exception이 발생하면 그것을 
			// 외부로 전달한다.
			//----------------------------------------------------------------------------------------
			// DF Load
			loadDF( oDFLoader );

			// 색인대상DB에 접속
			connectSourceConnection();
			
			// 컬럼타입정보 설정
			initColumnMatching();
			
			// DataReader객체 설정
			initDataReader();
			
			// 추가 Reader 객체 설정
			initEtcReader();
			
			// DataConvertor객체 로드 및 설정
			initDataConvertor();
			
			// ConvertCustomizer 객체 로드 및 설정
			initConvertCustomizer();
			
			// XmlWriter 설정.
			oXmlWriter = new XmlWriter();
			oXmlWriter.setColumnMatching ( mo_columnMatching );
			
			// 색인대상 Key Query
			oDocCollection = mo_dataReader.executeDataQuery();
			// Key데이터의 로우수를 알아낸다.
			mi_totalCnt = oDocCollection.getCount();
			
			// 색인데이터 결과를 담는 객체
			Map<String, Object> oDataReaderMap = null;
			// 컨버팅 결과를 담는 객체
			Map<String, Object> oResultMap = null;

			logger.info ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0021", mi_totalCnt ) );
			for (int iNext = 1; iNext <= mi_totalCnt; iNext++ ) {
				
				ms_curNotesId = null;
				
				// mi_maxIndexRows값이 설정되었다면 중간에서 루프를 빠져나온다.
				if ( mi_maxIndexRows == mi_totalCnt ) {
					break;
				}
				//mi_totalCnt ++;		
				
				// 색인외부파일 정보를 담을 객체를 생성한다.
				Attachments oAttachments = new Attachments();
				// 색인외부파일객체
				FileAppender oExternalFileAppender = null;		
				
				oDocument = oDocCollection.getNthDocument(iNext);
				
				if (oDocument == null ) {
					logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0060", iNext ) );
					continue;
					//throw new Exception ( iNext + ". 노츠 Document객체가 null입니다." );
				}
				
				//----------------------------------------------------------------------------------------
				// ### 에러로직 ###
				// [치명]적인 에러가 발생하면 색인프로그램을 종료
				// [일반]적인 에러가 발생하면 해당 로우를 Skip...
				// 따라서 여기서 호출되는 함수는 내부에서 Exception처리를 하고 성공여부를 리턴
				// 또는 치명적인 경우는 Excetion을 throw 통해 전달
				//----------------------------------------------------------------------------------------
				mi_tryCnt ++;			
				bWarned = false;
				
				// 결과 xml 파일의 이름을 만든다.
				// success 카운트를 기준으로 한다.
				if ( mi_successCnt % mi_resultMaxRows  == 0 ) {
					if ( mi_successCnt > 0 ) {
						if ( oXmlWriter.saveFile(oResultFile.getPath()) == false ) {
							throw new Exception ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0002" ) );
						}
						// XmlWriter 설정.
						oXmlWriter = new XmlWriter();
						oXmlWriter.setColumnMatching ( mo_columnMatching ); 
					}
					
					sCurrentTime = ESPTypes.dateFormatString( new java.util.Date(), "yyyyMMddHHmmss.S");
					oResultFile = new File( Config.RESULT_PATH, ms_resultFileName + "_" + sCurrentTime + ".xml");					
				}
				
				/*
				For Test...
				Vector oItems = oDocument.getItems();
				for( int i=0; i< oItems.size(); i++ ) {
					Item oItem = (Item) oItems.elementAt(i);
					System.out.println ("#" + oItem.getName() + "=" + oItem.getType() );
				}
				*/
				
				// oExternalFileAppender객체를 얻는다.
				oExternalFileAppender = initDataQueryParameter( oDocument );
				if ( oExternalFileAppender == null ) {
					writeIndexLog ( XmlWriter.FAILED );
					continue;
				}

				// Key값으로 색인데이터를 조회한다. 
				// oDataReaderMap을 얻는다.
				// 데이터 쿼리에 본문내용이 있으면 oExternalFileAppender에 추가
				// 데이터쿼리에 파일정보 있다면 oAttachments에 추가
				oDataReaderMap = getDocIndexData( oDocument, oExternalFileAppender, oAttachments, oDFLoader );
				if ( oDataReaderMap == null ) {
					writeIndexLog ( XmlWriter.FAILED );
					continue;
				}

				
				// 조회된 색인데이터를 변환 ( oDataReaderMap을 => oResultMap )
				oResultMap = convertIndexData( oDataReaderMap  );
				if ( oResultMap == null ) {
					logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0030" ) );
					writeIndexLog ( XmlWriter.FAILED );
					continue;
				}
				
//				//본문요약(ABSTRCT)
//				if ( oDFLoader.isAbstractContainAttFile() == false ) {
//					//본문요약 컬럼(ABSTRCT)을 설정/ 추가
//					addAbstractColumn( oResultMap, oExternalFileAppender.getFileName() );
//				}
//
//				
//				// 첨부파일들을 하나로 파일로 합침
//				if ( getterAttachFile( oAttachments, oExternalFileAppender ) == false ) {
//					writeIndexLog ( XmlWriter.FAILED );
//					continue;
//				}
//
//				//본문요약(ABSTRCT)
//				if ( oDFLoader.isAbstractContainAttFile() == true) {
//					//본문요약 컬럼(ABSTRCT)을 설정/ 추가
//					addAbstractColumn( oResultMap, oExternalFileAppender.getFileName() );
//				}
//				
//				// 자동으로 설정되는 SearchServer 컬럼정보를 oResultMap 추가
//				addAutoIndexColumn( oResultMap, oExternalFileAppender.getFileName(), oAttachments );
				
				// customizer가 동작
				if ( customize (  oDataReaderMap, oResultMap ) == false ) {
					logger.error( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0032" ) );
					writeIndexLog ( XmlWriter.FAILED );
					continue;
				}
				
				// 본문쿼리, 파일쿼리 외에 추가 쿼리를 수행한다.
				if ( queryEtcData ( oDataReaderMap, oResultMap ) == false ) {
					writeIndexLog ( XmlWriter.FAILED );
					continue;					
				}
				
				// xml에 추가
				iState = addXmlData( oXmlWriter, oResultMap);
			
				writeIndexLog ( iState, bWarned );

				if ( logger.isDebugEnabled() ) {
					logger.debug( Localization.getMessage( NotesIndexer.class.getName() + ".Logger.0036" ) );
					logger.debug( oResultMap );
				}
				
			} // for
		

			// 색인일을 기록. (아직은 실제 DF파일에 저장되진 않음)
			writeIndexDate( oDFLoader );
			
			// 조회수 적용하지 않는다.
//			if ( Config.INDEX_MODE == DFLoader.INDEX_MODIFIED ) {
//				// 조회수 색인...
//				runDoccount( oXmlWriter );
//			} 
			
			// DF에 최종 조회수 색인일을 기록 (아직은 실제 DF파일에 저장되진 않음)
			writeDoccountIndexDate( oDFLoader );
						
		} catch ( Exception e ) {
			logger.error ( Localization.getMessage( NotesIndexer.class.getName() + ".Exception.0007" ) , e);
		} finally {
			
			// 색인수행결과의 요약정보 로그로 남김
			writeResultLog();
						
			logger.info ( "====================================================================================" );
			
			if ( oXmlWriter != null ) {
				if ( oResultFile == null ) {
					oXmlWriter.saveFile( null );
				} else {
					oXmlWriter.saveFile( oResultFile.getPath() );
				}
			}
						
			//=============================================================================
			// 데이터소스의 Connection 해제
			//=============================================================================
			if( oDocCollection != null ) { try { oDocCollection.recycle(); } catch ( NotesException ne ) {} } 
			if( oDocument != null ) { try { oDocument.recycle(); } catch ( NotesException ne ) {} }	
			closeSourceConnection();
		
		} // finally
	}	// method 
	
		
}                       
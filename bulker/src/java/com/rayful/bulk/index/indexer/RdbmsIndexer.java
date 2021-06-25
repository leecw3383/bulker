/**
 *******************************************************************
 * 파일명 : RdbmsIndexer.java
 * 파일설명 : 데이터 소스가 RDBMS인 경우 색인메인로직을 처리하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성      
 * 2005/08/09   정충열    ABSTRACT추출시 FileReader를 사용하도록 변경        
 *******************************************************************
*/

package com.rayful.bulk.index.indexer;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.DFLoaderException;
import com.rayful.bulk.index.DataConvertor;
import com.rayful.bulk.index.KeyData;
import com.rayful.bulk.index.ResultWriter;
import com.rayful.bulk.index.customizer.ConvertCustomizer;
import com.rayful.bulk.index.elastic.JsonESWriter;
import com.rayful.bulk.index.fastsearch.XmlWriter;
import com.rayful.bulk.io.Attachment;
import com.rayful.bulk.io.AttachmentGather;
import com.rayful.bulk.io.Attachments;
import com.rayful.bulk.io.ErrorFile;
import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.io.FilesAccessor;
import com.rayful.bulk.io.PdfConverter;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.RdbmsConnector;
import com.rayful.bulk.sql.RdbmsDataReader;
import com.rayful.bulk.sql.RdbmsEtcReader;
import com.rayful.bulk.util.HangulConversion;
import com.rayful.io.namo.NamoDocument;
import com.rayful.io.namo.NamoParser;
import com.rayful.io.namo.NamoParserException;
import com.rayful.localize.Localization;

/**
 * 색인대상이 RDBMS인 경우의 색인메인로직을 담당한다.
 * @author  	정충열
 * @version  1.1
 * @see  NotesIndexer
 */
public class RdbmsIndexer implements BaseIndexer
{
	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger(RdbmsIndexer.class.getName(), Config.LOG_PATH );
	
	/** 색인대상 Connection 객체 */
	protected Connection mo_sourceConnection;
	
	/**
	 * RDBMS 색인대상 커넥터객체
	 * @uml.property  name="mo_sourceConnector"
	 * @uml.associationEnd  
	 */
	protected RdbmsConnector mo_sourceConnector;
	/**
	 * RDBMS 데이터조회 객체
	 * @uml.property  name="mo_dataReader"
	 * @uml.associationEnd  
	 */
	protected RdbmsDataReader mo_dataReader;
	/**
	 * RDBMS 추가데이터조회 객체
	 * @uml.property  name="mo_etcReader"
	 * @uml.associationEnd  
	 */
	protected RdbmsEtcReader mo_etcReader;	
	
	/**
	 * 색인데이터 컨버터 객체
	 * @uml.property  name="mo_dataConvertor"
	 * @uml.associationEnd  
	 */
	protected DataConvertor mo_dataConvertor;	
	/**
	 * 컬럼매칭정보 객체
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	protected ColumnMatching mo_columnMatching;
	/**
	 * 파일접근자 객체
	 * @uml.property  name="mo_filesAccessor"
	 * @uml.associationEnd  
	 */
	protected FilesAccessor mo_filesAccessor;
	/**
	 * 커스터마이징객체
	 * @uml.property  name="mo_convertCustomizer"
	 * @uml.associationEnd  
	 */
	protected ConvertCustomizer mo_convertCustomizer;
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
		
	/** 최대색인건수 : 이 수 만큼만 색인함 **/
	protected int mi_maxIndexRows;
	
	/** 데이터소스 특정건의 조회 Key값 **/
	protected String ms_keys;	

	/** KeyQuery Parameter배열 */
	protected String [] ma_keyParams;
	/** DataQuery Parameter배열 */
	protected String [][] ma_dataKeyParams;
	/** FileQuery Parameter배열 */
	protected String [] ma_fileKeyParams;
	/** 현재 색인중인 키값들을 담은 맵 */
	protected Map<String, String> mo_curKeyMap;
	
	/** 데이터 소스ID **/
	protected String ms_dataSourceId;
	/** 색인결과파일 최대 크기 지정  **/
	protected int mi_resultFileSize;
	/** Customizer클래스명 **/
	protected String ms_convertCustomizerClassName;
	/** 추가 데이터 조회 클래스명 **/	
	protected String ms_etcReaderClassName;

	protected String ms_resultFileName = null;
	protected int mi_resultMaxRows;
	
	/** 색인실행시간  **/
	protected java.util.Date mo_ExecDate;
	
	/** 
	* 생성자
	*/	
	public RdbmsIndexer ()
	{
		mo_sourceConnection = null;
		
		mo_sourceConnector = null;
		mo_dataReader = null;
		mo_etcReader = null;
		mo_dataConvertor = null;
		mo_convertCustomizer = null;
		mo_errorFile = null;
		mo_warnFile = null;
		
		mo_columnMatching = null;
		mo_filesAccessor = null;
		mb_isDefinedGlobalConnect = false;
		
		// 카운트 초기화
		mi_totalCnt = 0;
		mi_tryCnt = 0;
		mi_successCnt = 0;
		mi_failCnt = 0;
		mi_warnCnt = 0;
		
		mi_maxIndexRows = -1;
		
		ma_dataKeyParams = null;
		ma_fileKeyParams = null;
		mo_curKeyMap = null;
		
		ms_dataSourceId = null;
		mi_resultFileSize = -1;
		ms_convertCustomizerClassName = null;
		ms_etcReaderClassName = null;

		ms_keys = null;
		mo_ExecDate = new java.util.Date();
		
		ms_resultFileName = null;
		mi_resultMaxRows = -1;
	}

	/** 
	* 색인대상 Connection객체를 클래스외부에서 설정한다.
	* <p>
	* @param	oSourceConnection	데이터소스 커넥션 객체
	*/
	public void setConnection ( Connection oSourceConnection )
	{
		mo_sourceConnection = oSourceConnection;
	}

	/** 
	* 데이터소스 Connection객체를 알아낸다.
	* <p>
	* @return	데이터소스 Connection객체
	*/
	public Connection getConnection ()
	{
		return mo_sourceConnection;
	}
	

	/** 
	* DF파일의 정보를 로드하여 필요한 색인관련 객체들을 생성한다 
	* RdbmsConnector, DataReader, ColumnMatching, FilesAccessor, ConvertCustomizer
	* <p>
	* @param	oDFLoader	DFLoader객체
	* @throws Exception 
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
		mo_sourceConnector = ( RdbmsConnector )oDFLoader.getDataSourceConnector( iDataSourceType );
		
		// 조회관련객체 생성
		mo_dataReader = ( RdbmsDataReader )oDFLoader.getDataSourceDataReader( iDataSourceType );
		
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
			logger.info ( Localization.getMessage( DFLoader.class.getName() + ".Logger.0040", mi_maxIndexRows ) );
		}	else if ( mi_maxIndexRows == 0 ) {
			throw new Exception ( Localization.getMessage( DFLoader.class.getName() + ".Exception.0001" ) );
		}
		
		// 데이터소스 ID를 알아낸다.
		ms_dataSourceId = oDFLoader.getDataSourceId();
		
		// 색인파일 생성 시 최대 사이즈를 지정한다.
		mi_resultFileSize = oDFLoader.getDataSourceResultFileSize();
		
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
		// 데이터소스 Connection정보가 전역적으로 선언되었고, 커넥션객체가 이미생성되었다면...
		// 커넥션을 생성하지 않는다.
		if ( mb_isDefinedGlobalConnect && mo_sourceConnection != null ) {
			return;
		}
				
		try {
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0001" ) );
			mo_sourceConnection = mo_sourceConnector.getConnection();

		} catch ( ClassNotFoundException cnfe ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0002" ), cnfe);
			throw cnfe;
		} catch ( SQLException se ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0003" ), se);
			throw se;
		}
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0004" ) );
		
	}
	
	
	/** 
	* 색인대상과의 접속을 끊는다.<p>
	*/		
	protected void closeSourceConnection () 
	{
		this.closeSourceConnection( false );
	}
	
	/**
	 * 색인대상과의 접속을 끊는다.<p>
	 * @param bForce	true: DF에 전역적으로 선언되어 있더라도 강제로 종료한다.
	 */
	protected void closeSourceConnection ( boolean bForce ) 
	{
		// 데이터소스 Connection정보가 전역적으로 선언되었다면... 커넥션을 해제하지 않는다.
		// 그러나 강제옵션을 true로 주었다면 아래 조건을 만족하지 않으므로 커넥션이 해제된다.
		if ( mb_isDefinedGlobalConnect && ! bForce ) {
			return;
		}

		try{
			if ( mo_sourceConnection != null ) {
				mo_sourceConnection.close();
				logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0022" ) );
			}
		} catch ( SQLException e ) {
			logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0023" ) );
		} finally {
			mo_sourceConnection = null;
		}
		
		// 파일접근자의 disconnect() 함수 호출
		if ( mo_filesAccessor != null ) {
			mo_filesAccessor.disconnect();
		}
	}
	
	
	/** 
	* ColumnMatching객체내 source/target 컬럼 Type정보를 설정한다.
	* @throws SQLException
	*/		
	protected void initColumnMatching ()
	throws SQLException
	{
		String sDataQuerySql = null;
		
		//=============================================================================
		// 컬럼정보 로드...
		//=============================================================================
		// 색인대상컬럼정보를 로드하여 세팅한다.
		
		// 기존에는 ? 으로 data_query 에서 key 정보를 설정했으나
		// 변경된 방식에서는 keys 정보를 매핑하도록 한다.
		// 즉, data_query keys="ID1|ID2|ID3" 으로 되어 있었으면
		// 기존에는 WHERE SITE_CD = ? 으로 되어 있었음.
		// 이를 WHERE SITE_CD = #ID1# 으로 변경함.
		// 여기서 #ID1#, #ID2#, #ID3# 으로 되어 있는 정보를 ? 으로 변환 처리하도록 한다.
		sDataQuerySql = mo_dataReader.getDataQuerySql();
		
		for(int i=0; i<mo_dataReader.dataKeys.length; i++) {
			sDataQuerySql = sDataQuerySql.replace("#"+mo_dataReader.dataKeys[i]+"#", "?");
		}
		
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0005" ) );
		mo_columnMatching.loadSourceColumnInfo( mo_sourceConnection, sDataQuerySql, mo_dataReader.dataKeys.length * mo_dataReader.dataMulti );
		
		if ( logger.isDebugEnabled() ) {
			logger.debug ( mo_columnMatching );
		}
	}	

	
	/** 
	* DataReader객체를 세팅하고, 내부적으로 QueryParameter배열을 생성한다. 
	* ( ma_dataKeyParams, ma_fileKeyParams )
	*/		
	protected void initDataReader()
	{

		// DataReader를 세팅한다.
		mo_dataReader.setConnection( mo_sourceConnection, mo_sourceConnector, ms_dataSourceId );
		mo_dataReader.setColumnMatching( mo_columnMatching );	
		
		// Query Parameters 배열 초기화...
		if ( mo_dataReader.Keys != null ) {
			ma_keyParams = new String [ mo_dataReader.Keys.length ];
		}
		
		if ( mo_dataReader.dataKeys != null ) {
			ma_dataKeyParams = new String [ mo_dataReader.dataKeys.length ][ mo_dataReader.dataMulti ] ;
		}
		
		if ( mo_dataReader.fileKeys != null ) {
			ma_fileKeyParams = new String [ mo_dataReader.fileKeys.length ];	
		}
	}
	
	/** 
	* EtcReader객체를 세팅한다.
	* ( mo_etcReader )
	*/		
	@SuppressWarnings("unchecked")
	protected void initEtcReader()
	throws Exception
	{
		Map mapEtcQuery = mo_dataReader.getEtcQuerySql();
		
		if ( ms_etcReaderClassName == null ) {
			return ;
		}
		
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0006", ms_etcReaderClassName ) );		
		try {
			// 클래스 생성
			Class etcReaderClass = Class.forName( ms_etcReaderClassName );

			// Consturctor Parameter Value 값을 설정
			Object [] aConParamValues = new Object[2];
			Class[] aConParamTypes = new Class[2];
			
			aConParamValues[0] = mo_sourceConnection;
			aConParamValues[1] = mapEtcQuery;
			
			aConParamTypes[0] = Class.forName("java.sql.Connection");
			aConParamTypes[1] = Class.forName("java.util.Map");
			
			
			// Constructor를 알아낸다.
			java.lang.reflect.Constructor oConstructor = etcReaderClass.getConstructor(aConParamTypes);
			// 알아낸 Constructor객체를 이용하여 동적객체를 생성
			mo_etcReader = (RdbmsEtcReader)oConstructor.newInstance(aConParamValues);

		} catch ( Exception  e ) {
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0007", ms_etcReaderClassName ), e );
			throw e;
		}
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0008", ms_etcReaderClassName ) );

	}	
	
	/** 
	* Oracle 하위버전을 위한 설정.. ( 8.1.6 이하버전은 Unicode 변환을 사용 )
	* Oracle 8.1.6이상버전과, 다른 RDBMS는 Unicode변환 사용안하도록 설정
	* @throws SQLException
	*/		
	protected void initUnicodeConvertor()
	throws SQLException
	{
		HangulConversion.setUnicodeConvert( false );
		
		if ( mo_sourceConnector.getRdbmsType() == RdbmsConnector.RDBMSTYPE_ORACLE ) {
			String sOracleVersion = mo_dataReader.getOracleVersion();
			if ( sOracleVersion.compareTo( "8.1.6.0.0" ) < 0 ) {
				// Unicode 1.2 ==> Unicode 2.0 변환을 사용 [중요]
				HangulConversion.setUnicodeConvert( true );
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0009", sOracleVersion ) );
			} else {
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0010", sOracleVersion ) );
			}
		}		
	}
	
	/** 
	* DataConvertor 객체를  동적으로 로드한다.
	* @throws Exception
	*/		
	protected void initDataConvertor()
	throws Exception
	{
		mo_dataConvertor = new DataConvertor(ms_dataSourceId, mo_columnMatching);
	}
	
	
	/** 
	* DF에 ConvertCustomizer 클래스명을 명시한 경우 해당객체를 동적으로 로드한다.
	* @throws Exception
	*/		
	protected void initConvertCustomizer()
	throws Exception
	{
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0011" ) );
		if ( ms_convertCustomizerClassName == null ) {
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0012" ) );
			return;
		}
	
		try {
			mo_convertCustomizer = ( ConvertCustomizer )Class.forName( ms_convertCustomizerClassName ).newInstance();
		}  catch ( ClassNotFoundException cnfe ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0013" ), cnfe );
			throw cnfe;
		} catch ( InstantiationException ie ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0013" ), ie );
			throw ie;
		} catch ( IllegalAccessException iae ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0013" ), iae );
			throw iae;
		}
		logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0014" ) );
	}	
	
	/** 
	* PreQuery 결과로 부터 KeyQuery의 Parameter값을 알아낸다.
	* <p>
	* @throws SQLException
	*/			
	protected void initKeyQueryParameter()
	throws SQLException, IOException
	{
		// pre Query 수행으로 2012/07/04 추가 - opensky
		for ( int i=0; i < ma_keyParams.length; i++ ) {
			ma_keyParams[i] = (String)mo_curKeyMap.get( mo_dataReader.Keys[i] );
			//oSb.append( ma_dataKeyParams[i] + "_" );
		}		
	}
	
	/** 
	* KeyQuery 결과로 부터 DataQuery의 Parameter값을 알아낸다.
	* Data Query의 키들을 이용하여  색인외부파일의  Appender객체를 생성하여 리턴한다.
	* <p>
	* @return	DataQuery의 키값으로 구성된 색인외부파일 Appender객체 
	* @throws SQLException
	*/			
	protected FileAppender initDataQueryParameter()
	throws SQLException, IOException
	{
		// [색인외부파일명] = dataQuery의 Parameter들 ...
		//StringBuffer oSb = new StringBuffer();
		
		//File oExternalPath = null;
		//File oExternalFile = null;
		//FileAppender oExternalFileAppender = null;
		
		// 다운로드 base 경로 + 데이터소스ID + 
		//oExternalPath = new File ( Config.DOWNLOAD_PATH +  File.separatorChar + ms_dataSourceId + File.separatorChar);

		//if ( ! oExternalPath.exists()  ) {
		//	if ( oExternalPath.mkdirs() == false ) {
		//		logger.error( "본문파일 저장경로를 생성할 수 없습니다 : " + oExternalPath.getPath() );
		//		return null;
		//	}
		//}
		
		// 본문파일명을 구성 : Data Query Key들을 조합
		//oSb = new StringBuffer();
		
		// pre Query 수행으로 2012/06/08 추가 - opensky
//		for ( int i=0; i < ma_keyParams.length; i++ ) {
//			ma_keyParams[i] = (String)mo_curKeyMap.get( mo_dataReader.Keys[i] );
//			//oSb.append( ma_dataKeyParams[i] + "_" );
//		}
		// -- 2012/06/08
		
		//기본 로직
		// 이차원 배열에서 값을 가져오도록 변경
		// performance 수정
//		for ( int i=0; i < ma_dataKeyParams.length; i++ ) {
//			ma_dataKeyParams[i] = (String)mo_curKeyMap.get( mo_dataReader.dataKeys[i] );
//			//oSb.append( ma_dataKeyParams[i] + "_" );
//		}
		
		for ( int i=0; i < ma_dataKeyParams.length; i++ ) {
			ma_dataKeyParams[i][0] = (String)mo_curKeyMap.get( mo_dataReader.dataKeys[i] );
		}
		
		//oSb.append( ".txt" );	// 확장자는 txt
		
		
		// 본무파일경로 + 본문파일명 
		//oExternalFile = new File ( oExternalPath.getPath(), oSb.toString() );


		// 본문외부파일이 이미 존재하면 삭제
		//if ( oExternalFile.exists() ) {
		//	if ( ! oExternalFile.delete() ) {
		//		logger.error( "기존 외부파일(" + oExternalFile.getPath() + ")을 삭제할 수 없습니다" );
		//		return null;
		//	}
		//}
		
		// 외부파일 Appender객체 생성
		//oExternalFileAppender = new FileAppender ( oExternalFile.getPath());
		
		
		return null ;				
	}
	
	/** 
	* KeyQuery 결과로 부터 DataQuery의 Parameter값을 알아낸다.
	* Data Query의 키들을 이용하여  색인외부파일의  Appender객체를 생성하여 리턴한다.
	* <p>
	* @return	DataQuery의 키값으로 구성된 색인외부파일 Appender객체 
	* @throws SQLException
	*/			
	protected FileAppender initDataQueryParameter(KeyData oKeyData, int iPos)
	throws SQLException, IOException
	{
		
		if ( mo_dataReader.dataMulti == 1) {
			for ( int i=0; i < ma_dataKeyParams.length; i++ ) {
				ma_dataKeyParams[i][0] = (String)mo_curKeyMap.get( mo_dataReader.dataKeys[i] );
			}
		} else {
			Map<String, String> curKeyMap = new HashMap<String,String>();
			int iGetPos=0, iKeyDataSize=0;
			
			if ( oKeyData != null ) {
				iKeyDataSize = oKeyData.size();
				//System.out.print("oKeyData.size1 : " + iKeyDataSize);
			}
			else {
				return null;
			}
			
			for(int j=0; j<mo_dataReader.dataMulti; j++) {
				//curKeyMap = oKeyData.get(i);
				
				iGetPos = iPos + j;
				iGetPos = iGetPos >= iKeyDataSize-1 ? iKeyDataSize-1 : iGetPos;
				
				//System.out.print(" " + iGetPos);
				curKeyMap = oKeyData.get(iGetPos);
				
				for(int i=0; i<ma_dataKeyParams.length; i++) {
					//System.out.println("KeyData Pos : " + (iPos+j));
					ma_dataKeyParams[i][j] = (String)curKeyMap.get( mo_dataReader.dataKeys[i] );
				}
			}
			
		}
		
		return null ;				
	}
	
//	/** 
//	* 색인데이터를 쿼리하여 결과를 Map으로 리턴한다.
//	* <p>
//	* @return	쿼리결과 객체
//	*/		
//	protected List<Map<String,String>> queryPreQuery(String sPreCondition)
//	throws Exception
//	{
//		List<Map<String,String>> listPreCondition = new ArrayList<Map<String,String>>();
//		
//		try {
//			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0015" ) );
//			
//			listPreCondition = mo_dataReader.executePreQuery(sPreCondition);
//			
//		} catch ( Exception e ) {
//			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0020" ), e);
//			throw e;
//		}
//		return listPreCondition;
//	}
	
	/** 
	* 색인데이터를 쿼리하여 결과를 Map으로 리턴한다.
	* <p>
	* @return	쿼리결과 객체
	*/		
	protected KeyData queryIndexKey()
	throws Exception
	{
		KeyData oKeyData = new KeyData();
		try {
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0015" ) );
			
			// Key Query를 DB로 부터 조회
			if ( Config.KEYQUERY_METHOD == Config.KEY_ALL 
				|| Config.KEYQUERY_METHOD == Config.KEY_DBQUERY  ) {
				
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0016" ) );
				
				
				mo_dataReader.executeKeyQuery ( oKeyData, mi_maxIndexRows );			
			}

			
			// Key Data를 Error File로 부터 읽어들임
			if ( Config.KEYQUERY_METHOD == Config.KEY_ALL 
					|| Config.KEYQUERY_METHOD == Config.KEY_ERROR  ) {
				if ( mi_maxIndexRows > 0 ) {
					logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0017" ) );
				} else {
					
					logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0018" ) );
					// 에러로그파일에서 키값을 읽어 온다.
					if ( mo_errorFile != null ) {
						try {
							mo_errorFile.load( oKeyData );
						} catch ( FileNotFoundException fnfe ) {
							logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0019", mo_errorFile.getPath() ) );
						}
					}					
				}
			}
			
			// 이전 에러로그파일 삭제
			if ( mo_errorFile != null ) {
				mo_errorFile.delete();
			}
			
			/*
			 * 파일접근자의 connect() 함수 호출
			 * 키쿼리가 색인대상이  많으면 길어질 수 있으므로 
			 * 이부분에서 연결해야 세션이 끊어지지 않는다.
			 */
			if ( mo_filesAccessor != null ) {
				if ( mo_filesAccessor.connect() == false ) {
					throw new Exception ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0001" ) );
				}
			}

		} catch ( Exception e ) {
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0020" ), e);
			throw e;
		}
		return oKeyData;
	}
	
	/** 
	* 색인데이터를 쿼리하여 결과를 Map에 담는다.
	* <p>
	*/		
	protected void queryIndexKey(KeyData oPreKeyData, String sPreCondition)
	throws Exception
	{
		//KeyData oKeyData = new KeyData();
		try {
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0015" ) );
			
			// Key Query를 DB로 부터 조회
			if ( Config.KEYQUERY_METHOD == Config.KEY_ALL 
				|| Config.KEYQUERY_METHOD == Config.KEY_DBQUERY  ) {
				
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0016" ) );
				
				mo_dataReader.setPreConditionValue(sPreCondition);
				mo_dataReader.executeKeyQuery ( oPreKeyData, mi_maxIndexRows );			
			}

			
			// Key Data를 Error File로 부터 읽어들임
			if ( Config.KEYQUERY_METHOD == Config.KEY_ALL 
					|| Config.KEYQUERY_METHOD == Config.KEY_ERROR  ) {
				if ( mi_maxIndexRows > 0 ) {
					logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0017" ) );
				} else {
					
					logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0018" ) );
					// 에러로그파일에서 키값을 읽어 온다.
					if ( mo_errorFile != null ) {
						try {
							mo_errorFile.load( oPreKeyData );
						} catch ( FileNotFoundException fnfe ) {
							logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0019", mo_errorFile.getPath() ) );
						}
					}					
				}
			}
			
			// 이전 에러로그파일 삭제
			if ( mo_errorFile != null ) {
				mo_errorFile.delete();
			}
			
			/*
			 * 파일접근자의 connect() 함수 호출
			 * 키쿼리가 색인대상이  많으면 길어질 수 있으므로 
			 * 이부분에서 연결해야 세션이 끊어지지 않는다.
			 */
			if ( mo_filesAccessor != null ) {
				if ( mo_filesAccessor.connect() == false ) {
					throw new Exception ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0001" ) );
				}
			}

		} catch ( Exception e ) {
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0020" ), e);
			throw e;
		}
		
		//return oKeyData;
	}
	
	/** 
	* 색인데이터를 쿼리하여 결과를 Map으로 리턴한다.
	* <p>
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @param	oAttachments	색인목록정보 객체
	* @return	쿼리결과Map
	*/		
	protected List<Map<String, Object>> queryIndexData( )
	{
		try {
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0024" ) );
			return  mo_dataReader.executeDataQuery ( null, ma_dataKeyParams, mo_dataReader.dataMulti );

		} catch ( Exception e ) {
			// 에러가 발생하면 현재 Row를 색인하지 않는다.
			return null;
		}
		
	}
	
	/** 
	* 색인데이터를 쿼리하여 결과를 Map으로 리턴한다.
	* <p>
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @param	oAttachments	색인목록정보 객체
	* @return	쿼리결과Map
	*/		
	protected List<Map<String, Object>> queryIndexDataMulti( List<Map<String,String>> listKey )
	{
		try {
			
			
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0024" ) );
			return  mo_dataReader.executeDataQuery ( listKey, ma_dataKeyParams, mo_dataReader.dataMulti );

		} catch ( Exception e ) {
			// 에러가 발생하면 현재 Row를 색인하지 않는다.
			return null;
		}
		
	}
	
	
	/** 
	* 첨부파일정보를 쿼리한다.
	* <p>
	* @param	oAttachments	색인목록정보 객체
	* @return	true: 정상 / false : 에러발생
	*/		
	protected boolean queryFileInfo( Attachments oAttachments )
	{
		if ( ma_fileKeyParams != null  ) {
			
			try {
				// 첨부파일정보를 조회시 필요한 키정보들을 세팅
				for ( int i=0; i < ma_fileKeyParams.length; i++ ) {
					ma_fileKeyParams[i] = (String)mo_curKeyMap.get( mo_dataReader.fileKeys[i] );
				}
			
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0033" ) );
				mo_dataReader.executeFileInfoQuery ( ma_fileKeyParams, oAttachments );
			} catch (SQLException e ) {
				// 에러가 발생하면 현재 Row를 색인하지 않는다.
				return false;
			}
			
		}
		return true;
	}

	protected boolean getBodyFilePath ( FileAppender oExternalFileAppender, Map<String, String> oResultMap ) {
		boolean bReturn = true;
		
		File oBodyFile = new File( oExternalFileAppender.getFileName());
		// 나모관련 객체 
		NamoDocument namoDoc = null;
		File oNamoTmpFile = null;
		
		try {
			//---------------------------------------------------------------
			// Namo 파일파싱을 시도한다.
			namoDoc = null;	// numDoc을 초기화한다.(중요)						
			try {
				namoDoc = NamoParser.parse( oBodyFile.getPath() );
				logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0043" ) );
				
			} catch (NamoParserException e){ logger.info(e); }
			
			// Namo 파일일 경우 본문을 디코딩해서 임시파일로 저장한 다음
			// 다시 원래 본문파일명을 바꾼다.
			if ( namoDoc != null ) {
				oNamoTmpFile = new File( oBodyFile.getPath() + "_tmp" );
				namoDoc.saveBodyFile( oNamoTmpFile.getPath() );
				
				// 임시파일이 존재한다면...
				if ( oNamoTmpFile.exists() ) {
					oBodyFile.delete();
					oNamoTmpFile.renameTo(oBodyFile);
				}
			}
			
			// 본문파일 경로에 입력한다.
			oResultMap.put( "filepath", oBodyFile.getAbsolutePath() );

		} catch ( Exception e ) {
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0044" ), e);
			bReturn = false;
			//아무작업도 하지 않는다....
		}	
		
		return bReturn;
	}
	
	
	/** 
	* 조회된 색인데이터를 색인하기위한 형태로 Convert한다.
	* <p>
	* @param	oDataReaderMap	조회된 색인데이터 Map
	* @return	Converted 데이터 Map
	*/		
	protected Map<String, Object> convertIndexData(Map<String, Object> oDataReaderMap )
	{
		Map<String, Object> oResultMap = null;
		try {
			oResultMap =  mo_dataConvertor.convertData( oDataReaderMap );

		} catch (Exception e ) {
			e.printStackTrace();
			//아무작업도 하지 않는다....
		}		
		
		return oResultMap;
	}
	
	
	/** 
	* 첨부파일들의 텍스트를 하나의 색인외부파일에 모은다.
	* <p>
	* @param	oAttachments	색인목록정보 객체
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @return	true: 정상 / false : 에러발생
	*/	
	protected boolean gatherAttachFile( Attachments oAttachments )
	{
		if ( mo_filesAccessor == null ) {
			return true;
		}
		
		int iSuccessCnt = 0;
		int iAttachCnt = oAttachments.getAttachmentCount(); 
		
		if ( iAttachCnt> 0 ) {
			AttachmentGather oAttGather = new AttachmentGather( mo_filesAccessor );
			iSuccessCnt = oAttGather.gather( oAttachments );
		}
		
		if ( iAttachCnt == iSuccessCnt ) {
			return true;
		} else {
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0045" ) );
			return false;
		}
	}
	
	
	/** 
	* 첨부파일들의 텍스트를 하나의 색인외부파일에 모은다.
	* <p>
	* @param	oAttachments	색인목록정보 객체
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @return	true: 정상 / false : 에러발생
	*/	
	protected boolean addAttachFileInfo( XmlWriter oXmlWriter, Map<String, Object> oResultMap, Attachments oAttachments )
	{
		File oDownloadedFile = null;
		int iFileExtType = 0;
		Attachment oAttachment = null;
		int iSuccessCnt = 0;
		StringBuffer sbUrlList = new StringBuffer();
		StringBuffer sbFileNameList = new StringBuffer();
		String sUrl = null;
	
		for (int i=0; i< oAttachments.getAttachmentCount(); i++) {
			// 첨부파일정보객체를 얻는다.
			oAttachment = oAttachments.getAttachment(i);
			// 저장할 파일의 경로및 이름 
			oDownloadedFile = new File ( Config.DOWNLOAD_PATH,  oAttachment.getTargetFileName());
			// 다운로드할 파일의 확장자 Type
			iFileExtType = com.rayful.bulk.io.FileAppender.getFileExtentionType( oDownloadedFile.getPath());
		
			
			if ( iFileExtType == com.rayful.bulk.io.FileAppender.UNKNOWN ) {
				// -------------------------------------------------------
				// 색인대상파일이 아닌경우
				// -------------------------------------------------------				
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0035", oAttachment.getSourceFileName() ) );	
				//[주의]실패가 아님 - iSuccessCnt를 증가...
				iSuccessCnt ++;		
				
				// DB에 파일자체가 있는경우 파일이 이미 색인서버에 있으므로 삭제시도한다.
				if ( oDownloadedFile.exists() ) {
					oDownloadedFile.delete();
				}
				
				continue;	
			} else {
				
				if ( sbUrlList.length() > 0 ) {
					sbUrlList.append( "|" );
				}
				
				if ( sbFileNameList.length() > 0 ) {
					sbFileNameList.append( "|" );
				}					
				
				// -------------------------------------------------------
				// 첨부파일이 아직 원격지에 있는 경우 
				// -------------------------------------------------------
				if ( oAttachment.isRequireDownload() ) {
					try {
					sUrl = mo_filesAccessor.getURL( oAttachment.getSourceFilePath(), oAttachment.getSourceFileName());
					} catch ( Exception e) {
						
					}

					sbUrlList.append(sUrl);
					sbFileNameList.append(oAttachment.getOriginalFileName());
				} else {
				// -------------------------------------------------------
				// local에 있는 경우
				// -------------------------------------------------------					
					sbUrlList.append( mo_filesAccessor.buildUrlPath( "file:///", oDownloadedFile.getPath()));
					sbFileNameList.append(oAttachment.getOriginalFileName());
				}

			} // iFileExtType != com.yess.ss.index.io.FileAppender.UNKNOWN 
	
		} // for
	

		// 파일 관련  색인
		oResultMap.put("getpath", sbUrlList.toString());
		oResultMap.put("attachnames", sbFileNameList.toString());		
		return true;
		
	}	
	
	/** 
	* 첨부파일을  binary->base64인코딩으로 변환
	* <p>
	* @param	oAttachments	색인목록정보 객체
	* @param	oExternalFileAppender	색인외부파일 Appender객체
	* @return	true: 정상 / false : 에러발생
	*/	
	protected boolean addAttachFileBase64Info( ResultWriter resultWriter, Map<String, Object> oResultMap, Attachments oAttachments )
	{
		File oDownloadedFile = null;
		int iFileExtType = 0;
		Attachment oAttachment = null;
		int iSuccessCnt = 0;
		
		StringBuffer sbAttachmentData = new StringBuffer();
		String sBase64Data = null;
		
		for (int i=0; i< oAttachments.getAttachmentCount(); i++) {
			// 첨부파일정보객체를 얻는다.
			oAttachment = oAttachments.getAttachment(i);
			
			// 저장할 파일의 경로및 이름 
			oDownloadedFile = new File (Config.DOWNLOAD_PATH,  oAttachment.getTargetFileName());
			
			// 다운로드할 파일의 확장자 Type
			iFileExtType = com.rayful.bulk.io.FileAppender.getFileExtentionType( oDownloadedFile.getPath());
		
			if ( iFileExtType == com.rayful.bulk.io.FileAppender.UNKNOWN ) {
				
				// -------------------------------------------------------
				// 색인대상파일이 아닌경우
				// -------------------------------------------------------				
				logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0035", oAttachment.getSourceFileName() ) );	
				//[주의]실패가 아님 - iSuccessCnt를 증가...
				iSuccessCnt ++;		
				
				// DB에 파일자체가 있는경우 파일이 이미 색인서버에 있으므로 삭제시도한다.
				if ( oDownloadedFile.exists() ) {
					oDownloadedFile.delete();
				}
				
				continue;	
			} else {
				
				if ( sbAttachmentData.length() > 0 ) {
					sbAttachmentData.append( "|" );
				}
				
				// 새로 복사하기 위해 기존에 동일 파일이 있다면 삭제
				if (oDownloadedFile.exists()) {
					oDownloadedFile.delete();
				}
				
				try {
					
					Path in = Paths.get(oAttachment.getSourceFilePath());
					Path out = Paths.get(oDownloadedFile.getPath());
					
					// 원본 데이터 복사
					if(in != null && in != null) {
						Files.copy(in, out);
					}
					
					byte[] binary = null;
					
					if( iFileExtType == com.rayful.bulk.io.FileAppender.PDF ) {
						String newPdfFilePath = PdfConverter.convertPdf(oDownloadedFile.getPath());
						binary = oAttachments.getFileBinary(newPdfFilePath);
						File oNewDownloadedFile = new File (newPdfFilePath);
						
						if ( oNewDownloadedFile.exists() ) {
							oNewDownloadedFile.delete();
						}
					} else {
						binary = oAttachments.getFileBinary(oDownloadedFile.getPath());
						
					}
					
					if ( oDownloadedFile.exists() ) {
						oDownloadedFile.delete();
					}
					// base64의 라이브러리에서 encodeToString를 이용해서 byte[] 형식을 String 형식으로 변환합니다.
					sBase64Data = Base64.getEncoder().encodeToString(binary);
					//System.out.println(sBase64Data);
						
					// TIKA 사용시 테스트 부분
					/*File file = new File(oDownloadedFile.getPath());
					File destFile = null;
					
					TikaParser tikaParser = new TikaParser();
					
					if (file.isFile()) {
						tikaParser.getMimeType(file);
						tikaParser.getCharset(file);
						
						//String destFileName = file.getName() + ".txt";
						destFile = new File (oDownloadedFile.getPath() + ".txt");
						logger.info(file.getName());
						tikaParser.getMetadata(file, destFile);
						logger.info(file.getName() + ".txt");
					}*/
				} catch (Exception e) {
					logger.info(e);
				}
				
				sbAttachmentData.append(oAttachment.getOriginalFileName()).append("^").append(sBase64Data);

			} // iFileExtType != com.yess.ss.index.io.FileAppender.UNKNOWN 
	
		} // for

		String chkStr = sbAttachmentData.toString();
		
		if(chkStr != null && chkStr.length() > 0) {
			
			//System.out.println(chkStr.indexOf("|"));
			//System.out.println(chkStr.indexOf("^"));
			
			// multi file 이면 attachments, single file 이면 data로 넣도록 테스트
			// 단, pipeline 에서는 하나의 방식만을 선택해야함으로  실효성 없으나 테스트 용도로 만들어둠
			/*if(chkStr.indexOf("|") > 0) {
				oResultMap.put("attachments", chkStr);
			} else {
				if(chkStr.indexOf("^") > 0) {
					String [] attachedData = chkStr.split("\\^");
					
					if(attachedData != null && attachedData.length > 1) {
						oResultMap.put("data", attachedData[1]);
					}
				}
			}*/
			
			if(chkStr.indexOf("^") > 0) {
				// multi file  일 경우
				oResultMap.put("attachments", chkStr);
				
				// single file  일 경우
				/*String [] attachedData = chkStr.split("\\^");
				
				if(attachedData != null && attachedData.length > 1) {
					oResultMap.put("data", attachedData[1]);
				}*/
			} 
		}
		
		return true;
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
			logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0031" ), e );
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
	protected int addResultData( ResultWriter resultWriter, Map<String, Object> oResultMap )
	{
		int iState;
		//=============================================================================
		// 색인테이블에 반영 
		// Insert / Update
		//=============================================================================
		
		if ( resultWriter.addDocument(oResultMap) ) {
			iState = ResultWriter.SUCCESSED;
		} else {
			iState =  ResultWriter.FAILED;
		}

		return iState;
	}
	
	/** 
	* 색인요약결과를 로그에 기록한다.
	*/		
	protected void writeResultLog()
	{
			// 색인수행결과의 요약정보 로그로 남김
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0046" ) );
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0047", mi_totalCnt ) );
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0048", mi_tryCnt ) );
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0049", mi_successCnt ) );
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0050", mi_failCnt ) );
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0051", mi_warnCnt ) );			
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0046" ) );
	}
	
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	*/	
	protected void writeIndexLog( int iState  )
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
		
		if ( iState == ResultWriter.SUCCESSED ) {
			mi_successCnt ++;
			sMessage = ". success ";
		} else {
			mi_failCnt ++;
			sMessage = ". failed ";
		}

		
		//에러로그파일에 기록
		if ( iState == ResultWriter.FAILED  ) {
			
			if ( mo_errorFile != null ) {
				try { 
					mo_errorFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0026" ), e);
				}
			}			
		} else if ( bWarning ) {
			mi_warnCnt ++;
			sMessage += "but Warnning!!! ";
			
			if ( mo_warnFile != null ) {
				try { 
					mo_warnFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0027" ), e);
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
		
		logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0028", mi_tryCnt, sMessage ) );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	*/	
	protected void writeIndexLog( int iState, int iLoop  )
	{
		writeIndexLog( iState, false, iLoop );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	*/	
	protected void writeIndexLog( int iState, int iLoop, String sKeyString  )
	{
		writeIndexLog( iState, false, iLoop, sKeyString );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	* @param bWarning 경고대상여부
	*/	
	protected void writeIndexLog( int iState, boolean bWarning, int iLoop )
	{
		String sMessage = null; 
		
		if ( iState == ResultWriter.SUCCESSED ) {
			mi_successCnt ++;
			//mi_successCnt = (mi_successCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage = ". success ";
		} else {
			mi_failCnt ++;
			//mi_failCnt = (mi_failCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage = ". failed ";
		}

		
		//에러로그파일에 기록
		if ( iState == ResultWriter.FAILED  ) {
			
			if ( mo_errorFile != null ) {
				try { 
					mo_errorFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0026" ), e);
				}
			}			
		} else if ( bWarning ) {
			mi_warnCnt ++;
			//mi_warnCnt = (mi_warnCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage += "but Warnning!!! ";
			
			if ( mo_warnFile != null ) {
				try { 
					mo_warnFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0027" ), e);
				}
			}			
		}		
		
		if ( ma_dataKeyParams != null ) {
			StringBuffer oKeys = new StringBuffer ( " Keys:" );
			
			for ( int i=0; i < ma_dataKeyParams.length; i++ )
			{	
				if ( mo_dataReader.dataMulti == 1 ) {
					oKeys.append( ma_dataKeyParams [i][0] );
				} else {
					oKeys.append( ma_dataKeyParams [i][iLoop%mo_dataReader.dataMulti] );
				}

				if ( i < ma_dataKeyParams.length -1 ) {
					oKeys.append( " , " );
				}
				
			}
			sMessage += oKeys.toString();
		}
		
//		logger.info( "======== Sucess Cnt : " + mi_successCnt);
		logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0028", (mi_tryCnt-1)*mo_dataReader.dataMulti + (iLoop+1), sMessage ) );
		//logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0028", (mi_tryCnt-1)*mo_dataReader.dataMulti + 1, sMessage ) );
	}
	
	/** 
	* 색인건의 성공/실패여부를 로그에 기록한다.
	* @param iState 색인작업상태
	* @param bWarning 경고대상여부
	*/	
	protected void writeIndexLog( int iState, boolean bWarning, int iLoop, String sKeyString )
	{
		String sMessage = null; 
		
		if ( iState == ResultWriter.SUCCESSED ) {
			mi_successCnt ++;
			//mi_successCnt = (mi_successCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage = ". success ";
		} else {
			mi_failCnt ++;
			//mi_failCnt = (mi_failCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage = ". failed ";
		}
		
		//에러로그파일에 기록
		if ( iState == ResultWriter.FAILED  ) {
			
			if ( mo_errorFile != null ) {
				try { 
					mo_errorFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0026" ), e);
				}
			}			
		} else if ( bWarning ) {
			mi_warnCnt ++;
			//mi_warnCnt = (mi_warnCnt-1)*mo_dataReader.dataMulti + 1;
			sMessage += "but Warnning!!! ";
			
			if ( mo_warnFile != null ) {
				try { 
					mo_warnFile.write( mo_curKeyMap );
				} catch ( Exception e ) {
					logger.warn( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0027" ), e);
				}
			}			
		}		
		
		sMessage = sMessage + " Keys:" + sKeyString;
		
//		logger.info( "======== Sucess Cnt : " + mi_successCnt);
		logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0028", (mi_tryCnt-1)*mo_dataReader.dataMulti + (iLoop+1), sMessage ) );
		//logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0028", (mi_tryCnt-1)*mo_dataReader.dataMulti + 1, sMessage ) );
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
				logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0037" ) );
			}
		} else {
			logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0038" ) );
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
				logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0039" ) );
			}			
		} else {
			logger.info( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0040" ) );
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
	* 특정 row만 색인을 수행한다.(필수구현함수)
	* <p>
	* @param	sKeys	색인하려는 데이터의 키값
	* 현재 구현되지 않음
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
	* Rdbms 색인메인로직을 구현한다.(필수구현함수)
	* <p>
	* @param	oDFLoader	DFLoader객체
	* @param	oSSWriter	색인Writer객체
	* @param	oMetaConnection	SS관리DB Connection객체
	*/	
	public void run( DFLoader oDFLoader )
	throws Exception
	{
		// 키 데이터를 전체를  담은 클래스
		KeyData oKeyData = null;
		int iState =0;
		boolean bWarned = false;
		File oResultFile = null;
		String sCurrentTime = null;
		ResultWriter resultWriter = null;
		
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

			// DataSource가 oracle이고,  8.1.6이전일때 Unicode변환을 사용하도록 세팅
			// 그외의 경우는 Unicode변환을 사용하지 않음
			initUnicodeConvertor();
		
			// DataConvertor객체 로드 및 설정
			initDataConvertor();
			
			// ConvertCustomizer 객체 로드 및 설정
			initConvertCustomizer();

			// ResultWriter 설정.
			resultWriter = new JsonESWriter();
			// XmlWriter 설정.
			//resultWriter = new XmlWriter();
			
			resultWriter.setColumnMatching ( mo_columnMatching ); 
			
			// 색인대상 Key Query
			if ( mo_dataReader.getPreQuerySql() != null && mo_dataReader.getPreQuerySql().length() > 0 ) {
				List<String> listPreData = new ArrayList<String>();
				listPreData = mo_dataReader.executePreQuery();
				oKeyData = new KeyData();
				for(int iPreCnt=0; iPreCnt<listPreData.size(); iPreCnt++) {
					queryIndexKey(oKeyData, listPreData.get(iPreCnt));
				}
			} else {
				oKeyData = queryIndexKey();
			}
			// Key데이터의 로우수를 알아낸다.
			mi_totalCnt = oKeyData.size();
			
			// 색인데이터 결과를 담는 객체
			//Map<String, Object> oDataReaderMap = null;
			// 색인데이터 결과를 담는 리스트 객체
			List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
			// 컨버팅 결과를 담는 객체
			Map<String, Object> oResultMap = null;
			
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0021", mi_totalCnt ) );
			
			// 2012-03-05 add
			// jdbc connector 와 같은 역활을 하는 로직 구현
			// 모든 데이터를 DB에서 조회하여 저장하고 저장된 데이터와 비교하여 색인에서 삭제하도록 한다.
			//saveKeyToFile(oKeyData, oDFLoader.getDFName());
			
			for (int i=0; i< mi_totalCnt; i += mo_dataReader.dataMulti) {
				
				//i = mo_dataReader.dataMulti * i;
				
				//----------------------------------------------------------------------------------------
				// ### 에러로직 ###
				// [치명]적인 에러가 발생하면 색인프로그램을 종료
				// [일반]적인 에러가 발생하면 해당 로우를 Skip...
				// 따라서 여기서 호출되는 함수는 내부에서 Exception처리를 하고 성공여부를 리턴
				// 또는 치명적인 경우는 Excetion을 throw 통해 전달
				//----------------------------------------------------------------------------------------
				mi_tryCnt ++;			
				bWarned = false;
				
				// 결과  파일의 이름을 만든다.
				// success 카운트를 기준으로 한다.
//				logger.info("========== mi_successCnt % mi_resultMaxRows : " + (mi_successCnt % mi_resultMaxRows));
				if ( mi_successCnt % mi_resultMaxRows  == 0 ) {
					if ( mi_successCnt > 0 ) {
						if ( resultWriter.saveFile(oResultFile.getPath()) == false ) {
							throw new Exception ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0002" ) );
						}
						// JsonESWriter 설정.
						resultWriter = new JsonESWriter();
						// XmlWriter 설정.
						//resultWriter = new XmlWriter();
						resultWriter.setColumnMatching ( mo_columnMatching );
						
//						logger.info( "========== Change Result XML1 =========" );
					}
					
					sCurrentTime = ESPTypes.dateFormatString( new java.util.Date(), "yyyyMMddHHmmss.S");
					oResultFile = new File( Config.RESULT_PATH, ms_resultFileName + "_" + sCurrentTime + resultWriter.getFileExt());					
				}
				
				// 커넥션 재설정
				if ( i % 10000  == 0 && i > 0) {
					// --------------------------------------------------------
					// 커넥션을 재설정한다.
					// DB 커넥션이 너무 오래 동안 사용하면  간혹 끊기는 경우가 발생하므로
					// xml을 write하는 시점에서 커넥션을 다시 맺는다.
					// --------------------------------------------------------
					// 강제로 DB 커넥션을 종료한다. : true
					this.closeSourceConnection(true);
					// DB에 다시 접속한다.
					this.connectSourceConnection();
					// DB작업을 하는 DataReader의 커넥션을 재설정한다.
					mo_dataReader.setConnection(mo_sourceConnection, mo_sourceConnector, ms_dataSourceId );
					
					if ( mo_etcReader != null ) {
						// DB작업을 하는 EtcReader의 커넥션을 재설정한다.
						mo_etcReader.setConnection(mo_sourceConnection);
					}
					// --------------------------------------------------------					
				}
				
				// Key데이터 한 Row를 맵에 담는다.
				mo_curKeyMap = oKeyData.get(i);
				
				List<Map<String,String>> listKey = null;
				int iKeyCnt = 0;
				// Key값으로 색인데이터를 조회한다. 
				// oDataReaderMap을 얻는다.
				if ( mo_dataReader.dataMulti > 1) {
					
					// 쿼리에 필요한 Parameter들을 설정한다.
					initDataQueryParameter(oKeyData, i);
					
					listKey = new ArrayList<Map<String,String>>();
					
					int iGetPos = 0;
					int iKeyDataSize = oKeyData.size();
					
					for(int k=i; k<i+mo_dataReader.dataMulti; k++) {
						
						iGetPos = k >= iKeyDataSize-1 ? iKeyDataSize-1 : k;
						
						listKey.add(oKeyData.get(iGetPos));
					}
					
					listMap = queryIndexDataMulti( listKey );
				} else {
					
					// 쿼리에 필요한 Parameter들을 설정한다.
					initDataQueryParameter();
					
					listMap = queryIndexData( );
				}
				
				//for(Map<String, Object> oDataReaderMap : listMap) {
				// success
				//for(int iMapLoop=0; iMapLoop<listMap.size(); iMapLoop++) {
				
				if ( listMap == null ) {
					/*
					 * for ( int i=0; i< aParams.length; i++ ) {
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
					 */
					for(int ij=0;ij<mo_dataReader.dataMulti; ij++)
						writeIndexLog ( ResultWriter.FAILED, ij );
					
					continue;
				}
				
				iKeyCnt = listMap.size();
				for(int iMapLoop=0; iMapLoop<mo_dataReader.dataMulti; iMapLoop++) {
					
					if ( mi_successCnt % mi_resultMaxRows  == 0 ) {
						if ( mi_successCnt > 0 ) {
							if ( resultWriter.saveFile(oResultFile.getPath()) == false ) {
								throw new Exception ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Exception.0002" ) );
							}
							// JsonESWriter 설정.
							resultWriter = new JsonESWriter();
							// XmlWriter 설정.
							//resultWriter = new XmlWriter();
							
							resultWriter.setColumnMatching ( mo_columnMatching );
							
//							logger.info( "========== Change Result XML =========" );
						}
						
						sCurrentTime = ESPTypes.dateFormatString( new java.util.Date(), "yyyyMMddHHmmss.S");
						oResultFile = new File( Config.RESULT_PATH, ms_resultFileName + "_" + sCurrentTime + resultWriter.getFileExt());					
					}
					
					if ( (mi_tryCnt-1)*mo_dataReader.dataMulti + iMapLoop >= mi_totalCnt ) {
						if ( logger.isDebugEnabled() ) {
							logger.debug(mi_totalCnt + " Count : " + ( (mi_tryCnt-1)*mo_dataReader.dataMulti + iMapLoop ) );
						}
						break;
					}
					
					if ( ( mo_dataReader.dataMulti > 1 ) && ( iMapLoop >= iKeyCnt ) ) {						
						writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						continue;
					} 
					
					Map<String, Object> oDataReaderMap = null;
					
					oDataReaderMap = listMap.get(iMapLoop);
					
					if ( oDataReaderMap == null ) {
						logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0025" ) );
						if(mo_dataReader.dataMulti > 1) {
							//writeIndexLog ( ResultWriter.FAILED, iMapLoop );
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;
					}
					
					if ( logger.isDebugEnabled() ) {
						logger.debug( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0029" ) );
						logger.debug( oDataReaderMap );
					}
					
					// 조회된 색인데이터를 변환 ( oDataReaderMap을 => oResultMap )
					oResultMap = convertIndexData( oDataReaderMap );
					if ( oResultMap == null ) {
						logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0030" ) );
						if (mo_dataReader.dataMulti > 1) {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;
					}
					
					/*
					if ( getBodyFilePath(oExternalFileAppender, oResultMap) == false ) {
						logger.error( "getBodyFilePath()");
						writeIndexLog ( ResultWriter.FAILED );
						continue;
					}
					*/
					
					// customizer가 동작
					if ( customize (  oDataReaderMap, oResultMap ) == false ) {
						logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0032" ) );
						if (mo_dataReader.dataMulti > 1) {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;
					}				
	
					// 색인외부파일 정보를 담을 객체를 생성한다.
					Attachments oAttachments = new Attachments();	
					
					// Key값으로 첨부파일정보를 조회한다.
					// oAttachments에 추가
					if ( queryFileInfo( oAttachments ) == false ) {
						logger.error( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0034" ) );
						if (mo_dataReader.dataMulti > 1) {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;
					}
					
					// 첨부파일 경로정보를 포함한다.
					// 기존 첨부파일 방식
					//if ( addAttachFileInfo( resultWriter, oResultMap, oAttachments ) == false ) {
					// 첨부파일을 binary로 변환 후  base64 인코딩한 값을  TargetValue에 입력
					if ( addAttachFileBase64Info( resultWriter, oResultMap, oAttachments ) == false ) {
						if (mo_dataReader.dataMulti > 1) {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;
					}	
					
					// 본문쿼리, 파일쿼리 외에 추가 쿼리를 수행한다.
					if ( queryEtcData ( oDataReaderMap, oResultMap ) == false ) {
						if (mo_dataReader.dataMulti > 1) {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop, getDataKeyInfoInResult(ResultWriter.FAILED, listMap, iMapLoop, listKey) );
						} else {
							writeIndexLog ( ResultWriter.FAILED, iMapLoop );
						}
						continue;					
					}
					
					// 데이터 추출 파일에 추가
					iState = addResultData( resultWriter, oResultMap);
				
//					logger.info("[" + iMapLoop + "] =========== state : " + iState + " ==========" );
					
					
					
					if(mo_dataReader.dataMulti > 1) {
						writeIndexLog ( iState, bWarned, iMapLoop, getDataKeyInfoInResult(iState, listMap, iMapLoop, listKey) );
					} else {
						writeIndexLog ( iState, bWarned, iMapLoop );
					}
					
					if ( logger.isDebugEnabled() ) {
						logger.debug( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0036" ) );
						logger.debug( oResultMap );
					}
				}

			} // for

			//----------------------------------------------------------------------------------------
			// ### 에러로직 ###
			// 에러가 발생하면 색인프로그램을 종료
			// 여기서 호출되는 함수는 내부에서 Exception이 발생하면 그것을 전달
			//----------------------------------------------------------------------------------------
			// 색인일을 기록. (아직은 실제 DF파일에 저장되진 않음)
			writeIndexDate( oDFLoader );
			
			// 조회수 수정로직 없음.
			
			// DF에 최종 조회수 색인일을 기록 (아직은 실제 DF파일에 저장되진 않음)
			writeDoccountIndexDate( oDFLoader );			
						
		} catch ( Exception e ) {
			logger.error ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0041" ), e );
			
			//에러파일에 키들을 기록한다.
			try {
				if ( mo_errorFile != null ) {
					mo_errorFile.write(oKeyData, mi_tryCnt -1 );
				}
			} catch ( Exception ee ){}
			
			throw e;
		} finally {

			// 색인수행결과의 요약정보 로그로 남김
			writeResultLog();
			logger.info ( Localization.getMessage( RdbmsIndexer.class.getName() + ".Logger.0042" ) );	
			
			if ( resultWriter != null ) {
				if ( oResultFile == null ) {
					resultWriter.saveFile( null );
				} else {
					// result file size 가 없는 경우
					//resultWriter.saveFile( oResultFile.getPath() );
					resultWriter.saveFile( oResultFile.getPath(), mi_resultFileSize );
				}
			}
			
			//색인대상과의 연결을 종료합니다.
			closeSourceConnection();
		} // finally
	}	// run()
	
	public String getDataKeyInfoInResult(int iState, List<Map<String, Object>> listMap, int iLoop, List<Map<String,String>> listKey) 
	{
		int iDataKeysLen = mo_dataReader.dataKeys.length;
		int iListMapLen = listMap.size();
		int iListKeyLen = listKey.size();

		StringBuffer sb = new StringBuffer("");
		
		if ( iState == ResultWriter.FAILED ) {
			for(int k=0; k<iListKeyLen; k++) {
				Map<String, String> mapListKey = listKey.get(k);
				boolean bMapKey = false;
				for(int i=0; i<iListMapLen; i++) {
					Map<String, Object> mapData = listMap.get(i);
					
					Map<String, String> mapKey = new HashMap<String, String>();
					
					for(int j=0; j<iDataKeysLen; j++) {
						String sDataKey = mo_dataReader.dataKeys[j];
						
						mapKey.put(sDataKey, (String)mapData.get(sDataKey));
					}
					
					if ( mapListKey.equals(mapKey) ) {
						bMapKey = true;
					} 
				}
				
				if ( bMapKey == false ) {
					//System.out.println("No Data : " + mapListKey.toString());
					for(int j=0; j<iDataKeysLen; j++) {
						String sDataKey = mo_dataReader.dataKeys[j];
						
						if ( sb.length() > 0 ) 
							sb.append(" , ");
						
						sb.append(mapListKey.get(sDataKey));
					}
				}
			}
		} else if ( iState == ResultWriter.SUCCESSED ) {
			Map<String, Object> mapData = listMap.get(iLoop);
			
			for(int j=0; j<iDataKeysLen; j++) {
				String sDataKey = mo_dataReader.dataKeys[j];
				
				if ( sb.length() > 0 ) 
					sb.append(" , ");
				
				sb.append(mapData.get(sDataKey));
			}
		}
		
		return sb.toString();
	}
	
//	/**
//	 * DB에서 읽어온 key를 저장하고 저장된 key 값과 비교하여 존재하지 않은 데이터를 삭제하는 로직을 구현하도록 한다.
//	 * @param keydata
//	 */
//	public void saveKeyToFile(KeyData keydata, String sDFName)
//	{
//		File oKeyFile = new File( Config.KEY_INDEX_PATH, sDFName + "_keydata.txt");
//		
//		if ( oKeyFile.exists() ) {
//			//loadKeyData();
//		} 
//		saveKeyData(keydata, oKeyFile);
//	}
//	
//	public void saveKeyData(KeyData keydata, File oKeyFile)
//	{
//		
//	}
}
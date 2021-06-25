package com.rayful.bulk.index.indexer;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.DFLoaderException;
import com.rayful.bulk.index.KeyData;
import com.rayful.bulk.index.elastic.BulkData;
import com.rayful.bulk.index.elastic.JsonESWriter;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.RdbmsConnector;
import com.rayful.bulk.sql.RdbmsDataReader;
import com.rayful.bulk.sql.RdbmsEtcReader;
import com.rayful.localize.Localization;
	
public class DelIndexUpdator {
	
	static Logger logger = null;
	public static DelIndexUpdator  instance = null;

	private String collectionName = null;
	
	/** 추가 데이터 조회 클래스명 **/	
	private static String ms_etcReaderClassName;
	
	/**
	 * RDBMS 데이터조회 객체
	 * @uml.property  name="mo_dataReader"
	 * @uml.associationEnd  
	 */
	private static RdbmsDataReader mo_dataReader;
	/**
	 * RDBMS 추가데이터조회 객체
	 * @uml.property  name="mo_etcReader"
	 * @uml.associationEnd  
	 */
	private static RdbmsEtcReader mo_etcReader;
	
	// 로그를 위한 통계치
	static int mi_totalCnt = 0;
	static int mi_tryCnt = 0;
	static int mi_successCnt = 0;
	static int mi_failCnt = 0;
	static int mi_warnCnt =0;
	static int mi_totalDatasourceCnt = 0;
	static int mi_exeDatasourceCnt = 0;
	
	/** 색인실행시간  **/
	static java.util.Date mo_ExecDate;
	
	// localize 정보 : 색인 시 지정하지 않으면 EN으로 설정된다.
	static String localize = Localization.LOCLALIZE_EN;
	
	// 삭제 방법 정보 : 색인 시 지정하지 않으면 API 방식으로 설정된다.
	// API : Content API 를 사용하여 바로 삭제 처리
	// FILE : 삭제 정보 키 데이터를 파일로 저장.
	static String deleteMethod = "API";
	
	/**
	*	프로그램 사용법을 콘솔에 출력한다.
	*/
	public static void printUsage() {
		// Parameter를 지정하지 않으면 프로그램을 종료합니다.
		System.out.println( Localization.getMessage(DelIndexUpdator.class.getName() + ".Console.0001") );
		System.out.println( Localization.getMessage(DelIndexUpdator.class.getName() + ".Console.0002") );
		System.out.println( "" );
		System.exit(1);
	}
	
	@SuppressWarnings("unchecked")
	public static void main( String args[] ) {
		
		List<BulkData> mo_dataList = new ArrayList<BulkData>();
		
		DelIndexUpdator ins = null;
		Map<String, String> keyMap = null;
		int iKeyDataSize = 0;
		
		List<String> listDelKey = new ArrayList<String>();
		
		String sCollectionName = null, sDFName = null;
		String sIndexMode = null;
		
		DFLoader oDFLoader = null;
		File oDFFile = null;
		File oDeleteFile = null;
		String RemoveID = null;
		StringBuffer sbDelete = new StringBuffer("");
		
		mo_ExecDate = new java.util.Date();
		
		try {
			
			// 입력 파라미터에서 localize 관련 값 읽어 들인다.
			loadLocalize(args);
			
			// 입력 파라미터에서 삭제 방식 관련 값 읽어 들인다.
			//loadDeleteMethod(args);
			
			// Parameter 처리
			if (args.length == 5) {
				sIndexMode = args[3];
				sCollectionName = args[4];
				
				sDFName = sCollectionName;
			} else {
				System.out.println( " *** Error Input Data : Unmatch Parameter *** ");
				printUsage();
			}
			
			// 객체 생성 
			ins = DelIndexUpdator.getInstance();
			ins.setCollectionName(sCollectionName);
			
			// index.properits 설정파일 읽기
			ins.loadProperties();
			
			logger = RayfulLogger.getLogger(Bulker.class.getName(), Config.LOG_PATH);
			
			logger.info("======================== " + sDFName + " Delete Start ========================");
			logger.info(" ");
			
			// DF파일 읽기
			String sDFFileName = sDFName + "_deldf.xml";
		  	oDFFile = new File ( Config.DEFINITION_PATH, sDFFileName );
			oDFLoader = new DFLoader( "euc-kr" );
			oDFLoader.setLogger(Config.LOG_PATH);
			oDFLoader.load ( oDFFile.getPath() );
			
			Connection dbConnection = null;
			RdbmsConnector dbConnector  = null;
			RdbmsDataReader dbReader = null;
			int iDatasourceType;
			boolean bIsDefinedGlobalConnect;
			String sDataSourceId = null;
			String sLastDocIndexDate = null;
			KeyData oKeyData = null;
			
			while ( oDFLoader.nextDataSource() ) {
				
				sDataSourceId = oDFLoader.getDataSourceId();
				
				// =============================================================================
				// 특정데이터소스가 상태에 따른 처리
				// - ( state="S" )된 색인대상에서 제외 : false
				// - ( state="N" )된 색인대상 : true
				// =============================================================================
				if (checkDataSourceState(oDFLoader, sDataSourceId) == false) {
					// 해당 DataSource는 색인하지 않는다.
					continue;
				}
				
				bIsDefinedGlobalConnect = oDFLoader.isDefinedGlobalConnect();
				iDatasourceType = oDFLoader.getDataSourceType();
				dbConnector = ( RdbmsConnector ) oDFLoader.getDataSourceConnector(iDatasourceType);
				dbConnection = dbConnector.getConnection();
				sLastDocIndexDate = oDFLoader.getDataSourceLastDocIndexdate();
				//dbReader = ( RdbmsDataReader) oDFLoader.getDataSourceDataReader(iDatasourceType);
				mo_dataReader = ( RdbmsDataReader) oDFLoader.getDataSourceDataReader(iDatasourceType);
				
				initEtcReader(oDFLoader, dbConnection);
				
				if ( ! bIsDefinedGlobalConnect ) {
					iDatasourceType = oDFLoader.getDataSourceType();
					dbConnector = ( RdbmsConnector ) oDFLoader.getDataSourceConnector(iDatasourceType);
					dbConnection = dbConnector.getConnection();					
				}
				oKeyData =  new KeyData();
				
				if(sIndexMode.equalsIgnoreCase("all")) {
					Config.INDEX_MODE = DFLoader.INDEX_ALL;
				} else {
					Config.INDEX_MODE = DFLoader.INDEX_MODIFIED;
				}
				
				//dbReader.setConnection(dbConnection, dbConnector, sDataSourceId);
				//dbReader.executeKeyQuery(oKeyData, -1);
				mo_dataReader.setConnection(dbConnection, dbConnector, sDataSourceId);
				
				// 색인대상 Key Query
				if ( mo_dataReader.getPreQuerySql() != null && mo_dataReader.getPreQuerySql().length() > 0 ) {
					List<String> listPreData = new ArrayList<String>();
					listPreData = mo_dataReader.executePreQuery();
					//oKeyData = new KeyData();
					for(int iPreCnt=0; iPreCnt<listPreData.size(); iPreCnt++) {
						mo_dataReader.setPreConditionValue(listPreData.get(iPreCnt));
						mo_dataReader.executeKeyQuery ( oKeyData, -1 );
					}
					
				} else {
					mo_dataReader.executeKeyQuery(oKeyData, -1);
				}
				
				//bReturn = mo_etcReader.executeEtcQuery(oDataMap, oResultMap);

				iKeyDataSize = oKeyData.size();
				
				// Json 관련 ...
				BulkData bulkData = null;
				
				if ( iKeyDataSize > 0 ) {
					
					boolean bReturn = false;
					
					String sCurrentTime = ESPTypes.dateFormatString( new java.util.Date(), "yyyyMMddHHmmss.S");
					//oDeleteFile = new File( Config.RESULT_PATH, sDFName + "_" + sCurrentTime + "_del.txt" );
					oDeleteFile = new File( Config.RESULT_PATH, "del.json" );
					
					// key 목록 수 만큼 looping
					for ( int i=0; i < iKeyDataSize; i++ ) {
						
						bulkData = new BulkData(BulkData.OP_DELETE);
						
						keyMap = oKeyData.get(i);
						
						Map<String, Object> oDataMap = new HashMap<String, Object>();
						Map<String, Object> oResultMap = new HashMap<String, Object>();
						
						if (keyMap != null) {
							
							if( oResultMap != null && oResultMap.size() > 0 ) {
								listDelKey = (ArrayList<String>)oResultMap.get("KEYLIST");
								//RemoveID = (String)oResultMap.get("KEY");
							} else {
								listDelKey = new ArrayList<String>();
								listDelKey.add(keyMap.get("ID"));
							}
							
						} else {
							RemoveID = "";
							listDelKey = new ArrayList<String>();
						}
						
						if(listDelKey != null && listDelKey.size() > 0 ) {
							
							for(int iLoop=0; iLoop<listDelKey.size(); iLoop++) { 
								RemoveID = listDelKey.get(iLoop);
								
								if (RemoveID != null && RemoveID.trim().length() > 0) {
									bulkData.setIndexId(RemoveID);  
								}
								
								logger.info("Delete ID:" + RemoveID);
							}
						}
						
						mo_dataList.add(bulkData);
					} // for
					
					logger.info("Total Delete ID Count:" + iKeyDataSize);
					
					OutputStream os = null;
					
					try {
						os = new FileOutputStream( oDeleteFile.getPath() );
						
						int listSize = mo_dataList.size();
						for(int index =0; index <listSize; index++) {
							mo_dataList.get(index).toJson(os);
						}

					} catch ( Exception  e ) {
						logger.error( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0002" ), e);
					} finally {
						if ( os != null ) try { os.close(); } catch( Exception e) {}
					}
				}

				// 현재 수행 시간을  기록. (아직은 실제 DF파일에 저장되진 않음)
				ins.writeIndexDate( oDFLoader );
				
				// DF에 최종 조회수 수행 시간을 기록 (아직은 실제 DF파일에 저장되진 않음)
				ins.writeDoccountIndexDate( oDFLoader );
				
			} // while
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			
			if ( oDFLoader != null ) {
				oDFLoader.save( oDFFile.getPath() );
			}
		}
		
		logger.info(" ");
		logger.info("======================== " + sDFName + " Delete Finish ========================");
		
		System.exit(0);
	}
	
	/**
	 * DF에 명시된 DtatSource의 상태를 체크한다. ( state="D" )된 경우 그 데이터소스만 삭제처리한다. (색인대상제외)
	 * false 리턴 ( state="C" )된 색인대상에서 제외 false 리턴 ( state="S" )된 색인대상에서 제외 false
	 * 리턴 ( state="N" )된 색인대상 ture 리턴
	 * <p>
	 * 
	 * @param oDFLoader
	 *            DF로더객체
	 * @param sDataSourceId
	 *            특정데이터소스ID
	 * @param oSSWriter
	 *            색인수행객체
	 * @return true: 색인대상/ false: 색인대상아님
	 */
	private static boolean checkDataSourceState(DFLoader oDFLoader,
			String sDataSourceId) throws DFLoaderException, SQLException,
			IOException {
		// 데이터소스의 상태를 알아낸다.
		String sDataSourceState = oDFLoader.getDataSourceState();

		if (sDataSourceState.equals("N")) {
			return true;
		} else if (sDataSourceState.equals("S")) {
			// 이 데이터소스는 색인하지 않는다.
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0017"));
			return false;
		} else {
			// 이 데이터소스는 색인하지 않는다.
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0018"));
			return false;
		}
	}
	
	public static DelIndexUpdator getInstance() {
		if ( instance == null ) {
			instance = new DelIndexUpdator();
		}
		return instance;
	}

	public void setCollectionName( String sCollectionName ) {
		this.collectionName = sCollectionName;
	}
	
	public String getKeyString( Map<String, String> keyMap, String[] keys, String sGubun ) {
		int keySize = keys.length;
		StringBuffer sbKey = new StringBuffer();
		
		for ( int i=0; i< keySize; i++) {
			if ( i!= 0 ) {
				sbKey.append( sGubun );
			}
			sbKey.append( keyMap.get(keys[i]));
		}
		
		return sbKey.toString();
	}
	
	/**
	* Indexer.properites 로 부터 설정정보를 읽어 Config객에에 세팅.
	*/		
	public void loadProperties()
	{
		try {
			//String sPropFileName = "Indexer.properties";
			String sPropFileName = "Indexer.xml";
		  	
		  	ClassLoader cl;
	        cl = Thread.currentThread().getContextClassLoader();
	        if( cl == null )
	            cl = ClassLoader.getSystemClassLoader();                
			
		  	java.net.URL oUrl = cl.getResource( sPropFileName );
		  	
		  	//java.net.URL oUrl = Bulker.class.getResource( sPropFileName );
		  	
		  	if ( oUrl == null ) {
		  		throw new FileNotFoundException( Localization.getMessage(DelIndexUpdator.class.getName() + ".Exception.0002", sPropFileName) );
		  	}
		  	
		  	sPropFileName = oUrl.getPath();
		  	Config.load( sPropFileName, this.collectionName );
		} catch( FileNotFoundException fnfe ) {
		  	System.out.println (fnfe);
		  	printUsage();
		} catch( IOException ioe ) {
		  	System.out.println (ioe);
		  	printUsage();
		} catch (Exception e) {
			System.out.println (e);
		  	printUsage();
		}
	}
	
	/** 
	* DF에 문서 최종색인일을 기록한다.
	* @param oDFLoader DFLoader객체
	* @throws DFLoaderException
	*/		
	private void writeIndexDate( DFLoader oDFLoader )
	throws DFLoaderException
	{
		if ( Config.KEYQUERY_METHOD != Config.KEY_ERROR ) {	
			//String sLastDocIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss" );
			String sLastDocIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss", oDFLoader.getTimeZone());
			
			oDFLoader.setDataSourceLastDocIndexdate( sLastDocIndexDate );
		} else {
			logger.info( Localization.getMessage(DelIndexUpdator.class.getName() + ".Logger.0001") );
		}
	}
	
	/** 
	* DF에 조회수 최종색인일을 기록한다.
	* @param oDFLoader DFLoader객체
	* @throws DFLoaderException	
	*/		
	private void writeDoccountIndexDate( DFLoader oDFLoader )
	throws DFLoaderException
	{	
		if ( Config.KEYQUERY_METHOD != Config.KEY_ERROR ) {	
			//String sLastDoccountIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss" );
			String sLastDoccountIndexDate = ESPTypes.dateFormatString( mo_ExecDate, "yyyy-MM-dd HH:mm:ss", oDFLoader.getTimeZone() );
			oDFLoader.setDataSourceLastDoccountIndexdate( sLastDoccountIndexDate );
		} else {
			logger.info( Localization.getMessage(DelIndexUpdator.class.getName() + ".Logger.0002") );
		}
	}
	
	/**
	 * LogBudle_<Language>.properties 로 부터 로그 메세지를 읽어 Localization 객체에 할당.
	 */
	private static void loadLocalize(String[] aParameter) {
		String sCommand = null;
		String sParameterValue = null;

		try {

			try {
				for (int i = 0; i < aParameter.length - 1; i ++) {
					sCommand = aParameter[i].toLowerCase();
					sParameterValue = aParameter[i + 1].toLowerCase();

					if (sCommand.equals("-localize")) {
						localize = sParameterValue;
						break;
					}
				}
			} catch (Exception ae) {
			}

			ClassLoader cl;
			cl = Thread.currentThread().getContextClassLoader();
			if (cl == null)
				cl = ClassLoader.getSystemClassLoader();

			String sPropFileName = "LogBundle_" + localize.toUpperCase()
					+ ".properties";
			java.net.URL oUrl = cl.getResource(sPropFileName);

			// java.net.URL oUrl = Bulker.class.getResource( sPropFileName );

			if (oUrl == null) {
				throw new FileNotFoundException(Localization.getMessage(
						Bulker.class.getName() + ".Exception.0005", localize));
			}

			sPropFileName = oUrl.getPath();
			Localization.load(sPropFileName);

		} catch (FileNotFoundException fnfe) {
			System.out.println(fnfe);
			// printUsage();
		} catch (IOException ioe) {
			System.out.println(ioe);
			// printUsage();
		} catch (Exception e) {
			System.out.println(e);
			// printUsage();
		}

	}
	
	/**
	 * LogBudle_<Language>.properties 로 부터 로그 메세지를 읽어 Localization 객체에 할당.
	 */
	private static void loadDeleteMethod(String[] aParameter) {
		String sCommand = null;
		String sParameterValue = null;

		try {

			try {
				for (int i = 0; i < aParameter.length - 1; i ++) {
					sCommand = aParameter[i].toLowerCase();
					sParameterValue = aParameter[i + 1].toLowerCase();

					if (sCommand.equals("-m")) {
						deleteMethod = sParameterValue;
						break;
					}
				}
			} catch (Exception ae) {
			}

		} catch (Exception e) {
			System.out.println(e);
			// printUsage();
		}

	}
	
	/** 
	* EtcReader객체를 세팅한다.
	* ( mo_etcReader )
	*/		
	@SuppressWarnings("unchecked")
	private static void initEtcReader(DFLoader oDFLoader, Connection dbConnection)
	throws Exception
	{
		Map mapEtcQuery = mo_dataReader.getEtcQuerySql();
		
		// 추가조회객체 클래스명을 알아낸다.
		ms_etcReaderClassName = oDFLoader.getDataSourceEtcReaderClassName();
		
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
			
			aConParamValues[0] = dbConnection;
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
	
}

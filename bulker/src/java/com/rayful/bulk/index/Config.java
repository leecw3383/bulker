/**
 *******************************************************************
 * 파일명 : Config.java
 * 파일설명 : Properties(또는 xml) 파일로부터 설정정보를 읽어들이는 클래스를 정의
 *******************************************************************
 * 작성일자		작성자		내용
 * -----------------------------------------------------------------
 * 2005/05/30	정충열		최초작성
 * 2010/12/10	opensky		ESP / Notes / Time 설정 추가
 * 2011/01/25	opensky		xml 설정 추가
 *******************************************************************
*/
package com.rayful.bulk.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.rayful.localize.Localization;

/**
 * Indexer.Properties(또는 Indexer.xml) 파일로부터 설정 정보를 읽어들인다.
*/
public class Config
{
	/** Collection Name */
	public static String ESP_COLLECTION_NAME = null;
	/** 색인모드 */
	public static int INDEX_MODE = 0;	
	/** 색인일 */
	public static Date INDEX_DATE = new Date();	
	
	/** 정의(Definition)파일의 경로 및 이름 */
	public static String DEFINITION_PATH = null;
	/** 로그파일의 경로 및 이름 */
	public static String LOG_PATH = null;
	/** 삭제로그파일의 경로 및 이름 */
	public static String REMOVE_LOG_PATH = null;
	/** key 관리 파일의 경로 및 이름 */
	public static String KEY_INDEX_PATH = null;
	
	/** 락파일 경로 및 이름 */
	public static String LOCKFILE_PATH = null;	
	/** 결과파일 경로 */
	public static String RESULT_PATH = null;		
	/** 로그파일의 경로*/
	public static String SUMMARYLOG_PATH = null;
	/** 임시다운로드파일의 경로 및 이름 */
	public static String DOWNLOAD_PATH = null;
	/** ERROR 파일경로  */
	public static String ERROR_FILE_PATH = null;
	/** WARNNING 파일경로  */
	public static String  WARN_FILE_PATH= null;
	/** EtcReader 파일경로 */
	public static String  ETC_READER_PATH = null;
	
	/** ESP Content Distributor Server */
	public static String ESP_CONTENT_SERVER = null;
	/** ESP Content Distributor Port */
	public static String ESP_CONTENT_PORT = null;
	/** ESP QR Server */
	public static String ESP_QR_SERVER = null;
	/** ESP QR Port */
	public static String ESP_QR_PORT = null;
	
	/** ESP Collection */
	public static String ESP_CONFIG_COLLECTION = null;
	/** ESP Search View */
	public static String ESP_CONFIG_SEARCHVIEW = null;
	
	
	/** KeyQuery 방식  */
	public static int KEYQUERY_METHOD = Config.KEY_DBQUERY;
	
	/** key Query 방식 : 상수 */
	public final static int KEY_ALL = 1;
	public final static int KEY_DBQUERY = 2;
	public final static int KEY_ERROR = 3;
	
	/** Meta DB 서버 정보 */
	public static String METADB_DRIVERCLASS = null;
	public static String METADB_URL = null;
	public static String METADB_USER = null;
	public static String METADB_PASS = null;
	
	/** Notes 서버 정보 */
	public static String NOTES_SERVER = null;
	public static String NOTES_USER = null;
	public static String NOTES_PASS = null;
	
	/** Time Zone */
	public static String TIME_ZONE = null;
	
	/** xml Document */
	static Document mo_document;			// xml 문서를 가리키는 포인터
	
	int iLevel = 0;
	
	private static Config instance = null;
	

	public static Config getInstance() {
		if ( instance == null ) {
			instance = new Config();
		}
		return instance;
	}
	
	/**
	* properties파일로 부터 sKey에 해당하는 설정내용을 읽어들인다. 
	* <p>
	* @param	oProperties	Properties객체
	* @param	sKey	Properties내 Key값
	* @return	sKey에 대응되는 값
	*/		
	private static String getProperty( Properties oProperties, String sKey ) {
		String sRtnValue = null;
		
		if ( oProperties != null )  {
			sRtnValue = oProperties.getProperty( sKey, "" );
		}

		if ( sRtnValue != null ) {
			return sRtnValue;
		} else {
			return null;
		}
	}
	
//	/**
//	* properties 중  sKey에 해당하는 Property에 sValue를 설정
//	* <p>
//	* @param	oProperties	Properties객체
//	* @param	sKey	Properties내 Key
//	* @param	sValue	Properties Value
//	*/		
//	private static void setProperty( Properties oProperties, String sKey, String sValue ) {
//		
//		if ( oProperties != null )  {
//			oProperties.setProperty( sKey, sValue );
//		}
//	}
	
	/**
	* 경로스트링중에 "[%INDEX_NAME%]"스트링을 색인테이블명으로 변경
	* <p>
	* @param	sPathValue	경로스트링
	* @return	("[%INDEX_NAME%]"=>색인테이블명)으로 변경된 경로스트링
	*/	
	private static String replaceIndexName ( String sPathValue )
	{
		String sRtnString = null;
		String sRegularExp = null;
		String sIndexDate = null;
		
		if ( ESP_COLLECTION_NAME != null &&  sPathValue != null ) {
			sRegularExp = "\\[%INDEX_NAME%\\]";
			sRtnString = sPathValue.replaceAll( sRegularExp, ESP_COLLECTION_NAME );
			
			sIndexDate = com.rayful.bulk.ESPTypes.dateFormatString( Config.INDEX_DATE, "yyyyMMdd" );
			sRegularExp = "\\[%INDEX_DATE%\\]";	
			sRtnString = sRtnString.replaceAll( sRegularExp, sIndexDate );
		
		} else {
			sRtnString = sPathValue;
		}
		return sRtnString;
	}
	
	
	/**
	* Properties 파일로부터 설정값을 읽어들여 Config 클래스내 
	* 변수에 설정값을 세팅한다.
	* <p>
	* @param	sPropFileName		Properties 파일이 있는 경로명
	* @param	sESPCollectionName	Collection Name
	*/		
	public static void load( String sPropFileName,  String sESPCollectionName )
	throws FileNotFoundException, IOException, Exception
	{
		if (sPropFileName == null || sPropFileName.trim().length() < 1)
			throw new Exception("Properties File name is null");
		
		String aPropFileName[] = sPropFileName.split("\\.");
		
		if ( aPropFileName.length > 0 ) {
			
			// 서치서버명을 세팅 
			ESP_COLLECTION_NAME = sESPCollectionName;
			
			if ("properties".equalsIgnoreCase(aPropFileName[aPropFileName.length - 1])) {
				loadProperties(sPropFileName);
			} else if ("xml".equalsIgnoreCase(aPropFileName[aPropFileName.length - 1])) {
				loadXml(sPropFileName);
			} else {
				throw new Exception("Invalid Properties File : " + sPropFileName);
			}
		} else {
			throw new Exception("Invalid Properties File : " + sPropFileName );
		}
		
	}
	
	/**
	* Properties 파일로부터 설정값을 읽어들여 Config 클래스내 
	* 변수에 설정값을 세팅한다.
	* <p>
	* @param	sPropFileName		Properties 파일이 있는 경로명
	* @param	sESPCollectionName	Collection Name
	*/		
	public static void loadProperties( String sPropFileName )
	throws FileNotFoundException, IOException, Exception
	{
			
		// 나머지변수를 세팅하기 위해 properties파일을 로드
		FileInputStream oFileInputStream = null;
		Properties oProperties = null;

		oFileInputStream = new FileInputStream( sPropFileName );
		oProperties =  new Properties();
		oProperties.load( oFileInputStream );
		oFileInputStream.close();
			
		readProperty(oProperties);
	}
	
	private static void readProperty(Properties oProperties)
	throws FileNotFoundException, IOException, Exception
	{
		try {
			// 파일경로정보
			DEFINITION_PATH 	= replaceIndexName (getProperty( oProperties, "PATH.DEFINITION" ));
			LOG_PATH 			= replaceIndexName (getProperty( oProperties, "PATH.LOG" ));
			RESULT_PATH			= replaceIndexName (getProperty( oProperties, "PATH.RESULT" ));
			LOCKFILE_PATH 		= replaceIndexName (getProperty( oProperties, "PATH.LOCKFILE" ));
			DOWNLOAD_PATH 		= replaceIndexName (getProperty( oProperties, "PATH.DOWNLOAD" ));
			ERROR_FILE_PATH		= replaceIndexName (getProperty( oProperties, "PATH.ERROR" ));
			WARN_FILE_PATH		= replaceIndexName (getProperty( oProperties, "PATH.WARN" ));
			ETC_READER_PATH		= replaceIndexName (getProperty( oProperties, "PATH.ETCREADER" ));
			KEY_INDEX_PATH		= replaceIndexName (getProperty( oProperties, "PATH.KEYINDEX" ));
			
			METADB_DRIVERCLASS  = getProperty( oProperties, "METADB.DRIVERCLASS" );
			METADB_URL          = getProperty( oProperties, "METADB.URL" );
			METADB_USER         = getProperty( oProperties, "METADB.USER" );
			METADB_PASS         = getProperty( oProperties, "METADB.PASS" );
			//METADB_PASS         = Crypto.deCrypt(getProperty( oProperties, "METADB.PASS" ));
			
			ESP_CONTENT_SERVER  = replaceIndexName (getProperty( oProperties, "ESP.CONTENT.SERVER" ));
			ESP_CONTENT_PORT    = replaceIndexName (getProperty( oProperties, "ESP.CONTENT.PORT" ));
			ESP_QR_SERVER  = replaceIndexName (getProperty( oProperties, "ESP.QR.SERVER" ));
			ESP_QR_PORT    = replaceIndexName (getProperty( oProperties, "ESP.QR.PORT" ));
			ESP_CONFIG_COLLECTION		= replaceIndexName (getProperty( oProperties, "ESP.COLLECTION" ));
			ESP_CONFIG_SEARCHVIEW    	= replaceIndexName (getProperty( oProperties, "ESP.SEARCHVIEW" ));
			
			NOTES_SERVER  		= getProperty( oProperties, "NOTES.SERVER" );
			NOTES_USER    		= getProperty( oProperties, "NOTES.USER" );
			NOTES_PASS			= getProperty( oProperties, "NOTES.PASS" );
			
			TIME_ZONE			= getProperty( oProperties, "TIMEZONE" );
			
			//=============================================================================
			// 색인시 필요한 경로 ( 로그파일경로 / 본문파일저장경로/ 다운로드 임시저장경로 ) 
			//=============================================================================		
			File oLogPath = new File ( LOG_PATH );
			if ( ! oLogPath.exists()  ) {
				if ( oLogPath.mkdirs() == false ) {
					throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0001" ) );
				} 
			}
			SUMMARYLOG_PATH = LOG_PATH;
			
			File oLogFile = new File ( Config.LOG_PATH, "IDX_" + Config.ESP_COLLECTION_NAME + ".log" );
			Config.LOG_PATH = oLogFile.getPath();
			
			File oLocckFilePath = new File ( Config.LOCKFILE_PATH );
			if ( ! oLocckFilePath.exists()  ) {
				if ( oLocckFilePath.mkdirs() == false ) {
					throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0002" ) );
				}
			}
			
			File oResultPath = new File ( Config.RESULT_PATH );
			if ( ! oResultPath.exists()  ) {
				if ( oResultPath.mkdirs() == false ) {
					throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0003" ) );
				}
			}				
			
			File oDownloadPath = new File ( Config.DOWNLOAD_PATH );
			if ( ! oDownloadPath.exists()  ) {
				if ( oDownloadPath.mkdirs() == false ) {
					throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0004" ) );
				}
			}			
			
			if ( Config.ERROR_FILE_PATH.length() > 0 ) {
				File oErrorFilePath = new File ( Config.ERROR_FILE_PATH );
				if ( ! oErrorFilePath.exists()  ) {
					if ( oErrorFilePath.mkdirs() == false ) {
						throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0005" ) );
					}
				}
			}
			
			if ( Config.WARN_FILE_PATH.length() > 0 ) {
				File oErrorFilePath = new File ( Config.WARN_FILE_PATH );
				if ( ! oErrorFilePath.exists()  ) {
					if ( oErrorFilePath.mkdirs() == false ) {
						throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0006" ) );
					}
				}
			}
			
			if ( Config.KEY_INDEX_PATH.length() > 0 ) {
				File oIndexFilePath = new File ( Config.KEY_INDEX_PATH );
				if ( ! oIndexFilePath.exists()  ) {
					if ( oIndexFilePath.mkdirs() == false ) {
						throw new IOException( Localization.getMessage( Config.class.getName() + ".Exception.0007" ) );
					}
				}
			}		
		} catch ( IOException ioe ) {
			throw ioe;
		} catch ( Exception e ) {
			throw e;
		} 
//		finally {
//			if ( oFileInputStream != null ) {
//				try {
//					oFileInputStream.close();
//				} catch ( IOException ioe ) {
//					throw ioe;
//				}
//			}
//		}
	}
	
	/**
	* xml 파일로부터 설정값을 읽어들여 Config 클래스내 
	* 변수에 설정값을 세팅한다.
	* <p>
	* @param	sPropFileName		Properties 파일이 있는 경로명
	* @param	sESPCollectionName	Collection Name
	*/		
	private static void loadXml( String sPropFileName )
	throws FileNotFoundException, IOException, Exception
	{
		DocumentBuilderFactory oFactory = null;
		DocumentBuilder oBuilder = null;
		
		String sErrMsg = null;
		
		try {
			oFactory = DocumentBuilderFactory.newInstance();
			oBuilder = oFactory.newDocumentBuilder();
		} catch ( ParserConfigurationException pce ) {
			sErrMsg = "Creating error Config Builder.";
			System.out.println( sErrMsg );
			throw new Exception( sErrMsg );
		} 
		
		try {
			mo_document = oBuilder.parse( new File(sPropFileName) );
		} catch (SAXException sxe) {
			sErrMsg = "Config Parsing error.";
			System.out.println( sErrMsg );
			throw new Exception( sErrMsg );

		} catch (IOException ioe) {
			// I/O error
			sErrMsg = "Could not find DF file.";
			System.out.println( sErrMsg );
			throw new Exception( sErrMsg );
		}
		
		Properties oProperties =  new Properties();
		
		NodeList nodeList = mo_document.getElementsByTagName("config");
		
        Node node = nodeList.item(0);

        Config cfg = Config.getInstance();
        
        cfg.parsingNode(node, "", oProperties);
        
        readProperty(oProperties);
	}
	
	public void parsingNode(Node node, String sPath, Properties oProperties)
	{
		String sNodePath = sPath;
        String sNodeName = "";
        
        NodeList nodeList1 = node.getChildNodes();
        
        this.iLevel += 1;
        
        for(int iLoop=0; iLoop<nodeList1.getLength(); iLoop++) {
        	Node node1 = nodeList1.item(iLoop);
        	
        	if ( node1.getNodeType() == Node.ELEMENT_NODE ) {
        		if ( node1.getChildNodes().getLength() == 1 ) {
        			if (this.iLevel == 1) 
        				sNodeName = node1.getNodeName();
        			else {
//        				String aNodePath[] = sNodePath.split("\\.");
//        				//int iMax = this.iLevel > aNodePath.length ? aNodePath.length : this.iLevel;
//						int iMax = 0;
//        				
//        				if ( this.iLevel > aNodePath.length )
//        					iMax = aNodePath.length;
//        				else if ( this.iLevel == aNodePath.length )
//        					iMax = aNodePath.length - 1;
//        				else
//        					iMax = this.iLevel;
//        				
//        				//System.out.println("Max : " + iMax + " / Level : " + this.iLevel);
//        				for(int jLoop=0; jLoop<iMax; jLoop++) {
//        					if ( jLoop==0 )
//        						sNodePath = aNodePath[jLoop];
//        					else 
//        						sNodePath = sNodePath + "." + aNodePath[jLoop];
//        				}
//        				sNodeName = sNodePath + "." + node1.getNodeName();
        				sNodePath = getNodePath(sNodePath);
        				sNodeName = sNodePath + "." + node1.getNodeName();
        			}
        			
        			System.out.println("Level : " + this.iLevel + " / NodeName() : " + sNodeName
	        				+ " / Value : " + node1.getTextContent());
        			
        			oProperties.setProperty(sNodeName.toUpperCase(), node1.getTextContent());
        		} else {
    				if ( sNodePath.trim().length() == 0 )
    					sNodePath = node1.getNodeName();
    				else {
    					if ( sNodePath.split("\\.").length == this.iLevel ) {
    						sNodePath = node1.getNodeName();
    					}
    					else {
    						if ( this.iLevel == 1 ) {
    							sNodePath = node1.getNodeName();
    						} else {
    							
//    							//sNodePath = "";
//    							String aNodePath[] = sNodePath.split("\\.");
//    	        				//int iMax = this.iLevel > aNodePath.length ? aNodePath.length : this.iLevel;
//    							int iMax = 0;
//    	        				
//    	        				if ( this.iLevel > aNodePath.length )
//    	        					iMax = aNodePath.length;
//    	        				else if ( this.iLevel == aNodePath.length )
//    	        					iMax = aNodePath.length - 1;
//    	        				else
//    	        					iMax = this.iLevel;
//    	        				
//    	        				//System.out.println("Max1 : " + iMax + " / Level1 : " + this.iLevel);
//    	        				for(int jLoop=0; jLoop<iMax; jLoop++) {
//    	        					if ( jLoop==0 )
//    	        						sNodePath = aNodePath[jLoop];
//    	        					else 
//    	        						sNodePath = sNodePath + "." + aNodePath[jLoop];
//    	        				}
//    	        				
//    							sNodePath = sNodePath + "." + node1.getNodeName();
    							
    							sNodePath = getNodePath(sNodePath);
    							sNodePath = sNodePath + "." + node1.getNodeName();
    						}
    					}
    				}
    				parsingNode(node1, sNodePath, oProperties);
        		}
        		
        		//this.iLevel = parsingNode(node1, sNodePath, this.iLevel, oProperties);
        	} 
        }
        this.iLevel -= 1;
	}
	
	public String getNodePath(String pNodePath)
	{
		String sNodePath = "";
		
		String aNodePath[] = pNodePath.split("\\.");
		//int iMax = this.iLevel > aNodePath.length ? aNodePath.length : this.iLevel;
		int iMax = 0;
		
		if ( this.iLevel > aNodePath.length )
			iMax = aNodePath.length;
		else if ( this.iLevel == aNodePath.length )
			iMax = aNodePath.length - 1;
		else
			iMax = this.iLevel;
		
		//System.out.println("Max : " + iMax + " / Level : " + this.iLevel);
		for(int jLoop=0; jLoop<iMax; jLoop++) {
			if ( jLoop==0 )
				sNodePath = aNodePath[jLoop];
			else 
				sNodePath = sNodePath + "." + aNodePath[jLoop];
		}
		//sNodeName = sNodePath + "." + node1.getNodeName();
		
		return sNodePath;
	}
	
	public static String getNodeTypeString(Node node)
	{
		String nodeType = "UNKNOWN_NODE_TYPE";
	 
		switch(node.getNodeType()) {
			case Node.ATTRIBUTE_NODE:
				nodeType = "ATTRIBUTE_NODE";
				break;
			case Node.CDATA_SECTION_NODE:
				nodeType = "CDATA_SECTION_NODE";
				break;
			case Node.COMMENT_NODE:
				nodeType = "COMMENT_NODE";
				break;
			case Node.DOCUMENT_FRAGMENT_NODE:
				nodeType = "DOCUMENT_FRAGMENT_NODE";
				break;
			case Node.DOCUMENT_NODE:
				nodeType = "DOCUMENT_NODE";
				break;
			case Node.DOCUMENT_TYPE_NODE:
				nodeType = "DOCUMENT_TYPE_NODE";
				break;
			case Node.ELEMENT_NODE:
				nodeType = "ELEMENT_NODE";
				break;
			case Node.ENTITY_NODE:
				nodeType = "ENTITY_NODE";
				break;
			case Node.ENTITY_REFERENCE_NODE:
				nodeType = "ENTITY_REFERENCE_NODE";
				break;
			case Node.NOTATION_NODE:
				nodeType = "NOTATION_NODE";
				break;
			case Node.PROCESSING_INSTRUCTION_NODE:
				nodeType = "PROCESSING_INSTRUCTION_NODE";
				break;
			case Node.TEXT_NODE:
				nodeType = "TEXT_NODE";
				break;
		}

		return nodeType;
	}
	
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.<p>
	* static 함수이므로 ToString을 Override할수 없어 toText로 함수이름을 사용
	* <p>
	* @return	객체의 멤버변수내용을 담고있는 String
	*/	
	public static String toText() {
		StringBuffer oSb = new StringBuffer( "ClassName:Config {\r\n" );
		
		oSb.append( "DEFINITION_PATH=" );
		oSb.append( DEFINITION_PATH ).append( "\r\n");
		
		oSb.append( "LOG_PATH=" );
		oSb.append( LOG_PATH ).append( "\r\n");
		
		oSb.append( "LOCKFILE_PATH=" );
		oSb.append( LOCKFILE_PATH ).append( "\r\n");
		
		oSb.append( "RESULT_PATH=" );
		oSb.append( RESULT_PATH ).append( "\r\n");
		
		oSb.append( "SUMMARYLOG_PATH=" );
		oSb.append( SUMMARYLOG_PATH ).append( "\r\n");			
		
		oSb.append( "DOWNLOAD_PATH=" );
		oSb.append( DOWNLOAD_PATH ).append( "\r\n");

		oSb.append( "ERROR_FILE_PATH=" );
		oSb.append( ERROR_FILE_PATH ).append( "\r\n");			

		oSb.append( "WARN_FILE_PATH=" );
		oSb.append( WARN_FILE_PATH ).append( "\r\n");	
		
		oSb.append( "KEY_INDEX_PATH=" );
		oSb.append( KEY_INDEX_PATH ).append( "\r\n");

		oSb.append( "KEYQUERY_METHOD=" );
		oSb.append( KEYQUERY_METHOD ).append( "\r\n");	
		
		oSb.append( "METADB_DRIVERCLASS=" );
		oSb.append( METADB_DRIVERCLASS ).append( "\r\n");
		
		oSb.append( "METADB_URL=" );
		oSb.append( METADB_URL ).append( "\r\n");
		
		oSb.append( "METADB_USER=" );
		oSb.append( METADB_USER ).append( "\r\n");
		
		oSb.append( "METADB_PASS=" );
		oSb.append( METADB_PASS ).append( "\r\n");
		
		if ( ESP_CONTENT_SERVER != null && ESP_CONTENT_SERVER.trim().length() > 0 ) {
			oSb.append( "ESP_CONTENT_SEVER=" );
			oSb.append( ESP_CONTENT_SERVER ).append( "\r\n");
		}
		
		if ( ESP_CONTENT_PORT != null && ESP_CONTENT_PORT.trim().length() > 0 ) {
			oSb.append( "ESP_CONTENT_PORT=" );
			oSb.append( ESP_CONTENT_PORT ).append( "\r\n");
		}
		
		if ( ESP_QR_SERVER != null && ESP_QR_SERVER.trim().length() > 0 ) {
			oSb.append( "ESP_QR_SEVER=" );
			oSb.append( ESP_QR_SERVER ).append( "\r\n");
		}
		
		if ( ESP_QR_PORT != null && ESP_QR_PORT.trim().length() > 0 ) {
			oSb.append( "ESP_QR_PORT=" );
			oSb.append( ESP_QR_PORT ).append( "\r\n");
		}
		
		if ( ESP_CONFIG_COLLECTION != null && ESP_CONFIG_COLLECTION.trim().length() > 0 ) {
			oSb.append( "ESP_CONFIG_COLLECTION=" );
			oSb.append( ESP_CONFIG_COLLECTION ).append( "\r\n");
		}
		
		if ( ESP_CONFIG_SEARCHVIEW != null && ESP_CONFIG_SEARCHVIEW.trim().length() > 0 ) {
			oSb.append( "ESP_CONFIG_SEARCHVIEW=" );
			oSb.append( ESP_CONFIG_SEARCHVIEW ).append( "\r\n");
		}
		
		if ( NOTES_SERVER != null && NOTES_SERVER.trim().length() > 0 ) {
			oSb.append( "NOTES_SERVER=" );
			oSb.append( NOTES_SERVER ).append( "\r\n");
		}
		
		if ( NOTES_USER != null && NOTES_USER.trim().length() > 0 ) {
			oSb.append( "NOTES_USER=" );
			oSb.append( NOTES_USER ).append( "\r\n");
		}
		
		if ( NOTES_PASS != null && NOTES_PASS.trim().length() > 0 ) {
			oSb.append( "NOTES_PASS=" );
			oSb.append( NOTES_PASS ).append( "\r\n");
		}
		
		if ( TIME_ZONE != null && TIME_ZONE.trim().length() > 0 ) {
			oSb.append( "TIME_ZONE=" );
			oSb.append( TIME_ZONE ).append( "\r\n");
		}
		
		return oSb.toString();
	}
}
/**
 *******************************************************************
 * 파일명 : DFLoader.java
 * 파일설명 : 색인정의파일(xml)을 읽어들여 색인정보를 읽어들이는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/25   정충열    최초작성      
 * 2005/05/19   정충열     FileAccessor 추가
 * 2011/06/13	주현필       timestamp columns 로직 제외
 *******************************************************************
*/

package com.rayful.bulk.index;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.io.FileAccessor;
import com.rayful.bulk.io.FilesAccessor;
import com.rayful.bulk.io.FtpAccessor;
import com.rayful.bulk.io.HttpAccessor;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.JdbcConnector;
import com.rayful.bulk.sql.NotesConnector;
import com.rayful.bulk.sql.NotesDataReader;
import com.rayful.bulk.sql.RdbmsConnector;
import com.rayful.bulk.sql.RdbmsDataReader;
import com.rayful.bulk.util.Utils;
import com.rayful.localize.Localization;


/**
 * 색인정보파일(xml)을 로드하여 색인정보를 읽어들이는 클래스
*/
public class DFLoader {
	
	
	/** 색인방식 : 알 수 없는 색인방식 */
	public static final int INDEX_NONE = 0;
	/** 색인방식 : 기존색인을 지우고 다시 모두 재색인 */
	public static final int INDEX_REINDEX = 1;
	/** 색인방식 : 기존색인을 지우지 않고 모두 재색인 */
	public static final int INDEX_ALL = 2;
	/** 색인방식 : 삭제, 수정되거나 추가된 건에 한해 색인 */
	public static final int INDEX_MODIFIED = 3;
	
	/** datasource 커서상태 : datasource의 맨처음, 아직 datasource를 가리키지 못함 */
	public static final int DATASOURCE_CURSOR_BOF = 1;
	/** datasource 커서상태 : 정상, datasource를 가리키고 있음 **/
	public static final int DATASOURCE_CURSOR_NORMAL = 2;
	/** datasource 커서상태 : datasource의 끝,  datasource를 가리키지 않음 **/
	public static final int DATASOURCE_CURSOR_EOF = 3;
	
	/** datasource 정보유형 : connect 정보 **/
	public static final int DATASOURCE_INFO_CONNECT = 1;
	/** datasource 정보유형 : query 정보 **/
	public static final int DATASOURCE_INFO_QUERY = 2;
	/** datasource 정보유형 : file acess 정보 **/
	public static final int DATASOURCE_INFO_FILEACCESS = 3;
	/** datasource 정보유형 : column matcing 정보 **/
	public static final int DATASOURCE_INFO_COLUMNMATCHING = 4;
	/** datasource 정보유형 : indexer class 정보 **/
	public static final int DATASOURCE_INFO_INDEXER = 5;
	/** datasource 정보유형 : indexer class 정보 **/
	public static final int DATASOURCE_INFO_CUSTOMIZER = 6;	
	/** datasource 정보유형 : datreader class 정보 **/
	public static final int DATASOURCE_INFO_DATAREADER = 7;	
	/** datasource 정보유형 : datreader class 정보 **/
	public static final int DATASOURCE_INFO_ETCREADER = 8;		
	
	
	/** datasource type : UNKNOWN **/
	public static final int DATASOURCE_TYPE_UNKNOWN = 0;
	/** datasource type : RDBMS **/
	public static final int DATASOURCE_TYPE_RDBMS = 1;
	/** datasource type : Notes System **/
	public static final int DATASOURCE_TYPE_NOTES = 2;
	
	//static Logger logger = YessLogger.getLogger( DFLoader.class.getName(), Config.LOG_PATH );
	static Logger logger = null;
	
	Document mo_document;			// xml 문서를 가리키는 포인터
	Node mo_dataSourceCursor;		// data source를 가리키는 포인터
	int mi_cursorState; 			// data source cursor의 상태
	boolean mb_dfModified;			// df에 수정을 했는지 여부( 최종 색인일 Update등 )
	
	// 색인정보
	int mi_indexMode;				// 색인모드 ( reindex/all/modified )
	String ms_resultFileName ;
	int mi_resultMaxRows; 
	
	// 데이터 소스의 Global 정보
	Node mo_globalConnectInfo;		// 접속정보가  <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalQueryInfo;		// 쿼리정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalMatchInfo;		// 컬럼매칭정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalFileAccessInfo;	// 파일접근정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalIndexer;			// Indexer 정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalCustomizer;		// ConverCustomizer 정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalDataReader;		// DataReader 정보가 <data_source> 외부정의시 그 노드를 가리킴
	Node mo_globalEtcReader;		// EtcReader 정보가 <data_source> 외부정의시 그 노드를 가리킴
	
	int mi_dataSourceCount;				// 정의된 데이터소스의 수
	
	// 데이터 소스의 Local 정보
	String ms_curDataSourceId;			// data sourece id
	int mi_limitResultFileSize;			// 색인결과 파일 최대 사이즈
	int mi_curDataSourceType;			// dataSoure의 종류
	int mi_curMaxIndexRows;				// 최대 색인시도 갯수
	String ms_curDataSourceState;		// 상태 - N: 정상색인, D:색인에서삭제
	String ms_curLastDocIndexdate;		// 최종 문서 색인일
	String ms_curLastDoccountIndexdate;	// 최종 조회수 색인일

	ArrayList<BatchJob> mo_batchJobList; 	// BatchJob들의 목록
	
	String ms_dfEncode; 		// DF(XML) Encoding
	
	String ms_timeZone;			// Time Zone
	
	int ms_resulFileSize;		// result file size
	
	String ms_DFName;			// DF Name
	
	/**
	* DFLoader의 기본생성자
	**/
	public DFLoader( String sDFEncode ) 
	{
		mo_document= null;
		mo_dataSourceCursor =null;
		mi_cursorState = DFLoader.DATASOURCE_CURSOR_BOF;
		
		mi_indexMode = DFLoader.INDEX_NONE;
		ms_resultFileName = null;
		mi_resultMaxRows = -1;
		
		// 데이터 소스의 [Global 영역] 정보를 담는 변수의 초기화
		mo_globalConnectInfo = null;
		mo_globalQueryInfo = null ;
		mo_globalMatchInfo = null ;
		mo_globalFileAccessInfo = null;
		mo_globalIndexer = null;
		mo_globalCustomizer = null;
		mo_globalDataReader = null;
		mo_globalEtcReader = null;

		mi_dataSourceCount = 0;
		mb_dfModified = false;
		
		ms_dfEncode = sDFEncode;
		
		ms_timeZone = null;
		ms_resulFileSize = 0;
		
		ms_DFName = null;
	}
	
	public void setLogger(String sLogPath) 
	{
		logger = RayfulLogger.getLogger( DFLoader.class.getName(), sLogPath );
	}
	
	/**
	* 메모리에 로드된 DF내용을 파일로 저장한다.
	* <p>
	* @param	sDFFileName	DF파일경로/이름
	**/	
	public void save ( String sDFFFileName ) 
	{
		try {
			if ( ! mb_dfModified ) {
				// df가 수정되지 않았다면 함수를 종료한다.
				return;
			}
			
			if ( mo_document == null ) {
				return ;
			}
			
			File oDFFile = new File( sDFFFileName );
			
		   // Use a Transformer for output
		  TransformerFactory tFactory = TransformerFactory.newInstance();
		  Transformer transformer = tFactory.newTransformer();
		
		  DOMSource oDomSource = new DOMSource(mo_document);
		  StreamResult oStreamResult = new StreamResult(oDFFile);
		  
		  // UTF8/EUC-KR로 Encoding하기위해서 반드시 필요
		  // 일단 헤드부분이 잘리는 현상때문에 주석으로 막음
		  transformer.setOutputProperty("encoding", ms_dfEncode );
		  
		  // Dom을 파일로 저장시킨다.
		  transformer.transform(oDomSource, oStreamResult); 
				
		} catch (TransformerConfigurationException tce) {
		  // Error generated by the parser
		  logger.error( Localization.getMessage( DFLoader.class.getName() + ".Looger.0041" ), tce);
		
		   // Use the contained exception, if any
		  Throwable x = tce;
		  if (tce.getException() != null) {
		    x = tce.getException();
		  }
		  x.printStackTrace();
		} catch (TransformerException te) {
		  // Error generated by the parser
		  logger.error( Localization.getMessage( DFLoader.class.getName() + ".Looger.0042" ), te);
		
		  // Use the contained exception, if any
		  Throwable x = te;
		  if (te.getException() != null)
		    x = te.getException();
		  x.printStackTrace();
		}
	}
	
	
	/**
	* DF파일의 내용을 xml parser를 이용하여 로드한다.
	* <br>
	* 내부에서 loadTargetInfo, loadBatchJobInfo, loadSourceInfo 를 호출
	* <p>
	* @param	sDFFileName	DF파일경로/이름
	**/
	public void load( String sDFFileName )
	throws DFLoaderException
	{
		DocumentBuilderFactory oFactory = null;
		DocumentBuilder oBuilder = null;
		
		String sErrMsg = null;
		String sFunctionName = "load()";
		
		try {
			oFactory = DocumentBuilderFactory.newInstance();
			oBuilder = oFactory.newDocumentBuilder();
		} catch ( ParserConfigurationException pce ) {
			// Parser with specified options can't be built
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Exception.0002" );
			logger.error( sErrMsg, pce );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		} 
		
		try {
			mo_document = oBuilder.parse( new File(sDFFileName) );
			
			ms_DFName = sDFFileName;
		} catch (SAXException sxe) {
			// Error generated during parsing
			Exception ox = sxe;
			if (sxe.getException() != null)
			{
				ox = sxe.getException();
			}
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Exception.0003" );
			logger.error( sErrMsg, ox );
			throw new DFLoaderException( sErrMsg, sFunctionName );

		} catch (IOException ioe) {
			// I/O error
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Exception.0004" );
			logger.error( sErrMsg, ioe );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		//target의 정보를 로드
		loadTargetInfo();
		
		//BatchJob의 정보를 로드
		loadBatchJobInfo();
		
		//source의 정보를 로드
		loadSourceInfo();
	}
	
	/**
	* Target노드의 Attribute정보를 읽는다.
	* <br>
	* Attribute : indexing
	**/			
	private void loadTargetInfo()
	throws DFLoaderException
	{
		Element oElem;
		NodeList oNodeList; 
		//NodeList oChildList; 		
		Node oNode;
		
		String sErrMsg = null;
		String sFunctionName = "loadTargetInfo()";
		String sValue = null;
		
		oElem = mo_document.getDocumentElement();
		oNodeList = oElem.getElementsByTagName( "target");
		if ( oNodeList == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0049" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}	
		
		// target node
		oNode = oNodeList.item(0);
		
		// [DataSource] 노드의 속성을 구한다.
		NamedNodeMap oNodeMap = oNode.getAttributes();
		Node oAttributeNode;

		// id값을 알아낸다.
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "indexing" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
				
				if ( sValue == null || sValue.length() == 0 ) {
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0050" );
					logger.error( sErrMsg );
					throw new DFLoaderException( sErrMsg, sFunctionName );
				}				
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0051" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}
		
		if ( sValue.equalsIgnoreCase( "reindex" ) ) {
			mi_indexMode = DFLoader.INDEX_REINDEX;
		} else if ( sValue.equalsIgnoreCase( "all" ) ) {
			mi_indexMode = DFLoader.INDEX_ALL;
		} else if ( sValue.equalsIgnoreCase( "modified" ) ) {
			mi_indexMode = DFLoader.INDEX_MODIFIED;
		} else {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0052" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );			
		}
		
		
		// result_file Node
		oNode = oNode.getChildNodes().item(1);
		
		System.out.println( Localization.getMessage( DFLoader.class.getName() + ".Console.0001", oNode.getNodeName() ) );
		
		if ( oNode != null ) {
			oNodeMap = oNode.getAttributes();
			if ( oNodeMap == null ) {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0043" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);						
			}

			// attribute : file_name
			oAttributeNode = oNodeMap.getNamedItem( "file_name" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
				
				if ( sValue == null || sValue.length() == 0 ) {
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0044" );
					logger.error( sErrMsg );
					throw new DFLoaderException( sErrMsg, sFunctionName );
				}				
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0045" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
			ms_resultFileName = sValue;

			// attribute : max_rows
			oAttributeNode = oNodeMap.getNamedItem( "max_rows" );
			if (oAttributeNode != null) {
				try {
					mi_resultMaxRows = Integer.parseInt( oAttributeNode.getNodeValue() );
				} catch ( NumberFormatException e ) {
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0046", oAttributeNode.getNodeValue() );
					logger.error( sErrMsg );
					throw new DFLoaderException( sErrMsg, sFunctionName);
				}			
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0047" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}		
		}
	}
	
	/**
	* batchjob 노드의 자식노드들의 Attribute 정보 및 노드값을 읽는다.
	* <br>
	* batchjob 자식노드의 Attribute : name, error, dfload
	* batchjob 자식노드의 노드값 : 실행명령
	**/			
	private void loadBatchJobInfo()
	throws DFLoaderException
	{
		Element oElem;
		NodeList oNodeList; 
		Node oNode;
		
		NodeList oJobNodeList = null; 
		Node oJobNode = null;		
		Node oCommandNode = null;
		
		String sErrMsg = null;
		String sFunctionName = "loadBatchJobInfo()";
		
		String sJobName = null;
		String sJobCommand = "";
		String sErrorProcessType = null;
		boolean bDfReload = false;
		
		int iErrorProcessType = BatchJob.ERRORPROCESS_TYPE_UNKNOWN;
		
		BatchJob oBatchJob = null;
		
		//BatchJob을 저장할 arrayList
		mo_batchJobList = new ArrayList<BatchJob>();
		
		oElem = mo_document.getDocumentElement();
		oNodeList = oElem.getElementsByTagName( "batchjob");
		if ( oNodeList == null ) {
			return;
		}	
		
		oNode = oNodeList.item(0);
		if ( oNode == null ) {
			return;
		}
		
		
		oJobNodeList = oNode.getChildNodes();
		
		
		for ( int i=0; i< oJobNodeList.getLength(); i ++ ) {
			oJobNode = oJobNodeList.item(i);
			
			// source_column을 찾는다.
			if ( oJobNode.getNodeType() == Node.ELEMENT_NODE && 
						oJobNode.getNodeName().equalsIgnoreCase("job") ) {				
		
				// [batchjob] 노드의 속성을 구한다.
				NamedNodeMap oNodeMap = oJobNode.getAttributes();
				Node oAttributeNode;
		
				// name, error 속성값을 알아낸다.
				if ( oNodeMap != null ) {
					
					// name 속성
					oAttributeNode = oNodeMap.getNamedItem( "name" );
					if (oAttributeNode != null) {
						sJobName = oAttributeNode.getNodeValue();
						
						if ( sJobName == null || sJobName.length() == 0 ) {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0053" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );
						}				
					} else {
						sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0054" );
						logger.error( sErrMsg );
						throw new DFLoaderException( sErrMsg, sFunctionName);
					}
			
					// error 속성
					oAttributeNode = oNodeMap.getNamedItem( "error" );
					if (oAttributeNode != null) {
						sErrorProcessType = oAttributeNode.getNodeValue();
						
						if ( sErrorProcessType == null || sErrorProcessType.length() == 0 ) {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0055" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );
						}
						
						if ( sErrorProcessType.equalsIgnoreCase("continue") ) {
							iErrorProcessType = BatchJob.ERRORPROCESS_TYPE_CONTINUE;
						} else if ( sErrorProcessType.equalsIgnoreCase("stop") ) {
							iErrorProcessType = BatchJob.ERRORPROCESS_TYPE_STOP;
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0056" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );
						}
							
					} else {
						sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0057" );
						logger.error( sErrMsg );
						throw new DFLoaderException( sErrMsg, sFunctionName);
					}
					
					// dfreload 속성
					bDfReload = false;
					oAttributeNode = oNodeMap.getNamedItem( "dfreload" );
					if ( oAttributeNode != null ) {
						if ( oAttributeNode.getNodeValue() != null ) {
							if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "on" ) ) {
								bDfReload = true;
							}
						}
					}
							
					
				} else {	// if ( oNodeMap != null )
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0058" );
					logger.error( sErrMsg );
					throw new DFLoaderException( sErrMsg, sFunctionName );			
				}
				
				// Batch Job Command ( Node Value )
				oCommandNode = oJobNode.getFirstChild();
				if ( oCommandNode != null ) {
					sJobCommand = oCommandNode.getNodeValue();
					
					if ( sJobCommand == null || sJobCommand.length() == 0 ) {
						sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0059" );
						logger.error( sErrMsg );
						throw new DFLoaderException( sErrMsg, sFunctionName);						
					}
				} else {
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0060" );
					logger.error( sErrMsg );
					throw new DFLoaderException( sErrMsg, sFunctionName);					
				}
				
				oBatchJob = new BatchJob ( sJobName, sJobCommand, iErrorProcessType, bDfReload );
				mo_batchJobList.add ( oBatchJob );
			} // if ( oNode.getNodeName().equalsIgnoreCase( "job") ) {
		} // for
	
	}	
	
	
	/**
	* Source노드아래 정의된 정보들을 읽는다.
	* <br>
	* 데이터소스의 수, Source태그아래 정의된 노드들의 포인터를 저장한다.
	* <br>
	* Source태그 아래 정의된 노드들 :  connect_info, query_info, match_info, 
	* file_access, indexer,convert_customizer, data_reader, sswriter
	**/		
	private void loadSourceInfo() 
	throws DFLoaderException
	{
		NodeList oNodeList, oDataSourceList;
		Node oNode;
		Element oElem;
		int iDataSourceCnt = 0;
		
		String sErrMsg = null;
		String sFunctionName = "loadSourceInfo()";
		String sValue = null;
		
		oElem = mo_document.getDocumentElement();
		oNodeList = oElem.getElementsByTagName( "source");
		if ( oNodeList == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0061" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		oNode = oNodeList.item(0);
		
		// 노드의 속성을 구한다.
		NamedNodeMap oNodeMap = oNode.getAttributes();
		Node oAttributeNode;

		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "timezone" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
				if ( sValue != null )
					ms_timeZone = sValue;
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0062" );
				logger.info( sErrMsg );
				ms_timeZone = "Asia/Seoul";
				//throw new DFLoaderException( sErrMsg, sFunctionName);
			}
			
			oAttributeNode = oNodeMap.getNamedItem( "resultfilesize" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
				if ( sValue != null )
					ms_resulFileSize = Integer.parseInt(sValue);
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0084" );
				logger.info( sErrMsg );
				ms_resulFileSize = 10;
				//throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}
		
		oDataSourceList = oNode.getChildNodes();
		
		for ( int i=0; i< oDataSourceList.getLength(); i ++ ) {
			oNode = oDataSourceList.item(i);
			if ( oNode.getNodeName().equalsIgnoreCase( "data_source") ) {
				iDataSourceCnt ++ ;
			}
			
			if ( oNode.getNodeName().equalsIgnoreCase( "connect_info") ) {
				// 멤버변수에 설정
				mo_globalConnectInfo = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "query_info") ) {
				// 멤버변수에 설정
				mo_globalQueryInfo = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "match_info") ) {
				// 멤버변수에 설정
				mo_globalMatchInfo = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "file_access") ) {
				// 멤버변수에 설정
				mo_globalFileAccessInfo = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "indexer") ) {
				// 멤버변수에 설정
				mo_globalIndexer = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "convert_cutomizer") ) {
				// 멤버변수에 설정
				mo_globalCustomizer = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "data_reader") ) {
				// 멤버변수에 설정
				mo_globalDataReader = oNode;
			} else if ( oNode.getNodeName().equalsIgnoreCase( "etc_reader") ) {
				// 멤버변수에 설정
				mo_globalEtcReader = oNode;
			}
			
		}
		
		// 멤버변수에 설정
		mi_dataSourceCount = iDataSourceCnt;
	}
	
	/**
	* DataSource노드에 정의된 Attribute정보들을 읽는다.
	* <br>
	* 데이터소스ID, 소스타입(rdbms/notes), max_rows, 최종 문서 수정일, 최종 조회수 수정일
	**/			
	private void loadDataSourceInfo() 
	throws DFLoaderException
	{
		String sValue = null;
		String sErrMsg = null;
		String sFunctionName = "loadDataSourceInfo()";
		
		if ( mo_dataSourceCursor == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0063" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );					
		}	
						
		// [DataSource] 노드의 속성을 구한다.
		NamedNodeMap oNodeMap = mo_dataSourceCursor.getAttributes();
		Node oAttributeNode;

		// id값을 알아낸다.
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "id" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0064" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}
		if ( sValue == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0065" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );			
		}
		ms_curDataSourceId = sValue;
		
		sValue = null;
		int iResulFileSize = -1;
		// resultfilesize값을 알아낸다.
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "result_file_size" );
			if (oAttributeNode != null) {
				iResulFileSize = Integer.parseInt( oAttributeNode.getNodeValue() );
			} else {
				iResulFileSize = 10; // 기본 파일 크기 10mb
			}
		}
		mi_limitResultFileSize = iResulFileSize;

		// source_type값을 알아낸다.
		sValue = null;
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "source_type");
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0066" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);			
			}
		}
		if ( sValue.equalsIgnoreCase ("RDBMS") ) {
			mi_curDataSourceType = DFLoader.DATASOURCE_TYPE_RDBMS ;
		} else if ( sValue.equalsIgnoreCase ("NOTES") ) {
			mi_curDataSourceType = DFLoader.DATASOURCE_TYPE_NOTES;
		} else {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0067", sValue );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// state값을 알아낸다.	
		sValue = null;	
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "state" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0068" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}
		if ( sValue == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0069" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );			
		} else {
			sValue = sValue.toUpperCase();
			if ( !sValue.equals("N") && !sValue.equals("D") && !sValue.equals("C") &&  !sValue.equals("S") ) {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0070" );
				logger.error( sErrMsg );
			}
		}
		ms_curDataSourceState = sValue;		
		
		
		// max_rows값을 알아낸다.
		int iMaxRows = -1;
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "max_rows" );
			if (oAttributeNode != null) {
				try {
					iMaxRows = Integer.parseInt( oAttributeNode.getNodeValue() );
				} catch ( NumberFormatException e ) {
					sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Exception.0005", iMaxRows );
					logger.warn( sErrMsg );
				}
			}
		}
		mi_curMaxIndexRows = iMaxRows;
		
		// last_doc_indexdate값을 알아낸다.
		sValue = null;
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "last_doc_indexdate" );
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0071" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}

		if ( sValue.length() > 0 ) {
			try {
				sValue = ESPTypes.dateFormatString( sValue );
			} catch ( IllegalArgumentException iae ) {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0072" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName );
			}			
		} else {
			sValue = null;
		}
		ms_curLastDocIndexdate = sValue;
		
		// last_doccount_indexdate값을 알아낸다.
		sValue = null;
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "last_doccount_indexdate");
			if (oAttributeNode != null) {
				sValue = oAttributeNode.getNodeValue();
			} else {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0073" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName);
			}
		}
		
		if ( sValue.length() > 0 ) {
			try {
				sValue = ESPTypes.dateFormatString( sValue );
			} catch ( IllegalArgumentException iae ) {
				sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0074" );
				logger.error( sErrMsg );
				throw new DFLoaderException( sErrMsg, sFunctionName );
			}			
		} else {
			sValue = null;
			// last_doc_indexdate값이 null이 아니면 그값을 세팅...(중요)
			if ( ms_curLastDocIndexdate != null ) {
				sValue = ms_curLastDocIndexdate;
			}
		}
		ms_curLastDoccountIndexdate = sValue;

	}	


	/**
	* BatchJob목록을 리턴한다.
	* @return	BatchJob목록
	**/		
	public ArrayList<BatchJob> getBatchJobList()
	{
		return mo_batchJobList;
	}

	/**
	* indexing mode를 알아낸다.
	* @return	indexing
	**/		
	public int getTargetIndexMode()
	{
		return mi_indexMode;
	}
	
	public String getTimeZone()
	{
		return ms_timeZone;
	}
	
	public int getResulFileSize()
	{
		return ms_resulFileSize;
	}
	
	public String getTargetResultFileName() {
		return ms_resultFileName;
	}
	
	public int getTargetResultMaxRows() {
		return mi_resultMaxRows;
	}
		
	/**
	* data_source의 갯수를 리턴
	* @return	data_source tag의 갯수
	**/	
	public int getDataSourceCount()
	{
		return mi_dataSourceCount;
	}
	

	/**
	* 현재 데이터소스의 id를 알아낸다. <p>
	* <p>
	* @return 데이터소스의 ID
	**/	
	public String getDataSourceId()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceId()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return ms_curDataSourceId;
	}
	
	/**
	* 현재 데이터소스의 result_file_size값을 알아낸다. <p>
	* max_rows값이 설정되면 색인대상의 수를 max_rows보다 작거나 같은 수로 간주한다.
	* <p>
	* @return max_rows값
	**/	
	public int getDataSourceResultFileSize()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceResultFileSize()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return mi_limitResultFileSize;
	}

	/**
	* 현재 데이터소스의 종류를 알아낸다. <p>
	* <p>
	* @return 데이터소스의 종류
	* <br>DATASOURCE_TYPE_RDBMS : RDBMS
	* <br>DATASOURCE_TYPE_NOTES : Notes	
	**/	
	public int getDataSourceType()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceType()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}

		return mi_curDataSourceType;
	}

	/**
	* 현재 데이터소스의 max_rows값을 알아낸다. <p>
	* max_rows값이 설정되면 색인대상의 수를 max_rows보다 작거나 같은 수로 간주한다.
	* <p>
	* @return max_rows값
	**/	
	public int getDataSourceMaxRows()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceMaxRows()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return mi_curMaxIndexRows;
	}
	
	/**
	* 현재 데이터소스의 상태값을 알아낸다. <p>
	* 색인로직에서는 이 값이 (N)이면 색인을 수행하고, 
	* (D)이면 색인으로부터 해당데이터소스데이터를 삭제처리한다.
	* <p>
	* @return 데이터소스의 State값 
	* <p>
	**/	
	public String getDataSourceState()
	throws DFLoaderException
	{
		String sErrMsg = "DATASOURCE_CURSOR_EOF 이거나 DATASOURCE_CURSOR_EOF 입니다";
		String sFunctionName = " getDataSourceState()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return ms_curDataSourceState;
	}		
	
	/**
	* 현재 데이터소스의 상태값을 설정한다. <p>
	* 주로 데이터소스의 상태가 (D)인 것을 삭제처리 한 후에 
	* 삭제처리가 완료되었다는 표시(C)를 하기 위해 사용한다. 
	* <p>
	* @param	sState	설정하려는 노드의 상태
	**/	
	public void setDataSourceState( String sState )
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = "setDataSourceState()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}

		// [DataSource] 노드의 state속성을 설정한다.
		NamedNodeMap oNodeMap = mo_dataSourceCursor.getAttributes();
		Node oAttState = oNodeMap.getNamedItem( "state" );
		oAttState.setNodeValue ( sState );
		
		// 현재[DataSource] 노드의 state속성을 표시한는 멤버변수값도 변경 ** 중요 **
		ms_curDataSourceState = sState;
		
		mb_dfModified = true;	//DF가 변경됨을 표시
	}		
	
	
	/**
	* 현재 데이터소스의 최종 문서 색인일을 알아낸다. <p>
	* <p>
	* @return 최종 문서 색인일
	**/	
	public String getDataSourceLastDocIndexdate()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceLastDocIndexdate()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return ms_curLastDocIndexdate;
	}	
	
	/**
	* 현재 데이터소스의 최종 문서조회수 색인일을 알아낸다. <p>
	* <p>
	* @return 최종 문서조회수 색인일
	**/	
	public String getDataSourceLastDoccountIndexdate()
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " getDataSourceLastDoccountIndexdate()";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return ms_curLastDoccountIndexdate;
	}
	
	/**
	* 현재 데이터소스의 최종 문서 색인일을 DF에 기록한다.
	* save호출시 save Method가 수행되도록 변경플레그(mb_dfModified)를 세팅한다.
	* <p>
	* @param	sLastDocIndexdate	최종 문서조회수 색인일
	**/		
	public void setDataSourceLastDocIndexdate( String sLastDocIndexdate ) 
		throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " saveDataSourceLastDocIndexdate";
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// [DataSource] 노드의 속성을 구한다.
		NamedNodeMap oNodeMap = mo_dataSourceCursor.getAttributes();
		Node oAttributeNode;

		// id값을 알아낸다.
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "last_doc_indexdate" );
			if (oAttributeNode != null) {
				
				try {
					oAttributeNode.setNodeValue( sLastDocIndexdate );
					mb_dfModified = true;		//DF가 변경됨을 표시
					
					//Attr oNewAttributeNode = mo_document.createAttribute( "last_doc_indexdate");
					//oNewAttributeNode.setValue ( sLastDocIndexdate );
					//oAttributeNode.getParentNode().replaceChild ( oNewAttributeNode, oAttributeNode );
					
				} catch ( DOMException de ) {
					logger.error( de.toString() );
				} catch ( Exception e ) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	* 현재 데이터소스의 최종 문서조회수 색인일을 DF에 기록한다.
	* save호출시 save Method가 수행되도록 변경플레그(mb_dfModified)를 세팅한다.
	* <p>
	* @param	sLastDoccountIndexdate	최종 문서조회수 색인일
	**/		
	public void setDataSourceLastDoccountIndexdate( String sLastDoccountIndexdate ) 
	throws DFLoaderException
	{
		String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0002" );
		String sFunctionName = " saveDataSourceLastDoccountIndexdate()";
		
		if ( mi_cursorState != DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			logger.error( sErrMsg );
			new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// [DataSource] 노드의 속성을 구한다.
		NamedNodeMap oNodeMap = mo_dataSourceCursor.getAttributes();
		Node oAttributeNode;

		// id값을 알아낸다.
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "last_doccount_indexdate" );
			if (oAttributeNode != null) {		
				oAttributeNode.setNodeValue( sLastDoccountIndexdate );
				mb_dfModified = true;		//DF가 변경됨을 표시
			}
		}		
	}	
	
	
	/**
	* 커서가 다음 DataSource를 가리키도록 한다.
	* (중요)삭제처리가 완료(state=C)된 DataSource정보가 있다면 DF로 부터 삭제한다.
	* @return	true:정상 / false:데이터소스의 끝
	**/		
	public boolean nextDataSource()
	throws DFLoaderException
	{
		NodeList oNodeList;
		Node oNode;
		Element oElem;		

		//String sErrMsg = null;
		String sFunctionName = "nextDataSource()";
		
		// data_source 정보를 담는 변수의 초기화
		ms_curDataSourceId = null;
		mi_limitResultFileSize = -1;
		mi_curDataSourceType = DFLoader.DATASOURCE_TYPE_UNKNOWN;
		mi_curMaxIndexRows = -1;
		ms_curLastDocIndexdate = null;
		ms_curLastDoccountIndexdate = null;
		
		// next로 이동하기 이전노드
		Node oPrevDataSource = null;
		// next로 이동하기 이전의 데이터소스 상태값
		String sPrevState = "N";
						
		if ( mi_cursorState == DFLoader.DATASOURCE_CURSOR_BOF ) {
			oElem = mo_document.getDocumentElement();		
			oNodeList = oElem.getElementsByTagName( "data_source");
			
			if (oNodeList == null) {
				String sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0001" );
				logger.error( sErrMsg );
				throw new DFLoaderException ( sErrMsg, sFunctionName );
			}
			
			oNode = oNodeList.item(0);
			mo_dataSourceCursor = oNode;
			mi_cursorState = DFLoader.DATASOURCE_CURSOR_NORMAL;
			
		} else if ( mi_cursorState == DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			
			//현재 데이터소스 노드값을 기억
			oPrevDataSource = mo_dataSourceCursor;
			sPrevState = this.getDataSourceState();
			
			
			while( ( mo_dataSourceCursor = mo_dataSourceCursor.getNextSibling() ) != null ) {
				if ( mo_dataSourceCursor.getNodeName().equalsIgnoreCase("data_source") ) {
					break;
				}
			}
			
			if ( mo_dataSourceCursor == null ) {
				mi_cursorState = DFLoader.DATASOURCE_CURSOR_EOF;
			}	
		}
		
		// 이전 데이터소스 노드의 상태가 (C-삭제완료)라면 DF에서 이전 데이터소스 노드를 삭제한다.
		if ( sPrevState.equals("C") ) {
			this.removeDataSource( oPrevDataSource );
		}
		
		if ( mi_cursorState == DFLoader.DATASOURCE_CURSOR_NORMAL ) {
			//데이터 소스의 [Local 영역] 정보를 알아낸다.
			loadDataSourceInfo();
			return true;
		} else {
			return false;
		}
	}


		
		
	/**
	* DataSource에 정의된 특정정보를 저장하고 있는 노드의 포인터를 리턴
	* <br>
	* 먼저 Datasource 전역영역에서 찾고 찾지 못할 경우
	* 현재데이터소스 커서내에서 선언된 정보가 있는지 조회한다.
	* <br>
	* Parameter iInfoType의 값에 찾고자하는 정보의 유형이 결정된다.
	& <p>
	* @param iInfoType	알아낼 정보의 유형을 나타낸다.
	* <br>DATASOURCE_INFO_CONNECT : Connection 정보 
	* <br>DATASOURCE_INFO_QUERY : 쿼리정보 
	* <br>DATASOURCE_INFO_FILEACCESS : 첨부파일접근정보
	* <br>DATASOURCE_INFO_COLUMNMATCHING : 컬럼매칭정보
	* <br>DATASOURCE_INFO_INDEXER : Indexer정보
	* <br>DATASOURCE_INFO_CUSTOMIZER : customizer정보
	* <br>DATASOURCE_INFO_DATAREADER : DataReader 정보
	* <br>DATASOURCE_INFO_SSWRITER : SSWriter 정보 
	* <p>
	* @return 필요한정보가 있는 Node
	**/		
	private Node getDataSourceNode( int iInfoType ) 
	{
		NodeList oNodeList = null;
		Node oNode = null;
		boolean bIsFind = false;

		
		if ( iInfoType == DFLoader.DATASOURCE_INFO_CONNECT ) {
			// 전역적으로 선언된 접속정보가 있다면 그것을 사용
			if ( mo_globalConnectInfo != null ) {
				return mo_globalConnectInfo;
			}
		/*
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_QUERY ) {
			// 전역적으로 선언된 쿼리정보가 있다면 그것을 사용
			if ( mo_globalQueryInfo != null ) {
				return mo_globalQueryInfo;
			}
		*/		
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_COLUMNMATCHING ) {
			// 전역적으로 선언된 컬럼매칭정보가 있다면 그것을 사용
			if ( mo_globalMatchInfo != null ) {
				return mo_globalMatchInfo;
			}	
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_FILEACCESS ) {
			// 전역적으로 선언된 파일접근정보가 있다면 그것을 사용
			if ( mo_globalFileAccessInfo != null ) {
				return mo_globalFileAccessInfo;
			}	
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_INDEXER ) {
			// 전역적으로 선언된 Indexer명이 있다면 그것을 사용
			if ( mo_globalIndexer != null ) {
				return mo_globalIndexer;
			}
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_CUSTOMIZER ) {
			// 전역적으로 선언된 ConverCustomizer명이 있다면 그것을 사용
			if ( mo_globalCustomizer != null ) {
				return mo_globalCustomizer;
			}			
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_DATAREADER ) {
			// 전역적으로 선언된 DataReader명이 있다면 그것을 사용
			if ( mo_globalDataReader != null ) {
				return mo_globalDataReader;
			}	
		} else if ( iInfoType == DFLoader.DATASOURCE_INFO_ETCREADER ) {
			// 전역적으로 선언된 Indexer명이 있다면 그것을 사용
			if ( mo_globalEtcReader != null ) {
				return mo_globalEtcReader;
			} //else {
				// (중요)sswriter는 전역적으로만 선언되므로 
				// 전역역역에서 찾을 수 없다면 null을 리턴한다.
			//	return null;
			//}
		}
		
		
		// 현재데이터소스 커서내  자식노드들을 얻는다.
		oNodeList = mo_dataSourceCursor.getChildNodes();
		
		// 현재데이터소스의 자식노드내에서 필요한 정보를 찾는다.
		for ( int i=0; i< oNodeList.getLength(); i++ ) {
			oNode = oNodeList.item(i);
			
			switch ( iInfoType ) {
				case DFLoader.DATASOURCE_INFO_CONNECT :
					// connect_info node를 찾는다
					if ( oNode.getNodeName().equalsIgnoreCase( "connect_info" ) ) {
						bIsFind = true;
					}
					break;
					
				case DFLoader.DATASOURCE_INFO_QUERY : 
					// query_info node를 찾는다
					if ( oNode.getNodeName().equalsIgnoreCase("query_info") ) {
						bIsFind = true;
					}				
					break;

				case DFLoader.DATASOURCE_INFO_COLUMNMATCHING :
					// match_info node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("match_info") ) {
						bIsFind = true;
					}
					break;
										
				case DFLoader.DATASOURCE_INFO_FILEACCESS :
					// file_access node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("file_access") ) {
						bIsFind = true;
					}	
					break;
					
				case DFLoader.DATASOURCE_INFO_INDEXER :
					// indexer node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("indexer") ) {
						bIsFind = true;
					}	
					break;	
					
				case DFLoader.DATASOURCE_INFO_CUSTOMIZER :
					// convert_customizer node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("convert_customizer") ) {
						bIsFind = true;
					}	
					break;						
					
				case DFLoader.DATASOURCE_INFO_DATAREADER :
					// data_reader node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("data_reader") ) {
						bIsFind = true;
					}	
					break;		
				case DFLoader.DATASOURCE_INFO_ETCREADER :
					// data_reader node를 찾는다				
					if ( oNode.getNodeName().equalsIgnoreCase("etc_reader") ) {
						bIsFind = true;
					}	
					break;							

			}
			
			if ( bIsFind ) {
				// For Loop를 빠져나온다.
				return oNode;
			}
		}
		return null;
	}

	/**
	* DF파일로부터 접속정보를 알아낸다.
	* <p>
	* @param iDataSourceType
	* <br>DATASOURCE_TYPE_RDBMS : RDBMS
	* <br>DATASOURCE_TYPE_NOTES : Notes System
	* @return Object 객체(RDBMS : JdbcConnector/ Notes System : NotesConnector )
	**/	
	public Object getDataSourceConnector( int iDataSourceType )
	{
		if ( iDataSourceType == DFLoader.DATASOURCE_TYPE_RDBMS  ) {
			return this.getDataSourceRdbmsConnector();
		} else if ( iDataSourceType == DATASOURCE_TYPE_NOTES ) {
			return this.getDataSourceNotesConnector();
		} else {
			return null;
		}
	}
	
	/**
	* DF파일로부터 RDBMS 접속정보를 알아낸다.
	* <p>
	* @return JdbcConnector 객체 
	**/	
	private JdbcConnector getDataSourceRdbmsConnector() 
	{
		Utils utils = new Utils();
		
		String sNodeName;
		Node oCurNode;
		Node oValueNode;
		
		String sDriverClass = null;
		String sUrl = null;
		String sUsername = null;		
		String sPassword = null;
		String sEncrypt = null;

		Node node = getDataSourceNode( DFLoader.DATASOURCE_INFO_CONNECT );
		if ( node == null ) {
			//노드를 찾지 못해도 예외를 발생시키지는 않는다.
			return null;
		}
		
		// connect_info 는 Attribute를 기본적으로 갖지 않는다.
		NamedNodeMap oNodeMap = node.getAttributes();
		
		if ( oNodeMap != null ) {
			Node oAttributeNode = oNodeMap.getNamedItem( "encrypt" );
			
			if (oAttributeNode != null) 
				sEncrypt = oAttributeNode.getNodeValue();
		}
		
		NodeList nodelist = node.getChildNodes();
		
		for ( int i=0; i< nodelist.getLength(); i++ ) {
			oCurNode = nodelist.item(i);
			
			sNodeName = oCurNode.getNodeName();
			
			if (sNodeName.equalsIgnoreCase("driverClass") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sDriverClass = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			}else if (sNodeName.equalsIgnoreCase("url") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sUrl = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			}else if (sNodeName.equalsIgnoreCase("username") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sUsername = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			}else if (sNodeName.equalsIgnoreCase("password") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sPassword = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}								
			}
		}
		
		JdbcConnector oConnector = new RdbmsConnector( sDriverClass ,sUrl ,sUsername ,sPassword	);
		//System.out.println("RdmbsConnector=" + ((RdbmsConnector)oConnector).toString());
		return oConnector;
	}		
	
	
	/**
	* DF파일로부터 Notes 접속정보를 알아낸다.
	* <p>
	* @return NotesConnector 객체 
	**/	
	private NotesConnector getDataSourceNotesConnector() 
	{
		Utils utils = new Utils();
		String sNodeName;
		Node oCurNode;
		Node oValueNode;
		
		String sHostName = null;
		String sPortNumber = null;
		String sDbName = null;
		String sDbAccount = null;
		String sDbPassword = null;
		String sEncrypt = null;

		Node node = getDataSourceNode( DFLoader.DATASOURCE_INFO_CONNECT );
		if ( node == null ) {
			//노드를 찾지 못해도 예외를 발생시키지는 않는다.
			return null;
		}
		
		// connect_info 는 Attribute를 기본적으로 갖지 않는다.
		NamedNodeMap oNodeMap = node.getAttributes();
		
		if ( oNodeMap != null ) {
			Node oAttributeNode = oNodeMap.getNamedItem( "encrypt" );
			
			if (oAttributeNode != null) 
				sEncrypt = oAttributeNode.getNodeValue();
		}
		
		NodeList nodelist = node.getChildNodes();
		
		for ( int i=0; i< nodelist.getLength(); i++ ) {
			oCurNode = nodelist.item(i);
			sNodeName = oCurNode.getNodeName();
			
			if (sNodeName.equalsIgnoreCase("hostname") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sHostName = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			} else if (sNodeName.equalsIgnoreCase("port") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sPortNumber = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}				
			} else if (sNodeName.equalsIgnoreCase("dbname") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sDbName = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			} else if (sNodeName.equalsIgnoreCase("userid") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sDbAccount = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			} else if (sNodeName.equalsIgnoreCase("password") ) {
				oValueNode = oCurNode.getFirstChild();
				if (oValueNode != null ) {
					sDbPassword = utils.Decrypt(oValueNode.getNodeValue(), sEncrypt);
				}
			}
		}
		
		NotesConnector oConnector = new NotesConnector( sHostName,
														sPortNumber, 
														sDbName,
														sDbAccount, 
														sDbPassword
														);
		
		return oConnector;
	}
	
	/**
	* DF파일로부터 쿼리정보를 알아낸다.
	* <p>
	* @param iDataSourceType
	* <br>DATASOURCE_TYPE_RDBMS : RDBMS
	* <br>DATASOURCE_TYPE_NOTES : Notes System
	* @return Object 객체(RDBMS : RdbmsDataReader/ Notes System : NotesDataReader )
	**/	
	public Object getDataSourceDataReader( int iDataSourceType )
	throws DFLoaderException
	{
		if ( iDataSourceType == DFLoader.DATASOURCE_TYPE_RDBMS  ) {
			return this.getDataSourceRdbmsDataReader();
		} else if ( iDataSourceType == DATASOURCE_TYPE_NOTES ) {
			return this.getDataSourceNotesDataReader();
		} else {
			return null;
		}
	}
		
	/**
	* DF파일로부터 Rdbms 쿼리정보를 알아낸다.
	* <p>
	* @return RdbmsDataReader 객체
	**/		
	@SuppressWarnings("unchecked")
	private RdbmsDataReader getDataSourceRdbmsDataReader() 
	throws DFLoaderException
	{
		String sNodeName;
		Node oCurNode;
		Node oValueNode;
		Node oSubValueNode;
		
		String sErrMsg = null;
		String sFunctionName = "getDataSourceRdbmsDataReader()";
		String sKeyQuery = null;
		String sKeyCols = null;
		String sKeyDuplicateRemove = null;
		String sKeyIncQuery = null;
		String sKeyIncCols = null;
		String sKeyIncDuplicateRemove = null;
		String sDataQuery = null;
		String sDataKeyCols = null;
		String sFileQuery = null;
		String sFileKeyCols = null;
		String sPreQuery = null;
		String sPreKeyCols = null;
		String sDataMultiCols = null;
		
		Map<String, String> mapEtcQuery = null;
		
		int iQueryType = RdbmsDataReader.FQUERY_UNKNOWN;
		int iContentType = 0;

		/*
		 * 전역영역과 데이터소스영역에서 같은 쿼리정보를 정의했다면
		 * 전역영역의 쿼리정보보다  데이터소스영역의 쿼리정보가 우선한다.
		 * for문에서 전역영역을 처리한 후 데이터소스영역을 처리해서 overwirte되기 때문이다.
		 * 우선순위를 바꾸고 싶다면 배열에 넣는 순서를 바꾸기만 하면 된다.
		 */
		Node [] node = new Node [2];
		node[0] = mo_globalQueryInfo;		// 전역영역의 쿼리정보
		node[1] = getDataSourceNode( DFLoader.DATASOURCE_INFO_QUERY ); // 데이터소스영역의 쿼리정보

		// timestamp column 관련 ...
		NamedNodeMap oNodeMap = null;
		Node oAttributeNode = null;
		NamedNodeMap oKeyNodeMap = null;
		Node oKeyAttributeNode = null;
		
		// Loop내 필요한 변수 선언 
		boolean bFindQuery = false;
		boolean bFindKeyQuery = false;
		boolean bFindKeyIncQuery = false;
		boolean bFindDataQuery = false;
		boolean bFindFileQuery = false;
		
		NodeList oSubList = null;
		NodeList oSubSubList = null;
		NodeList nodelist = null;

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// DF의 전역영역과 데이터소스영역에서 쿼리문 정보를 추출한다.
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~		
		for ( int inode =0; inode < node.length; inode ++) {
			
			if ( node[ inode ] == null ) {
				continue;	// 다음 query_info 
			} else {
				bFindQuery = true;
			}
			
			nodelist = node[ inode ].getChildNodes();
			
			for ( int i=0; i< nodelist.getLength(); i++ ) {
				oCurNode = nodelist.item(i);
				sNodeName = oCurNode.getNodeName();
				
				// Loop내 필요한 변수 초기화
				oSubList = null;
				
				//====================================
				//	key query 정보
				//====================================
				// timestamp column은 key_inc_query 사용하면서 제외함.
//				if (sNodeName.equalsIgnoreCase("timestamp_columns") ) {	
//
//					oTimestampColumnList = oCurNode.getChildNodes();
//					
//					for ( int j=0; j< oTimestampColumnList.getLength(); j ++ ) {
//						oTimestampColumnNode = oTimestampColumnList.item(j);
//						
//						// timestamp_column을  찾는다.
//						if ( oTimestampColumnNode.getNodeType() == Node.ELEMENT_NODE && 
//								oTimestampColumnNode.getNodeName().equalsIgnoreCase("timestamp_column") ) {				
//					
//							// [timestamp_column을] 노드의 속성을 구한다.
//							oNodeMap = oTimestampColumnNode.getAttributes();
//
//							// name, type 속성값을 알아낸다.
//							if ( oNodeMap != null ) {
//								// name 속성
//								oAttributeNode = oNodeMap.getNamedItem( "name" );
//								if (oAttributeNode != null) {
//									sTimestampColumnName = oAttributeNode.getNodeValue();
//									
//									if ( sTimestampColumnName == null || sTimestampColumnName.length() == 0 ) {
//										sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0003" );
//										logger.error( sErrMsg );
//										throw new DFLoaderException( sErrMsg, sFunctionName );
//									}				
//								} else {
//									sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0004" );
//									logger.error( sErrMsg );
//									throw new DFLoaderException( sErrMsg, sFunctionName);
//								}
//						
//								// type 속성
//								oAttributeNode = oNodeMap.getNamedItem( "type" );
//								if (oAttributeNode != null) {
//									sTimestampColumnType = oAttributeNode.getNodeValue();
//									
//									if ( sTimestampColumnType == null || sTimestampColumnType.length() == 0 ) {
//										sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0005" );
//										logger.error( sErrMsg );
//										throw new DFLoaderException( sErrMsg, sFunctionName );
//									}
//									
//									if ( sTimestampColumnType.equalsIgnoreCase("string") ) {
//										iTimestampColumnType = TimestampColumn.TIMESTMP_STRING;
//									} else if ( sTimestampColumnType.equalsIgnoreCase("datetime") ) {
//										iTimestampColumnType = TimestampColumn.TIMESTMP_DATETIME;
//									} else {
//										sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0006" );
//										logger.error( sErrMsg );
//										throw new DFLoaderException( sErrMsg, sFunctionName );
//									}
//										
//								} else {
//									sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0007" );
//									logger.error( sErrMsg );
//									throw new DFLoaderException( sErrMsg, sFunctionName);
//								}
//								
//							} else {	// if ( oNodeMap != null )
//								sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0008" );
//								logger.error( sErrMsg );
//								throw new DFLoaderException( sErrMsg, sFunctionName );			
//							}
//							
//							// list에 timestamp_column 정보 추가. 
//							oTimeStamps.add( new TimestampColumn(sTimestampColumnName, iTimestampColumnType));
//						} // if ( oTimestampColumnNode.getNodeName().equalsIgnoreCase("timestamp_column") ) {
//						
//					} // for
//					
//					
//				} else 
				if (sNodeName.equalsIgnoreCase("key_query") ) {
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							bFindKeyQuery = true;
							sKeyQuery = oValueNode.getNodeValue();
							break;
						}
					}
					
					// key query의 duplicate_remove column 정보를 알아낸다.
					oKeyNodeMap = oCurNode.getAttributes();
	
					if ( oKeyNodeMap != null ) {
						oKeyAttributeNode = oKeyNodeMap.getNamedItem( "duplicate_remove" );
						if (oKeyAttributeNode != null) {
							sKeyDuplicateRemove = oKeyAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0075" );
							logger.info( sErrMsg );	
						}
						
						oKeyAttributeNode = oKeyNodeMap.getNamedItem( "keys" );
						
						if (oKeyAttributeNode != null) {
							sKeyCols = oKeyAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0081" );
							logger.info( sErrMsg );	
						}
						
					} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0076" );
							logger.info( sErrMsg );
					}
					
				//====================================
				//	key inc query 정보
				//====================================	
				} else if (sNodeName.equalsIgnoreCase("key_inc_query") ) {
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							bFindKeyIncQuery = true;
							sKeyIncQuery = oValueNode.getNodeValue();
							break;
						}
					}
					
					// key incquery의 duplicate_remove column 정보를 알아낸다.
					oKeyNodeMap = oCurNode.getAttributes();
	
					if ( oKeyNodeMap != null ) {
						oKeyAttributeNode = oKeyNodeMap.getNamedItem( "duplicate_remove" );
						if (oKeyAttributeNode != null) {
							sKeyIncDuplicateRemove = oKeyAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0077" );
							logger.error( sErrMsg );
						}
						
						oKeyAttributeNode = oKeyNodeMap.getNamedItem( "keys" );
						
						if (oKeyAttributeNode != null) {
							sKeyIncCols = oKeyAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0082" );
							logger.info( sErrMsg );	
						}
					} else {
						sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0078" );
						logger.error( sErrMsg );
					}
					
				//====================================
				//	data query 정보
				//====================================				
				} else if (sNodeName.equalsIgnoreCase("data_query") ) {
	
					// data query의 key column 정보를 알아낸다.
					oNodeMap = oCurNode.getAttributes();
	
					if ( oNodeMap != null ) {
						oAttributeNode = oNodeMap.getNamedItem( "keys" );
						if (oAttributeNode != null) {
							sDataKeyCols = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0009" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );	
						}
						
						oAttributeNode = oNodeMap.getNamedItem( "multi" );
						if (oAttributeNode != null) {
							sDataMultiCols = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0083" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );	
						}
						
					} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0010" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );
					}
									
					// data query의 SQL문을 알아낸다.
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							bFindDataQuery = true;
							sDataQuery = oValueNode.getNodeValue();
							break;
						}
					}
			
				//====================================
				//	file query 정보
				//====================================				
				} else if (sNodeName.equalsIgnoreCase("file_query") ) {
					
					// file query의 key column 정보를 알아낸다.
					oNodeMap = oCurNode.getAttributes();
					
					String sQueryType = null;
					String sContentType = null;
					
					if ( oNodeMap != null ) {
						// keys
						oAttributeNode = oNodeMap.getNamedItem( "keys" );
						if (oAttributeNode != null) {
							sFileKeyCols = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0011" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}
						
						// query_type
						oAttributeNode = oNodeMap.getNamedItem( "query_type" );
						if (oAttributeNode != null) {
							sQueryType = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0012" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}
						
						if ( sQueryType.equalsIgnoreCase( "file_list") ) {
							iQueryType = RdbmsDataReader.FQUERY_FILELIST;
						} else if ( sQueryType.equalsIgnoreCase( "file_content") ) {
							iQueryType = RdbmsDataReader.FQUERY_FILECONTENT;
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0013" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );							
						}					
						
						if ( iQueryType == RdbmsDataReader.FQUERY_FILECONTENT ) {
							// content_type
							oAttributeNode = oNodeMap.getNamedItem( "content_type" );
							if (oAttributeNode != null) {
								sContentType = oAttributeNode.getNodeValue();
							} else {
								sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0014" );
								logger.error( sErrMsg );
								throw new DFLoaderException( sErrMsg, sFunctionName );						
							}
							
							if ( sContentType.equalsIgnoreCase("longvarbinary") ) {
								iContentType = Types.LONGVARBINARY;
							} else if ( sContentType.equalsIgnoreCase("blob") ) {
								iContentType = Types.BLOB;
							} else {
								sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0015" );
								logger.error( sErrMsg );
								throw new DFLoaderException( sErrMsg, sFunctionName );								
							}
						}
					
					} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0016" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );								
					}
									
					// file query의 SQL문을 알아낸다.
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							bFindFileQuery = true;
							sFileQuery = oValueNode.getNodeValue();
							break;
						}
					}

				}
				//====================================
				//	etc query 정보
				//====================================
				else if (sNodeName.equalsIgnoreCase("etc_query") ) {
					// data query의 key column 정보를 알아낸다.
					oNodeMap = oCurNode.getAttributes();
	
//					if ( oNodeMap != null ) {
//						oAttributeNode = oNodeMap.getNamedItem( "count" );
//						if (oAttributeNode != null) {
//							sEtcCount = oAttributeNode.getNodeValue();
//						} else {
//							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0079" );
//							logger.error( sErrMsg );
//							throw new DFLoaderException( sErrMsg, sFunctionName );	
//						}
//					} else {
//							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0080" );
//							logger.error( sErrMsg );
//							throw new DFLoaderException( sErrMsg, sFunctionName );
//					}
									
					// data query의 SQL문을 알아낸다.
					oSubList = oCurNode.getChildNodes();
					
					if ( oSubList.getLength() > 0 ) mapEtcQuery = new HashMap<String,String>();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						sNodeName = oValueNode.getNodeName();
						
						//logger.info( "etc node name : " + sNodeName );
						
						if ( sNodeName.indexOf("query_") == 0) {
							oSubSubList = oValueNode.getChildNodes();
							for ( int k=0; k < oSubSubList.getLength(); k++ ) {
								oSubValueNode = oSubSubList.item(k);
								if ( oSubValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
									mapEtcQuery.put(sNodeName,oSubValueNode.getNodeValue());
									//break;
								}
							}
						}
					}
				}
				//====================================
				//	previous query 정보
				//====================================
				else if (sNodeName.equalsIgnoreCase("pre_query") ) {
					// data query의 key column 정보를 알아낸다.
					oNodeMap = oCurNode.getAttributes();
	
					if ( oNodeMap == null ) {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0010" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );
					}
									
					// previous query의 SQL문을 알아낸다.
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							sPreQuery = oValueNode.getNodeValue();
							break;
						}
					}
				}
			} // for
		} // for
		
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// 추출한 정보의 타당성을 조사...
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// query_info 노드를 찾았는지...
		if ( bFindQuery == false ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0017" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// key_query가 정의 되었는지 ...	
		if ( bFindKeyQuery == false ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0018" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// key_inc_query가 정의 되었는지 ...	
		if ( bFindKeyIncQuery == false ) {
			sErrMsg = "(Tag:key_inc_query)로 부터 SQL문을 얻는데 실패했습니다.";
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// data_query가 정의 되었는지 ...		
		if ( bFindDataQuery == false ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0019" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// file_query가 정의 되었는지 ...			
		if ( bFindFileQuery == false ) {
			logger.warn( Localization.getMessage( DFLoader.class.getName() + ".Logger.0020" ) );
		}
		
		// timestamp column은 key_inc_query 사용하면서 제외함.
//		// timestamp 컬럼이 정의 되었는지 ...
//		if ( oTimeStamps.size() == 0 ) {
//			logger.warn( Localization.getMessage( DFLoader.class.getName() + ".Logger.0021" ) );
//		}
			
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// DataReader 클래스의 Instance를 생성...
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
		RdbmsDataReader oDataReader = null;
		// DataReader 클래스명을 알아낸다.
		String sDataReaderClassName = this.getDataSourceDataReaderClassName();
		// 최종문서색인일 정보를 알아낸다.
		String sLastDocIndexDate = this.getDataSourceLastDocIndexdate();
		
		logger.info ( Localization.getMessage( DFLoader.class.getName() + ".Logger.0022", sDataReaderClassName ) );		
		try {
			// 클래스 생성
			Class oLoadClass = Class.forName( sDataReaderClassName );
			
			// Consturctor Parameter Value 값을 설정
			Object [] aConParamValues = new Object[15];
			
			aConParamValues[0]  = sPreQuery;
			aConParamValues[1]  = sPreKeyCols;
			aConParamValues[2]  = sKeyQuery;
			aConParamValues[3]  = sKeyCols;
			aConParamValues[4]  = sKeyIncQuery;
			aConParamValues[5]  = sKeyIncCols;
			aConParamValues[6]  = sDataQuery;
			aConParamValues[7]  = sDataKeyCols;
			aConParamValues[8]  = sDataMultiCols;
			aConParamValues[9]  = sFileQuery;
			aConParamValues[10]  = sFileKeyCols;
			aConParamValues[11] = sLastDocIndexDate;
			aConParamValues[12] = sKeyDuplicateRemove;
			aConParamValues[13] = sKeyIncDuplicateRemove;
			aConParamValues[14] = mapEtcQuery;
			
//			aConParamValues[0] = sKeyQuery;
//			aConParamValues[1] = sDataQuery;
//			aConParamValues[2] = sDataKeyCols;
//			aConParamValues[3] = sFileQuery;
//			aConParamValues[4] = sFileKeyCols;
//			aConParamValues[5] = sLastDocIndexDate;
//			aConParamValues[6] = oTimeStamps;
			
			// Constructor Parameter Type을 알아낸다.
			Class[] aConParamTypes = new Class[aConParamValues.length];
			
			aConParamTypes[0]  = Class.forName("java.lang.String");
			aConParamTypes[1]  = Class.forName("java.lang.String");
			aConParamTypes[2]  = Class.forName("java.lang.String");
			aConParamTypes[3]  = Class.forName("java.lang.String");
			aConParamTypes[4]  = Class.forName("java.lang.String");
			aConParamTypes[5]  = Class.forName("java.lang.String");
			aConParamTypes[6]  = Class.forName("java.lang.String");
			aConParamTypes[7]  = Class.forName("java.lang.String");
			aConParamTypes[8]  = Class.forName("java.lang.String");
			aConParamTypes[9]  = Class.forName("java.lang.String");
			aConParamTypes[10] = Class.forName("java.lang.String");
			aConParamTypes[11] = Class.forName("java.lang.String");
			aConParamTypes[12] = Class.forName("java.lang.String");
			aConParamTypes[13] = Class.forName("java.lang.String");
			aConParamTypes[14] = Class.forName("java.util.Map");
			//aConParamTypes[6] = Class.forName("java.util.List");
			
			
			// Constructor를 알아낸다.
			Constructor oDataReaderConstructor = oLoadClass.getConstructor(aConParamTypes);
			// 알아낸 Constructor객체를 이용하여 동적객체를 생성
			oDataReader = (RdbmsDataReader)oDataReaderConstructor.newInstance(aConParamValues);
		
		} catch ( Exception  e ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0023", sDataReaderClassName );
			logger.error( sErrMsg, e );
			throw new DFLoaderException( sErrMsg, sFunctionName );			
		}

		logger.info ( Localization.getMessage( DFLoader.class.getName() + ".Logger.0024", sDataReaderClassName ) );	
		
		// 이전 객체생성소스...
		// oDataReader = new RdbmsDataReader(sKeyQuery, sDataQuery, sDataKeyCols, sFileQuery, sFileKeyCols, sLastDocIndexDate);
		//} catch ( SQLException se ) {
		//	sErrMsg = "RdbmsDataReader객체를 만들지 못했습니다.";
		//	throw new DFLoaderException( sErrMsg, sFunctionName  );
		//}
		
		if ( sFileQuery != null ) {
			oDataReader.setFileQueryType( iQueryType );
			oDataReader.setFileContentType( iContentType );
		}
		
		return oDataReader;
	}
	
	/**
	* DF파일로부터 Notes 쿼리정보를 알아낸다.
	* <p>
	* @return NotesDataReader 객체
	**/		

	private NotesDataReader getDataSourceNotesDataReader() 
	throws DFLoaderException 
	{
		String sNodeName;
		Node oCurNode;
		Node oValueNode;
		String sErrMsg= null;
		String sFunctionName = "getDataSourceRdbmsDataReader()";
		
		String sDataQuery = null;
		String sDocCountColumnName = null;
		String sModifiedTimeColumnName = null;		
		
		/*
		 * 전역영역과 데이터소스영역에서 같은 쿼리정보를 정의했다면
		 * 전역영역의 쿼리정보보다  데이터소스영역의 쿼리정보가 우선한다.
		 * for문에서 전역영역을 처리한 후 데이터소스영역을 처리해서 overwirte되기 때문이다.
		 * 우선순위를 바꾸고 싶다면 배열에 넣는 순서를 바꾸기만 하면 된다.
		 */
		Node [] node = new Node [2];
		node[0] = mo_globalQueryInfo;		// 전역영역의 쿼리정보
		node[1] = getDataSourceNode( DFLoader.DATASOURCE_INFO_QUERY ); // 데이터소스영역의 쿼리정보
		
		
		// Loop내 필요한 변수 선언 
		boolean bFindQuery = false;
		boolean bFindDataQuery = false;
		
		NodeList oSubList = null;
		NodeList nodelist = null;

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// DF의 전역영역과 데이터소스영역에서 쿼리문 정보를 추출한다.
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~		
		for ( int inode =0; inode < node.length; inode ++) {
			
			if ( node[ inode ] == null ) {
				continue;	// 다음 queyr_info 
			} else {
				bFindQuery = true;
			}
			
			nodelist = node[ inode ].getChildNodes();
			
			for ( int i=0; i< nodelist.getLength(); i++ ) {
				oCurNode = nodelist.item(i);
				sNodeName = oCurNode.getNodeName();
				
				// Loop내 필요한 변수 초기화
				oSubList = null;
	
				//====================================
				//	data query 정보
				//====================================
				if (sNodeName.equalsIgnoreCase("data_query") ) {
					oSubList = oCurNode.getChildNodes();
					
					for ( int j=0; j < oSubList.getLength(); j++ ) {
						oValueNode = oSubList.item(j);
						
						if ( oValueNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
							bFindDataQuery = true;
							sDataQuery = oValueNode.getNodeValue();
							break;
						}
					}
				}

				//====================================
				//	doccount query 정보
				//====================================
				if (sNodeName.equalsIgnoreCase("doccount_query") ) {
					oSubList = oCurNode.getChildNodes();
					
					NamedNodeMap oNodeMap = oCurNode.getAttributes();
					Node oAttributeNode = null;
					
					if ( oNodeMap != null ) {
						oAttributeNode = oNodeMap.getNamedItem( "doccunt_column" );
						if (oAttributeNode != null) {
							sDocCountColumnName = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0025" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}
						
						if ( sDocCountColumnName != null && sDocCountColumnName.length() == 0 ) {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0026" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}
						
						oAttributeNode = oNodeMap.getNamedItem( "modifiedtime_column" );
						if (oAttributeNode != null) {
							sModifiedTimeColumnName = oAttributeNode.getNodeValue();
						} else {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0027" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}
						
						if ( sModifiedTimeColumnName !=null && sModifiedTimeColumnName.length() == 0 ) {
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0028" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );						
						}							
					}		
				}			
			} // for
		} // for
		
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// 추출한 정보의 타당성을 조사...
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// query_info 노드를 찾았는지...
		if ( bFindQuery == false ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0029" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// data_query가 정의 되었는지 ...		
		if ( bFindDataQuery == false ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0030" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}

	
		//====================================
		//	최종문서색인일/최종문서조회수 색인일정보
		//====================================		
		String sLastDocIndexDate = this.getDataSourceLastDocIndexdate();
		String sLastDoccountIndexDate = this.getDataSourceLastDoccountIndexdate();
		
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// DataReader 클래스의 Instance를 생성...
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~			
		return new NotesDataReader(sDataQuery, 
									sLastDocIndexDate, 
									sLastDoccountIndexDate,
									sDocCountColumnName,
									sModifiedTimeColumnName	);
	}

	/**
	* DF파일로부터 컬럼매칭정보를 알아낸다.
	* <p>
	* @return ColumnMatching 객체
	**/			
	public ColumnMatching getDataSourceColumnMatching()
	throws DFLoaderException
	{
		Node oCurNode;
		Node oAttributeNode;
		ColumnMatching oColumnMatching;

		String sErrMsg = null;
		String sFunctionName = "getDataSourceColumnMatching()";
		
		int iTargetColumnCnt = 0;
		
		Node node = getDataSourceNode( DFLoader.DATASOURCE_INFO_COLUMNMATCHING );		
		if ( node == null ) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0031" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		oColumnMatching = new ColumnMatching();		
		NodeList nodelist = node.getChildNodes();
		int iKeyColumnCnt = 0;
		TargetColumn oTargetColumn = null;
		
		for ( int i=0; i< nodelist.getLength(); i++ ) {
			String sSSColumnName = null;
					
			oCurNode = nodelist.item(i);
			
			// target_column을 찾는다.
			if ( oCurNode.getNodeType() == Node.ELEMENT_NODE && 
						oCurNode.getNodeName().equalsIgnoreCase("target_column") ) {
				iTargetColumnCnt += 1;
				
				
				NamedNodeMap oNodeMap = oCurNode.getAttributes();
				if ( oNodeMap != null ) {
					oAttributeNode = oNodeMap.getNamedItem( "id" );
					if (oAttributeNode != null) {
						// 주의 : 대문자로 변환할것
						//sSSColumnName = oAttributeNode.getNodeValue().toUpperCase();
						sSSColumnName = oAttributeNode.getNodeValue();
						oTargetColumn = new TargetColumn( sSSColumnName );
					} else {
						sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0032" );
						logger.error( sErrMsg );
						throw new DFLoaderException( sErrMsg, sFunctionName );						
					}
					
					oAttributeNode = oNodeMap.getNamedItem( "key" );
					if (oAttributeNode != null) {
						if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "on" ) ) {
							iKeyColumnCnt ++;
							oTargetColumn.setKey();
						}
					}
					
					oAttributeNode = oNodeMap.getNamedItem( "contenttype" );
					if (oAttributeNode != null) {
						if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "namo" ) ) {
							oTargetColumn.setContentType( ColumnMatching.CONTENT_TYPE_NAMO );
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "html" ) ) {
							oTargetColumn.setContentType( ColumnMatching.CONTENT_TYPE_HTML );
						}
					}
						
					oAttributeNode = oNodeMap.getNamedItem( "type" );
					if (oAttributeNode != null) {
						if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "date" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.DATE );
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "integer" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.INTEGER );
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "real" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.REAL );							
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "longtext" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.LONGTEXT );
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "dom_element" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.DOMELEMENT );	
						} else if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "string" ) ) {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.STRING );
						} else {
							oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.STRING );
							sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0033" );
							logger.error( sErrMsg );
							throw new DFLoaderException( sErrMsg, sFunctionName );								
						}
					} else {
						oTargetColumn.setColumnType( com.rayful.bulk.ESPTypes.STRING );
					}
				
					// notnull 여부
					oAttributeNode = oNodeMap.getNamedItem( "notnull" );
					if (oAttributeNode != null) {
						if ( oAttributeNode.getNodeValue().equalsIgnoreCase( "on" ) ) {
							oTargetColumn.setNotNull ();
						}
					}									
				}
				
				NodeList sourceNodelist = oCurNode.getChildNodes();
				Node oSourceCurNode = null;
				String sDbColumnName = null;
				String sNoIndex = null;
				String sColumnText = "";
				String sFileInfoType = null;
				String sSystem = null;
				int iSystemInfoType;
				int iFileInfoType;
				
				SourceColumn oSourceColumn = null;
				
				
				for ( int j=0; j< sourceNodelist.getLength(); j++ ) {
					sDbColumnName = null;
					sNoIndex = null;
					sFileInfoType = null;
					sSystem = null;
					
					iFileInfoType = SourceColumn.FINFOTYPE_UNKNOWN;
					iSystemInfoType = SourceColumn.SINFOYPE_UNKNOWN;
					
					oSourceCurNode = sourceNodelist.item(j);
					
					// source_column을 찾는다.
					if ( oSourceCurNode.getNodeType() == Node.ELEMENT_NODE && 
								oSourceCurNode.getNodeName().equalsIgnoreCase("source_column") ) {	
													
						NamedNodeMap oSourceNodeMap = oSourceCurNode.getAttributes();
						Node oSourceNameAttNode = null;
						Node oSourceNoindexAttNode = null;
						Node oSourceFileInfoTypeAttNode = null;
						Node oSourceNotnullAttNode = null;
						Node oSourceDetaultAttNode = null;
						Node oSourceSystemAttNode = null;
						
						
						if ( oSourceNodeMap != null ) {
							
							// name 값
							oSourceNameAttNode = oSourceNodeMap.getNamedItem( "name" );
							if (oSourceNameAttNode != null) {
								// [ 주의 : 대문자로 변환할것 ]
								//sDbColumnName = oSourceNameAttNode.getNodeValue().toUpperCase();
								sDbColumnName = oSourceNameAttNode.getNodeValue();
								
								// file_info정보
								oSourceFileInfoTypeAttNode = oSourceNodeMap.getNamedItem( "file_info" );
								if ( oSourceFileInfoTypeAttNode != null ) {
									sFileInfoType = oSourceFileInfoTypeAttNode.getNodeValue();
									
									if ( sFileInfoType.equalsIgnoreCase("file_list") ) {
										iFileInfoType = SourceColumn.FINFOTYPE_FILELIST;
									} else if ( sFileInfoType.equalsIgnoreCase("file_content") ) {
										iFileInfoType = SourceColumn.FINFOTYPE_FILECONTENT;
									} else if ( sFileInfoType.equalsIgnoreCase("file_name") ) {
										iFileInfoType = SourceColumn.FINFOTYPE_FILENAME;
									} else {
										sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0034" );
										logger.error( sErrMsg );
										throw new DFLoaderException( sErrMsg, sFunctionName );											
									}
								}
								
								// system 정보
								oSourceSystemAttNode = oSourceNodeMap.getNamedItem( "system" );
								if ( oSourceSystemAttNode != null ) {
									sSystem = oSourceSystemAttNode.getNodeValue();

									if ( sSystem.equalsIgnoreCase("on") ) {
										if ( sDbColumnName.equalsIgnoreCase("@HOSTNAME") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_HOSTNAME;
										} else if ( sDbColumnName.equalsIgnoreCase("@DATASOURCEID") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_DATASURCE_ID;
										} else if ( sDbColumnName.equalsIgnoreCase("@DBNAME") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_DBNAME;
										} else if ( sDbColumnName.equalsIgnoreCase("@NSFNAME") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_NOTES_NSFNAME;
										} else if ( sDbColumnName.equalsIgnoreCase("@UNIVERSALID") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_NOTES_UNVERSALID;
										} else if ( sDbColumnName.equalsIgnoreCase("@FILEID") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_FILEID;
										// 추가할 시스템 정보가 있으면 여기에 추가....
										//} else if ( sDbColumnName.equalsIgnoreCase("SYSTEMINFO") ) {
											
										} else if ( sDbColumnName.equalsIgnoreCase("@MODIFIED") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_NOTES_MODIFIED;
										} else if ( sDbColumnName.equalsIgnoreCase("@CREATED") ) {
											iSystemInfoType = SourceColumn.SINFOYPE_NOTES_CREATED;
										} else {
											sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0035", sDbColumnName );
											logger.error( sErrMsg );
											throw new DFLoaderException( sErrMsg, sFunctionName );											
										}
									}
									
								}								
								
								// noindex여부
								oSourceNoindexAttNode = oSourceNodeMap.getNamedItem( "noindex" );
								if ( oSourceNoindexAttNode != null ) {
									sNoIndex = oSourceNoindexAttNode.getNodeValue();
									
									if ( ! sNoIndex.equalsIgnoreCase("on") && 
											( iFileInfoType == SourceColumn.FINFOTYPE_UNKNOWN ) ) { 
										sColumnText += "[%" + sDbColumnName + "%]";
										oTargetColumn.addSourceColumnNames ( sDbColumnName );	
									}
								} else {
									sColumnText += "[%" + sDbColumnName + "%]";
									oTargetColumn.addSourceColumnNames ( sDbColumnName );
								}
								
								// Source컬럼객체를 생성한다.
								oSourceColumn = new SourceColumn( sDbColumnName, 
																	sSSColumnName, 
																	iFileInfoType, 
																	iSystemInfoType );
					
								// default 값
								oSourceDetaultAttNode = oSourceNodeMap.getNamedItem( "default" );
								if (oSourceDetaultAttNode != null) {
									String sDefaultValue = oSourceDetaultAttNode.getNodeValue();
									oSourceColumn.setDefaultValue ( sDefaultValue );
								}								
								
								// notnull 여부
								oSourceNotnullAttNode = oSourceNodeMap.getNamedItem( "notnull" );
								if (oSourceNotnullAttNode != null) {
									if ( oSourceNotnullAttNode.getNodeValue().equalsIgnoreCase( "on" ) ) {
										oSourceColumn.setNotNull ();
									}
								}
								
								// Source column Map에 추가...
								oColumnMatching.source.put( sDbColumnName, oSourceColumn );
	
							} else {
								sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0036" );
								logger.error( sErrMsg );
								throw new DFLoaderException( sErrMsg, sFunctionName );									
							}
						}						
					} else if ( oSourceCurNode.getNodeType() == Node.TEXT_NODE ) { 
						String sCurColumnText = oSourceCurNode.getNodeValue();
						if ( sCurColumnText != null ) {
							sColumnText +=  sCurColumnText.trim();
						}
					} else if ( oSourceCurNode.getNodeType() == Node.CDATA_SECTION_NODE ) {
						String sCurColumnText = oSourceCurNode.getNodeValue();
						if ( sCurColumnText != null ) {
							sColumnText +=  sCurColumnText;
						}						
					}
					
				} //for - SourceColumn
				oTargetColumn.setColumnText( sColumnText );
				oColumnMatching.target.put( sSSColumnName, oTargetColumn );
			}
		} //for - TargetColumn
		
		
		if (iTargetColumnCnt < 1 ) {
			//에러처리...
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0037" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		// PK컬럼을 설정한다.
		if ( iKeyColumnCnt < 1) {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0038" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		return oColumnMatching;
	}

	/**
	* DF파일로부터 파일접근정보 로드하여 FilesAccessor 객체를 생성한다.
	* 프로코콜에 따라 FtpAccessor, HttpAccessor, FileAccessor객체를 생성하여 리턴한다.
	* <p>
	* @return FilesAccessor 객체
	**/		
	public FilesAccessor getDataSourceFilesAccessor() 
	throws DFLoaderException
	{
		String sErrMsg = null;
		String sFunctionName = "getDataSourceFilesAccessor()";
		
		String sNodeName;
		
		Node oCurNode;
		Node oValueNode;	
		Node oAttributeNode;
		
		String sProtocolName = null;
		String sHostName = null;
		String sPortNumber = null; 
		String sBasePath = null;
		String sAccount = null;
		String sPassword = null;
		
		FilesAccessor oFilesAccessor = null;
		
		Node node = getDataSourceNode( DFLoader.DATASOURCE_INFO_FILEACCESS );			
		if ( node == null ) {
			// 노드를 찾지못해도 예외를 발생시키지 않는다.
			return null;
		}
		
		NodeList nodelist = node.getChildNodes();
		
		// 파일접근 프로토콜을 알아낸다.
		NamedNodeMap oNodeMap = node.getAttributes();
		if ( oNodeMap != null ) {
			oAttributeNode = oNodeMap.getNamedItem( "protocol");
			if (oAttributeNode != null) {
				sProtocolName = oAttributeNode.getNodeValue();
			}			
		}
		
		// 파일에 접근하기위한 정보들을 알아낸다.
		for ( int i=0; i< nodelist.getLength(); i++ ) {
			oCurNode = nodelist.item(i);
			sNodeName = oCurNode.getNodeName();
			

			if ( sNodeName.equalsIgnoreCase("host") ) {
				oValueNode = oCurNode.getFirstChild();
				if ( oValueNode != null ) {
					sHostName = oValueNode.getNodeValue();
				}			
			} else if ( sNodeName.equalsIgnoreCase("port") ) {
				oValueNode = oCurNode.getFirstChild();
				if ( oValueNode != null ) {
					sPortNumber = oValueNode.getNodeValue();
				}			
			} else if ( sNodeName.equalsIgnoreCase("userid") ) {
				oValueNode = oCurNode.getFirstChild();
				if ( oValueNode != null ) {
					sAccount = oValueNode.getNodeValue();
				}
			}else if ( sNodeName.equalsIgnoreCase("password") ) {
				oValueNode = oCurNode.getFirstChild();
				if ( oValueNode != null ) {
					sPassword = oValueNode.getNodeValue();
				}
			} else if ( sNodeName.equalsIgnoreCase("basepath") ) {
				oValueNode = oCurNode.getFirstChild();
				if ( oValueNode != null ) {
					sBasePath = oValueNode.getNodeValue();
				} else {
					sBasePath = "/";
				}
			} 
		}
		
		// 프로토콜에 따라 다른 파일처리객체를 생성
		if ( sProtocolName.equalsIgnoreCase("FTP") ) {
			
			// FtpAccessor Instance 생성
			oFilesAccessor = new FtpAccessor ( sHostName, 
												sPortNumber, 
												sBasePath, 
												sAccount, 
												sPassword );		
																			
		} else if ( sProtocolName.equalsIgnoreCase("HTTP") ) { 
			
			// HttpAccessor Instance 생성
			oFilesAccessor = new HttpAccessor ( sHostName, 
												sPortNumber, 
												sBasePath );
																			
		} else if ( sProtocolName.equalsIgnoreCase("FILE") ) { 
			// FileAccesor Instance 생성
			oFilesAccessor = new FileAccessor ( sBasePath );			
			
		} else {
			sErrMsg = Localization.getMessage( DFLoader.class.getName() + ".Logger.0039" );
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}

		return oFilesAccessor;
	}	
	
	/**
	* DF파일로부터 Indexer 클래스명을 알아낸다.
	* <p>
	* @return Indexer 클래스명
	**/	
	public String getDataSourceIndexerClassName() 
	throws DFLoaderException
	{
		
		String sClassName = null;
		
		Node oIndexerNode = getDataSourceNode( DFLoader.DATASOURCE_INFO_INDEXER );			

		if ( oIndexerNode != null ) {
			// Indexer 클래스명을 알아낸다.
			NamedNodeMap oNodeMap = oIndexerNode.getAttributes();
			Node oAttributeNode;
			if ( oNodeMap != null ) {
				oAttributeNode = oNodeMap.getNamedItem( "class_name" );
				if (oAttributeNode != null) {
					sClassName = oAttributeNode.getNodeValue();
				}	//if (oAttributeNode != null)
			}	//if ( oNodeMap != null )
		} //if ( oNode != null )		
		
		
		// 기본 Indexer 클래스명을 설정한다.
		if ( sClassName == null ) {
			if ( this.getDataSourceType() == DFLoader.DATASOURCE_TYPE_RDBMS	) {
				sClassName = "com.rayful.bulk.index.indexer.RdbmsIndexer";
			} else {
				sClassName = "com.rayful.bulk.index.indexer.NotesIndexer";
			}
		}
		
		return sClassName;
	}		

	
	/**
	* DF파일로부터 ConvertCustomizer 클래스명을 알아낸다.
	* <p>
	* @return ConvertCustomizer 클래스명
	**/	
	public String getDataSourceConvertCustomizerClassName() 
	{
		String sClassName = null;
		Node oNode = getDataSourceNode( DFLoader.DATASOURCE_INFO_CUSTOMIZER );			

		if ( oNode != null ) {
			// ConvertCustomizer 클래스명을 알아낸다.
			NamedNodeMap oNodeMap = oNode.getAttributes();
			Node oAttributeNode;
			if ( oNodeMap != null ) {
				oAttributeNode = oNodeMap.getNamedItem( "class_name" );
				if (oAttributeNode != null) {
					sClassName = oAttributeNode.getNodeValue();
				}	//if (oAttributeNode != null)
			}	//if ( oNodeMap != null )
		} //if ( oNode != null )		

		return sClassName;
	}	
	
	/**
	* DF파일로부터 DataReader 클래스명을 알아낸다.
	* <p>
	* @return DataReader 클래스명
	**/	
	public String getDataSourceDataReaderClassName() 
	throws DFLoaderException
	{
		String sClassName = null;
		Node oNode = getDataSourceNode( DFLoader.DATASOURCE_INFO_DATAREADER );			

		if ( oNode != null ) {
			// DataReader 클래스명을 알아낸다.
			NamedNodeMap oNodeMap = oNode.getAttributes();
			Node oAttributeNode;
			if ( oNodeMap != null ) {
				oAttributeNode = oNodeMap.getNamedItem( "class_name" );
				if (oAttributeNode != null) {
					sClassName = oAttributeNode.getNodeValue();
				}	//if (oAttributeNode != null)
			}	//if ( oNodeMap != null )
		} //if ( oNode != null )		
		
		
		// 기본 DataReader 클래스명을 설정한다.
		if ( sClassName == null ) {
			if ( this.getDataSourceType() == DFLoader.DATASOURCE_TYPE_RDBMS	) {
				sClassName = "com.rayful.bulk.sql.RdbmsDataReader";
			} else {
				sClassName = "com.rayful.bulk.sql.NotesDataReader";
			}
		}
		return sClassName;
	}
	
	/**
	* DF파일로부터 EtcReader 클래스명을 알아낸다.
	* <p>
	* @return DataReader 클래스명
	**/	
	public String getDataSourceEtcReaderClassName() 
	throws DFLoaderException
	{
		String sClassName = null;
		Node oNode = getDataSourceNode( DFLoader.DATASOURCE_INFO_ETCREADER );			

		if ( oNode != null ) {
			// SSWriter 클래스명을 알아낸다.
			NamedNodeMap oNodeMap = oNode.getAttributes();
			Node oAttributeNode;
			if ( oNodeMap != null ) {
				oAttributeNode = oNodeMap.getNamedItem( "class_name" );
				if (oAttributeNode != null) {
					sClassName = oAttributeNode.getNodeValue();
				}	//if (oAttributeNode != null)
			}	//if ( oNodeMap != null )
		} //if ( oNode != null )		
		
		
		return sClassName;
	}
	
	/**
	 * DF Name을 리턴
	 * 
	 * @return DF Name
	 */
	public String getDFName() 
	{
		return ms_DFName;
	}
	
	/**
	* DF에 global Connection정보가 정의되었는지 여부
	* <p>
	* @return	ture: 정의됨 / false: 정의안됨
	**/	
	public boolean isDefinedGlobalConnect() 	
	{
		if ( mo_globalConnectInfo != null ) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	* 데이터소스 노드 하나를 삭제한다.
	* <p>
	* @param	oNode	삭제할노드
	**/	
	private void removeDataSource( Node oNode )
	{
		if ( oNode != null ) {
			Node oParentNode = oNode.getParentNode();
			if ( oParentNode != null ) {
				oParentNode.removeChild( oNode );
				mb_dfModified = true;	//DF가 변경됨을 표시
			}
		}
	}	
}

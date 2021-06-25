package com.rayful.bulk.index.indexer;
/**
 *******************************************************************
 * 파일명 : MailDatasourceUpdator.java
 * 파일설명 : 색인대상DB가 가변적으로 변하는 색인대상을 DF에 Update한다.
 * 						컬럼매칭은 동일하고 데이터소스만 유동적인경우에 한해 
 *						이 프로그램을 이용할 수 있다.
 *******************************************************************
 * 작성일자		작성자	내용
 * -----------------------------------------------------------------
 * 2005/08/01	정충열	최초작성
 * 2010/12/06	opensky	DF Update logic edit
 *******************************************************************
*/


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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

import lotus.domino.Database;
import lotus.domino.DocumentCollection;
import lotus.domino.NotesException;

import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Comment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoaderException;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.NotesConnector;
import com.rayful.bulk.sql.NotesSessionPool;
import com.rayful.bulk.sql.RdbmsConnector;

/**
* 색인대상DB가 가변적으로 변하는 색인대상의 경우 가변적인 색인대상을 찾아내 DF에 반영시킨다.
* 추가작업 : 데이터소스를 자동변경하려는 색인 추가시  ( "// %%%"로 변경포인트 표시 )
* 				1. 상수정의를 추가한다.
* 				2. main()에  색인명 체그하는 로직에 추가
* 				3. updateDataSourceXXX : XXX=색인파일명 의 함수작성
* 				4. updateDataSource에서 새로 생성한 updateDataSourceXXX를 호출하도록함.
**/
public class DataSourceUpdator 
{
	private static String ms_hostName = null;
	private static String ms_account = null;
	private static String ms_password = null;
	
	private static String ms_collectionName = null;
	
	private static boolean mb_dfModified = false;

	private static File mo_orgDFFile = null;
	
	
	private static org.w3c.dom.Document mo_document = null;
	private static Node mo_sourceNode = null;
	
	private static Map<String, Node> mo_datasourceMap = new HashMap<String, Node>();
		
	/** logger */
	static Logger logger = null;	
	
	/**
	*	프로그램 사용법을 콘솔에 출력한다.
	*/
	private static void printUsage() {
		// Parameter를 지정하지 않으면 프로그램을 종료합니다.
		System.out.println( "Desc.: Automatic update Datasource of DF");
		System.out.println( "      Only use Mail / Board ");
		System.out.println( "Usage: java DataSourceUpdator DF_Template mail|board");
		System.exit(1);
	}
	

	/**
	*	프로그램 로직을 수행
	* @param	argv	실행 Parameter
	*/
	public static void main ( String [] argv ) {
		Connection oMetaDbConnection = null;
		RdbmsConnector mo_sourceConnector = null;
		PreparedStatement oPstmt = null;
		ResultSet oRs = null;
		
		Database oDatabase = null;
		DocumentCollection oDocCollection = null;
		lotus.domino.Document oDocument = null;
		String sCategory = null;
	
		//=============================================================================
		// Parameter체크
		//=============================================================================		
		if ( argv.length < 2 || argv.length > 3 ) {
			printUsage();
		}
		
		ms_collectionName = argv[argv.length - 2 ];
		if ( ms_collectionName == null ) {
			printUsage();
		}
		
		sCategory = argv[argv.length - 1];
		if ( sCategory == null ) {
			printUsage();
		}
		
		if ( ( "mail".equalsIgnoreCase(sCategory) || "board".equalsIgnoreCase(sCategory) ) == false ) {
			printUsage();
		}
		
		ms_collectionName = ms_collectionName.toLowerCase();	//소문자로 변경
		sCategory = sCategory.toLowerCase();
		
		//=============================================================================
		// properites 파일로 부터 색인에 필요한 설정정보를 알아내어 Config에 세팅.
		//=============================================================================	 
		try {
			String sPropFileName = "Indexer.properties" ;

			ClassLoader cl;
			cl = Thread.currentThread().getContextClassLoader();
			if( cl == null )
				cl = ClassLoader.getSystemClassLoader();                

			java.net.URL oUrl = cl.getResource( sPropFileName );

			if ( oUrl == null ) {
				throw new FileNotFoundException("Could not found Property File(Indexer.properties)");
			}
	  	
			sPropFileName = oUrl.getPath();
			Config.load( sPropFileName, ms_collectionName );
		} catch( FileNotFoundException fnfe ) {
			System.out.println (fnfe);
			printUsage();
		} catch( IOException ioe ) {
			System.out.println (ioe);
			printUsage();
		} catch( Exception e) {
			System.out.println (e);
			printUsage();
		}
		
		//=============================================================================
		// logger설정
		//=============================================================================			
		String sLogFileName = "DS_" + ms_collectionName + ".log";
		File oLogFile = new  File ( Config.SUMMARYLOG_PATH, sLogFileName );
		logger = RayfulLogger.getLogger( Bulker.class.getName(), oLogFile.getPath() );		
	
		//=============================================================================
		// DF load...
		//=============================================================================	
		String sDFFileName = ms_collectionName + "_template.xml";
		mo_orgDFFile = new File ( Config.DEFINITION_PATH, sDFFileName );	

		try {
			loadDF( mo_orgDFFile.getPath() );
		} catch ( DFLoaderException de ) {
			logger.fatal ( "Error : loading DF - ", de );
			System.exit(1);
		}
					
		logger.info ( "\n\n >>>Collection Name : Update datasource of " + ms_collectionName );
		
		logger.info ( "mo_datasourceMap.size()=" + mo_datasourceMap.size() );
		
		try {
				
			try {
				String sSql = null;
				String sNSFName = null;
				
				if ( "mail".equalsIgnoreCase(sCategory) ) {
					//sSql = "SELECT NSF FROM TB_NSFLIST WHERE CATEGORYTOP = 'mail' AND DEL_FLAG = 'N' ORDER BY NSF";
					sSql = "SELECT NSF FROM TB_NSFLIST WHERE CATEGORYTOP = 'mail' AND DEL_FLAG = 'N' AND charindex('nsf',nsf) > 0 ORDER BY NSF";
				} else if ( "board".equalsIgnoreCase(sCategory) ) {
					sSql = "SELECT NSF FROM TB_NSFLIST WHERE CATEGORYTOP = 'collaboration' AND CATEGORYMID = 'board' AND DEL_FLAG = 'N' ORDER BY NSF";
				}
				
				mo_sourceConnector = (RdbmsConnector)new RdbmsConnector( 
						Config.METADB_DRIVERCLASS ,
						Config.METADB_URL ,
						Config.METADB_USER ,
						Config.METADB_PASS );
				
				if (mo_sourceConnector != null) {
					oMetaDbConnection = mo_sourceConnector.getConnection();
				}
				
				oPstmt = oMetaDbConnection.prepareStatement( sSql );
				oRs = oPstmt.executeQuery();
				
				int iCnt = 1;
				while ( oRs.next() ) {
					sNSFName = oRs.getString( "NSF" );
					
					logger.info ( "************************************************");
					logger.info ( "* [" + iCnt + "] target NSF path : "  + sNSFName);
					logger.info ( "************************************************");
					
					updateDataSource( sNSFName );
					
					iCnt++;
					
					//if (iCnt >0) break;
					
				} // while											
				
			} catch ( SQLException se ) {
				logger.fatal( "\t(Management DB)Connection or Status Query Error", se );
				throw se;
			}
			
			
			//=============================================================================
			// DF에 저장한다.
			//=============================================================================
			String sSaveDFFileName = ms_collectionName + ".xml";
	  		File oSaveDFFileName = new File ( Config.DEFINITION_PATH, sSaveDFFileName );
	  		
			saveDF( oSaveDFFileName.getPath() );			
		
			logger.info ( ">>> Datasource Update normally. ");
			
		} catch ( Exception e ) {
			if ( oRs != null ) { try { oRs.close(); } catch( SQLException se ) {} }
			if ( oPstmt != null ) { try { oPstmt.close(); } catch( SQLException se ) {} }
			if ( oMetaDbConnection != null ) { try { oMetaDbConnection.close(); } catch( SQLException se ) {} }
			
			if ( oDatabase != null ) { try { oDatabase.recycle(); } catch( NotesException se ) {} }
			if ( oDocCollection != null ) { try { oDocCollection.recycle(); } catch( NotesException se ) {} }
			if ( oDocument != null ) { try { oDocument.recycle(); } catch( NotesException se ) {} }		
			NotesSessionPool.clearSession();	//노츠의 세션객체를 Clear...
						
			logger.error ( ">>> abnormally ", e );
			System.exit(1);
		}
	}
	
	/**
	 *	Mail DF의 data_source태그들을 수정한다.
	 * @param	sNSFName	대상 메일 NSF 경로
	 */			
	private static void updateDataSource ( String sNSFName ) 
	throws NotesException, DFLoaderException
	{
		
		String sFileDesc = sNSFName;
		String sDelete = "N";
			
		updateDF( ms_hostName, sNSFName, ms_account, ms_password, sFileDesc, sDelete );
		
	}
	
	/**
	*	해당색인의 DF를 로드한다.
	* @param	sDFFileName	DF파일명
	*/	
	public static void loadDF( String sDFFileName )
	throws DFLoaderException
	{
		DocumentBuilderFactory oFactory = null;
		DocumentBuilder oBuilder = null;
		
		String sErrMsg = null;
		String sFunctionName = "loadDF()";
		
		try {
			oFactory = DocumentBuilderFactory.newInstance();
			oBuilder = oFactory.newDocumentBuilder();
		} catch ( ParserConfigurationException pce ) {
			// Parser with specified options can't be built
			sErrMsg = "Error creating DF Builder";
			throw new DFLoaderException( sErrMsg, sFunctionName );
		} 
		
		try {
			
			mo_document = oBuilder.parse( new File(sDFFileName) );
			
			loadSourceNode();
			loadDatasourceNode();
			
		} catch (SAXException sxe) {
			// Error generated during parsing
			Exception ox = sxe;
			if (sxe.getException() != null)
			{
				ox = sxe.getException();
			}
			ox.printStackTrace();
			sErrMsg = "Error Parsing DF";
			throw new DFLoaderException( sErrMsg, sFunctionName );

		} catch (IOException ioe) {
			// I/O error
			sErrMsg = "Could not find DF file";
			throw new DFLoaderException( sErrMsg, sFunctionName );
		} catch ( DFLoaderException de ) {
			throw de;
		}
	}	
	
	/**
	*	변경된사항을 DF에 저장한다.
	* @param	sDFFileName	DF파일명
	*/		
	public static void saveDF ( String sDFFFileName ) 
	throws DFLoaderException
	{
		String sErrMsg = null;
		String sFunctionName = "saveDF()";
				
		try {
			if ( ! mb_dfModified ) {
			//	// df가 수정되지 않았다면 함수를 종료한다.
				return;
			}
			
			if ( mo_document == null ) {
				throw new DFLoaderException( "Could not reference lotus.domino.Document.", sFunctionName);
			}
			
			File oDFFile = new File( sDFFFileName );
			
		   // Use a Transformer for output
		  TransformerFactory tFactory = TransformerFactory.newInstance();
		  Transformer transformer = tFactory.newTransformer();
		
		  DOMSource oDomSource = new DOMSource(mo_document);
		  StreamResult oStreamResult = new StreamResult(oDFFile);
		  
		  // EUC-KR로 Encoding하기위해서 반드시 필요
		  transformer.setOutputProperty("encoding", "EUC-KR");
		  
		  // Dom을 파일로 저장시킨다.
		  transformer.transform(oDomSource, oStreamResult); 
		  
		  logger.info ( "Apply update in DF");
				
		} catch (TransformerConfigurationException tce) {
		
		   // Use the contained exception, if any
		  Throwable x = tce;
		  if (tce.getException() != null) {
		    x = tce.getException();
		  }
		  sErrMsg = "Error saving DF";
		  logger.error ( sErrMsg, x );
		  throw new DFLoaderException ( sErrMsg,sFunctionName );
		  
		} catch (TransformerException te) {
		  // Error generated by the parser
		  // Use the contained exception, if any
		  Throwable x = te;
		  if (te.getException() != null) {
		    x = te.getException();
		  }
		    
		  sErrMsg = "Error saving DF";
		  logger.error ( sErrMsg, x );
		  throw new DFLoaderException ( sErrMsg,sFunctionName );
		}
	}	
	
	
	public static void loadSourceNode() 
	throws DFLoaderException
	{
		Element oElem;
		NodeList oNodeList;
		
		String sErrMsg = null;
		String sFunctionName = "loadSourceNode()";
				
		oElem = mo_document.getDocumentElement();
		oNodeList = oElem.getElementsByTagName( "source");
		if ( oNodeList == null ) {
			sErrMsg = "Could not find (Tag:source)";
			logger.error ( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		mo_sourceNode = oNodeList.item(0);
		if ( mo_sourceNode == null ) {
			sErrMsg = "Could not find (Tag:source)";
			logger.error( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );			
		}
	}
	
	public static void loadDatasourceNode() 
	throws DFLoaderException
	{
		String sErrMsg = null;
		String sFunctionName = "loadDatasourceNode()";
		
		Element oElem = mo_document.getDocumentElement();
		NodeList oNodeList = oElem.getElementsByTagName( "data_source");
		if ( oNodeList == null ) {
			sErrMsg = "Could not find (Tag:data_source)";
			logger.error ( sErrMsg );
			throw new DFLoaderException( sErrMsg, sFunctionName );
		}
		
		Node oDsNode = oNodeList.item(0);
		NamedNodeMap oNodeMap = null;
		Node oAttributeNode = null;
		String sDatasourceId = null;
		
		while ( oDsNode != null ) {
			// [DataSource] 노드의 속성을 구한다
			if ( oDsNode.getNodeName().equals("data_source") ) {
				oNodeMap = oDsNode.getAttributes();
		
				// id값을 알아낸다.
				oAttributeNode = oNodeMap.getNamedItem( "id" );
				sDatasourceId = oAttributeNode.getNodeValue();

				// 데이터소스ID와 Node와 Datasource Node 매핑정보 저장
				mo_datasourceMap.put( sDatasourceId, oDsNode );
			} 
			
			
			NodeList oDsChildList = oDsNode.getChildNodes();
			Node oDsChildNode = null;
			Node oConChildNode = null;
			Node oValueNode = null;
			String sNodeName = null;
			
			for ( int i=0; i< oDsChildList.getLength(); i++ ) {
				oDsChildNode = oDsChildList.item(i);
				sNodeName = oDsChildNode.getNodeName();
				
				if (sNodeName.equalsIgnoreCase("connect_info") ) {
					NodeList oConChildlList = oDsChildNode.getChildNodes();
					
					for ( int j=0; j< oConChildlList.getLength(); j++ ) {
						oConChildNode = oConChildlList.item(j);
						sNodeName = oConChildNode.getNodeName();
						
						if (sNodeName.equalsIgnoreCase("hostname") ) {
							oValueNode = oConChildNode.getFirstChild();
							if (oValueNode != null ) {
								ms_hostName = oValueNode.getNodeValue();
							}
						}
						
						if (sNodeName.equalsIgnoreCase("userid") ) {
							oValueNode = oConChildNode.getFirstChild();
							if (oValueNode != null ) {
								ms_account = oValueNode.getNodeValue();
							}
						}
						
						if (sNodeName.equalsIgnoreCase("password") ) {
							oValueNode = oConChildNode.getFirstChild();
							if (oValueNode != null ) {
								ms_password = oValueNode.getNodeValue();
							}
						}
						
					} // for j
				}
				
			}	// for i
			
			// 다음 데이터소스를 찾는다.
			oDsNode = oDsNode.getNextSibling();
		}
	}
	
	public static Node getDatasourceNode( String sDatasourceId ) 
	{
		return ( Node ) mo_datasourceMap.get( sDatasourceId );
	}
	
	
	
	public static void updateDF( String sServerName,
								String sDBPath,
								String sUserID,
								String sPass,
								String sDesc,
								String sDelete )
	{
		
		//Utils oUtils = new Utils();
		
		String sDBName = NotesConnector.getNsfName( sDBPath );
		Node oDataSourceNode= null;
		Node oTextNode = null;
		
		oDataSourceNode = getDatasourceNode( sDBName ) ;
		
		// 데이터소스를 DF에서 못찾는다면.... insert
		if ( oDataSourceNode == null ) {
			
			if ( sDelete.equalsIgnoreCase("Y") ) {
				// 삭제처리된경우는 Insert하지 않는다.
				return;
			}
			
			// ----------------------------------
			// <data_source ></data_source>
			// ----------------------------------
			// data_source(Element)
			Element oNewDataSourceElem = mo_document.createElement("data_source");
			// Comment 
			Comment oComment = mo_document.createComment( sDesc );
			// id(Attr)
			Attr oDataSourceId = mo_document.createAttribute("id");
			oDataSourceId.setValue ( sDBName );
			// source_type(Attr)
			Attr oSourceType = mo_document.createAttribute("source_type");
			oSourceType.setValue ( "notes" );
			// last_doc_indexdate(Attr)
			Attr oLastDocIndexdate = mo_document.createAttribute("last_doc_indexdate");
			//oLastDocIndexdate.setValue(oUtils.dateFormatString(new java.util.Date()));
			oLastDocIndexdate.setValue("1970-01-01 00:00:01");
			// last_doccount_indexdate(Attr)
			Attr oLastDoccountIndexdate = mo_document.createAttribute("last_doccount_indexdate");
			oLastDoccountIndexdate.setValue("1970-01-01 00:00:01");
			// state 
			Attr oState = mo_document.createAttribute("state");
			oState.setValue ( "N" );
			// content_deltag
			Attr oDelTag = mo_document.createAttribute("content_deltag");
			oDelTag.setValue ( "on" );			
			
			oNewDataSourceElem.setAttributeNode ( oDataSourceId );
			oNewDataSourceElem.setAttributeNode ( oSourceType );
			oNewDataSourceElem.setAttributeNode ( oState );
			oNewDataSourceElem.setAttributeNode ( oLastDocIndexdate );
			oNewDataSourceElem.setAttributeNode ( oLastDoccountIndexdate );
			oNewDataSourceElem.setAttributeNode ( oDelTag );
			
			oNewDataSourceElem.appendChild( mo_document.createTextNode("\n\t\t") );
			oNewDataSourceElem.appendChild ( oComment );
			
			// ----------------------------------
			// <connect_info ></connect_info>
			// ----------------------------------					
			//connect_info(Element)
			Element oNewConnectInfo = mo_document.createElement("connect_info");
			//hostname(Element)
			
			Element oNewHostNameElem = mo_document.createElement("hostname");
			oTextNode = mo_document.createTextNode( sServerName );
			oNewHostNameElem.appendChild( oTextNode );
			//dbname(Element)
			Element oNewDbNameElem = mo_document.createElement("dbname");
			oTextNode = mo_document.createTextNode( sDBPath );
			oNewDbNameElem.appendChild( oTextNode );
			//userid(Element)
			Element oNewUseridElem = mo_document.createElement("userid");
			oTextNode = mo_document.createTextNode( sUserID );
			oNewUseridElem.appendChild( oTextNode );			
			//password(Element)
			Element oNewPasswordElem = mo_document.createElement("password");
			oTextNode = mo_document.createTextNode( sPass );
			oNewPasswordElem.appendChild( oTextNode );
			
			oNewConnectInfo.appendChild( mo_document.createTextNode("\n\t\t\t") );
			oNewConnectInfo.appendChild ( oNewHostNameElem );
			oNewConnectInfo.appendChild( mo_document.createTextNode("\n\t\t\t") );
			oNewConnectInfo.appendChild ( oNewDbNameElem );
			oNewConnectInfo.appendChild( mo_document.createTextNode("\n\t\t\t") );
			oNewConnectInfo.appendChild ( oNewUseridElem );
			oNewConnectInfo.appendChild( mo_document.createTextNode("\n\t\t\t") );
			oNewConnectInfo.appendChild ( oNewPasswordElem );		
			oNewConnectInfo.appendChild( mo_document.createTextNode("\n\t\t") );
			// ----------------------------------
			// <data_source>에 <connect_info>를 추가
			// ----------------------------------
			oNewDataSourceElem.appendChild( mo_document.createTextNode("\n\t\t") );
			oNewDataSourceElem.appendChild ( oNewConnectInfo );
			oNewDataSourceElem.appendChild( mo_document.createTextNode("\n\t") );

			// ----------------------------------
			// DF에 <data_source>를 추가
			// ----------------------------------
			// 데이터소스중 서버명이 다른 첫번째 노드를 알아낸다. (중요... )
			//Node oNodeAfterLast =  ( Node )mo_lastServerMap.get( sServerName ) ;
			
			// 소스노드( <source></source )에 붙인다.
			// oNodeAfterLast =null 이면 source노드의 가장 마지막에 붙는다.
			mo_sourceNode.insertBefore( mo_document.createTextNode("\n\t"), null );
			mo_sourceNode.insertBefore ( oNewDataSourceElem, null );
			mo_sourceNode.insertBefore( mo_document.createTextNode("\n\n\t"), null );
			mb_dfModified = true;

			
			logger.info ( "------------------------------------------------------" );
			logger.info ( "[ServerName]=" + sServerName );
			logger.info ( "[DESC]=" + sDesc );
			logger.info ( "[DBPATH]=" + sDBPath );
			logger.info ( "[DELETE]=" + sDelete );			
			logger.info ( ">>>Append new Datasource");
			logger.info ( "------------------------------------------------------" );
		} else {
			
			// 데이터소스를 DF에서 찾았다면 
			// delete처리를 해야하는 경우만 state를 "D"로 update
			NamedNodeMap oNodeMap = null;
			String sState = null;
			Node oAttState = null;
			
			if ( sDelete.equals("Y") ) {
				
				// [DataSource] 노드의 속성을 구한다
				oNodeMap = oDataSourceNode.getAttributes();
		
				// DF의 state값을 알아낸다.
				oAttState = oNodeMap.getNamedItem( "state" );
				sState = oAttState.getNodeValue();	

	
				if ( sState.equalsIgnoreCase("N") == false ) {
					// DF의 state가 D(삭제)혹은 C(삭제처리완료)라면 이미 삭제처리된 경우이므로
					// state를 Update하지 않는다.
					
					return;
				}
				
				//oAttState.setNodeValue ( "A" );
				oAttState.setNodeValue ( "D" );
				mb_dfModified = true;
				
				logger.info ( "------------------------------------------------------" );
				logger.info ( "[ServerName]=" + sServerName );
				logger.info ( "[DESC]=" + sDesc );
				logger.info ( "[DBPATH]=" + sDBPath );
				logger.info ( "[DELETE]=" + sDelete );			
				logger.info ( ">>>Change status delete flag of datasource");
				logger.info ( "------------------------------------------------------" );				
			}
		}
	}

}

	
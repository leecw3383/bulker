/**
 *******************************************************************
 * 파일명 : SSWriter.java
 * 파일설명 : 색인테이블에 insert 혹은 update하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/30   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index.fastsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom.CDATA;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.ResultWriter;
import com.rayful.bulk.index.TargetColumn;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.util.Utils;
import com.rayful.io.FileCharacterSetDetector;
import com.rayful.localize.Localization;

/**
 * 색인테이블에 색인테이터를 insert 혹은 update하는 클래스
 */
public class XmlWriter extends ResultWriter
{
	static Logger logger = null;
	private static final String RESULT_FILE_EXT = ".xml";
	
	Document mo_xmlDoc = null;
	Element mo_rootEle = null;
	String ms_txtEncode = null;
	boolean mb_modifed = false;

	/**
	 * Column Matching 정보
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	ColumnMatching mo_columnMatching = null;

	
	/**
	* 생성자
	* <p>
	* @param	oCon	SearchServer Connection 객체
	* @param	sSSTableName	SearchServer Table 명
	* @param	sLogPath	로그파일의 경로 및 이름
	*/			
	public XmlWriter ( String sLogPath ) 
	{
		logger = RayfulLogger.getLogger(XmlWriter.class.getName(), sLogPath );
		ms_txtEncode = "UTF-8";
		mb_modifed = false;
		
		mo_xmlDoc = new Document();
		mo_rootEle = new Element("documents");	// root element
		mo_xmlDoc.addContent(mo_rootEle);		
	}
	
	/**
	* 생성자
	* <p>
	* @param	oCon	SearchServer Connection 객체
	* @param	sSSTableName	SearchServer Table 명
	*/			
	public XmlWriter () 
	{
		logger = RayfulLogger.getLogger(XmlWriter.class.getName(), Config.LOG_PATH );
		ms_txtEncode = "UTF-8";
		mb_modifed = false;
		
		mo_xmlDoc = new Document();
		mo_rootEle = new Element("documents");	// root element
		mo_xmlDoc.addContent(mo_rootEle);			
	}	
	
	
	/**
	* 색인테이블에 존재하는지 확인 ( for Oracel )
	* @param	oColumnMatching	컬럼매칭정보 객체
	* <p>	
	*/		
	public boolean setColumnMatching ( ColumnMatching oColumnMatching )
	{
		//TargetColumn oTargetColumn = null;
				
		if ( oColumnMatching == null ){
			logger.error ( Localization.getMessage( XmlWriter.class.getName() + ".Logger.0001" ) );
			return false;
		}
		mo_columnMatching = oColumnMatching;
		return true;
	}
	
	/**
	* 로그파일의 경로를 변경한다.
	* <p>
	* @param	sLogfilePath	새로그파일경로
	*/	
	public void setLogPath ( Logger oNewLogger  ) {
		logger = oNewLogger;
	}

	/**
	* xml에 추가한다.
	* <p>
	* @param oResultMap 색인데이터를 가진 Map
	* @return	true: success/ false: failed
	*/	
	public boolean addDocument( Map<String, Object> oResultMap )
	{
		boolean bReturn = true;
		
		Object [] aKey = null;
		int iTargetColumnType;
		String sTargetColumnContentType = null;
		String sTargetColumnName = null;
		String sTargetColumnValue = null;
		Element eleTargetColumnValue = null;
		TargetColumn oTargetColumn = null;
		
		// xml elemet 
		Element doc = null;		
		Element ele = null;
		Element val = null;
		CDATA cdata = null;		
		
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		File oBodyFile = null;
		StringBuffer osbBody = null;
		String sReadLine = null;
		FileCharacterSetDetector oEncodeDetector = null;
		String sFileCharSet = null;
		
		Utils util = new Utils();
		
		try {
			doc = new Element( "document" );
		  	aKey =  mo_columnMatching.target.keySet().toArray();  	
		  	for ( int i=0; i<aKey.length; i++ ) {
		  		oTargetColumn = (TargetColumn)mo_columnMatching.target.get( aKey[i].toString() );
		  		iTargetColumnType = oTargetColumn.getColumnType();
		  		sTargetColumnContentType = oTargetColumn.getContentType();
		  		sTargetColumnName = ((String) oTargetColumn.getColumnName()).toLowerCase();
		  		//sTargetColumnName = ((String) oTargetColumn.getColumnName()).toUpperCase();
		  		
		  		if ( "xml".equalsIgnoreCase(sTargetColumnName) ) {
		  			eleTargetColumnValue = (Element)oResultMap.get( sTargetColumnName );
		  		} else {
		  			sTargetColumnValue = (String)oResultMap.get( sTargetColumnName );
		  		}
		  		
		  		// target_column 이름이 tmp 로 시작하면 XML 데이터로 저장하지 않는다.
		  		if ( sTargetColumnName.indexOf("tmp") == 0 ) {
		  			if ( iTargetColumnType == ESPTypes.LONGTEXT ) {	
		  				if ( sTargetColumnValue != null && sTargetColumnValue.length() > 0 ) {
			  				oBodyFile = new File(sTargetColumnValue );
			  				if ( ! oBodyFile.delete()) {
								logger.warn( Localization.getMessage( XmlWriter.class.getName() + ".Logger.0003", oBodyFile.getPath() ) );
							}
			  			}
		  			}
		  			continue;
		  		}
		  		
		  		if ( sTargetColumnValue != null ) {
			  		// xml
//		  			below related opensky comment
					ele = new Element( "element" );
					
					ele.setAttribute("name", sTargetColumnName);
					doc.addContent(ele);
					
					val = new Element ( "value" );
					ele.addContent(val);
//		  			opensky comment end
		  			
		  			
		  			// 서울대학교 분당 병원 POC
		  			// Scope Search 를 위한 XML 생성
//		  			if ( "xml".equalsIgnoreCase(sTargetColumnName) == false ) {
//			  			ele = new Element(sTargetColumnName);		  			
//			  			doc.addContent(ele);
//		  			}
		  			// Scope Search 를 위한 XML 생성 end
		  			
					if ( iTargetColumnType == ESPTypes.LONGTEXT ) {	
						
						// 색인필드 타입이 ESPTypes.LONGTEXT 이면 sTargetColumnValue가 본문파일의 경로가 된다.
						oBodyFile = new File(sTargetColumnValue );
						if ( oBodyFile.exists() ) {
							osbBody = new StringBuffer();
							
							oEncodeDetector = new FileCharacterSetDetector( oBodyFile );
							sFileCharSet = oEncodeDetector.getCharaterSetName();
							//logger.info( "\t" + sFileCharSet + ":" + oBodyFile.getPath() );
							
							fis = new FileInputStream( oBodyFile );
							isr = new InputStreamReader (fis, sFileCharSet);	// utf-8으로 읽는다.

							//isr = new InputStreamReader (fis );	// utf-8으로 읽는다.
							br  = new BufferedReader( isr );

							
							while ( (sReadLine = br.readLine()) != null  ) {
								osbBody.append( sReadLine ).append("\r").append("\n");
							}
							br.close();
							sTargetColumnValue = osbBody.toString();
							
						} else {
							throw new Exception( Localization.getMessage( XmlWriter.class.getName() + ".Exception.0001", sTargetColumnValue ) );
						}
						
						if ( ! oBodyFile.delete()) {
							logger.warn( Localization.getMessage( XmlWriter.class.getName() + ".Logger.0003", oBodyFile.getPath() ) );
						}

						
					} else if ( iTargetColumnType == ESPTypes.DATE ) {	
						// fast date foramt으로 변경
						sTargetColumnValue = ESPTypes.dateFormatString(
								Timestamp.valueOf(sTargetColumnValue), "yyyy-MM-dd HH:mm:ss").replaceAll(" ", "T");
					} /*else {
						logger.info("sTargetColumnName : " + sTargetColumnName + " / sTargetColumnValue : " + sTargetColumnValue);
					} */
					
					cdata = new CDATA("");
					// -------------------------------------------------------------------
					// IlleagalDataException이 발생하는 것을 막기위해
					// 에러를 유발하는 문자를 제거한다. ( x00-x1f ) : x0a, x0d은 제외 (개행문자)
					// 2008/08/11	정충열
					// -------------------------------------------------------------------
					
					if ( sTargetColumnValue.indexOf("[CDATA[") > 0 ) {
						sTargetColumnValue = "";
					}
					
					//sTargetColumnValue = sTargetColumnValue.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
					//sTargetColumnValue = sTargetColumnValue.replaceAll("<", " ").replaceAll(">", " ");
					//sTargetColumnValue = sTargetColumnValue.replaceAll("&lt;", " ").replaceAll("&gt;", " ");
					//if ( "body".equalsIgnoreCase(sTargetColumnName) ) {
					if ( sTargetColumnContentType != null && 
							( "html".equalsIgnoreCase(sTargetColumnContentType) || 
							  "namo".equalsIgnoreCase(sTargetColumnContentType) ) ) 
					{
						if ( sTargetColumnValue != null ) { 
							sTargetColumnValue = sTargetColumnValue.replaceAll("&lt;","<").replaceAll("&gt;",">");
						}
						
						int headPos[] = getHeadPositionFromHtml(sTargetColumnValue);
						
						if ( headPos[0] > 0 && headPos[1] > 0 )
							sTargetColumnValue = sTargetColumnValue.substring(0,headPos[0]) + sTargetColumnValue.substring(headPos[1] + 7);
						
						sTargetColumnValue = getTextFromHtml(sTargetColumnValue);
					}
						
					
//					logger.info("------------------------------------------------------------------------------------");
//					logger.info(sTargetColumnValue);
//					logger.info("------------------------------------------------------------------------------------");
					cdata.setText( sTargetColumnValue.replaceAll("[\\x00-\\x09|\\x0b-\\x0c|\\x0e-\\x1f]",""));	// opensky comment
					val.addContent(cdata);	// opensky comment
					
					// 서울대학교 분당 병원 POC
		  			// Scope Search 를 위한 XML 생성
//					if ( "xml".equalsIgnoreCase(sTargetColumnName) == false ) {
//						cdata.setText(sTargetColumnValue.replaceAll("[\\x00-\\x09|\\x0b-\\x0c|\\x0e-\\x1f]",""));
//						ele.addContent(cdata);
//					} else {
//						//doc.addContent(sTargetColumnValue.replaceAll("[\\x00-\\x09|\\x0b-\\x0c|\\x0e-\\x1f]",""));
//						doc.addContent(eleTargetColumnValue);
//					}
					//Scope Search 를 위한 XML 생성
		  		} /*else {
		  			if ( iTargetColumnType == ESPTypes.LONGTEXT ) {	
		  				logger.info("sTargetColumnName : " + sTargetColumnName);
		  			}
		  		} */ 
		  	} // for 
		  	
		  	mo_rootEle.addContent( doc );
		  	mb_modifed = true;
		  	
        } catch (Exception e) {
            logger.error( Localization.getMessage( XmlWriter.class.getName() + ".Logger.0004" ), e );
            bReturn = false;
        } finally {
        	if ( br != null ) try { br.close(); } catch( Exception e) {}
        	if ( isr != null ) try { isr.close(); } catch( Exception e) {}
        	if ( fis != null ) try { fis.close(); } catch( Exception e) {}
        }
        return bReturn;
	}

	
	public boolean saveFile( String sDestFileName ) {
		boolean bReturn = true;		
		OutputStream os = null;
		XMLOutputter outp = null;
		
		// 변경되지 않았다면 저장하지 않는다.
		if ( ! mb_modifed )   {
			return true;
		}

		try {
			os = new FileOutputStream( sDestFileName );
			outp = new XMLOutputter();
			outp.setFormat(Format.getPrettyFormat());
			//outp.output(ms_xmlDoc, System.out);
			outp.output(mo_xmlDoc, os);
			
			mb_modifed = false;

		} catch ( Exception  e ) {
			logger.error( Localization.getMessage( XmlWriter.class.getName() + ".Logger.0002" ), e);
			bReturn = false;
		} finally {
			if ( os != null ) try { os.close(); } catch( Exception e) {}
		}
		
		return bReturn;
	}
	
	public String getFileExt() { 
		return RESULT_FILE_EXT; 
	}
}
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
package com.rayful.bulk.index.elastic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.ColumnMatching;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.ResultWriter;
import com.rayful.bulk.index.TargetColumn;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.io.FileCharacterSetDetector;
import com.rayful.localize.Localization;

/**
 * 색인테이블에 색인테이터를 insert 혹은 update하는 클래스
 */
public class JsonESWriter extends ResultWriter
{
	static Logger logger = null;
	private static final String RESULT_FILE_EXT = ".json"; 
	
	String ms_txtEncode = null;
	boolean mb_modifed = false;
	
	List<BulkData> mo_dataList = new ArrayList<BulkData>();
	/**
	 * Column Matching 정보
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	ColumnMatching mo_columnMatching = null;
	
	/** 색인수행결과 상수 : 등록됨  */
	public static final int SUCCESSED = 1;
	/** 색인수행결과 상수 : 변경작업실패  */
	public static final int FAILED = 4;

	
	/**
	* 생성자
	* <p>
	* @param	oCon	SearchServer Connection 객체
	* @param	sSSTableName	SearchServer Table 명
	* @param	sLogPath	로그파일의 경로 및 이름
	*/			
	public JsonESWriter ( String sLogPath ) 
	{
		logger = RayfulLogger.getLogger(JsonESWriter.class.getName(), sLogPath );
		ms_txtEncode = "UTF-8";
		mb_modifed = false;

	}
	
	/**
	* 생성자
	* <p>
	* @param	oCon	SearchServer Connection 객체
	* @param	sSSTableName	SearchServer Table 명
	*/			
	public JsonESWriter () 
	{
		logger = RayfulLogger.getLogger(JsonESWriter.class.getName(), Config.LOG_PATH );
		ms_txtEncode = "UTF-8";
		mb_modifed = false;

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
			logger.error ( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0001" ) );
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
		TargetColumn oTargetColumn = null;
		
		// Json 관련 ...
		BulkData bulkData = new BulkData(BulkData.OP_INDEX);
		
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		File oBodyFile = null;
		StringBuffer osbBody = null;
		String sReadLine = null;
		FileCharacterSetDetector oEncodeDetector = null;
		String sFileCharSet = null;
		
		try {
		  	aKey =  mo_columnMatching.target.keySet().toArray();  	
		  	for ( int i=0; i<aKey.length; i++ ) {
		  		oTargetColumn = (TargetColumn)mo_columnMatching.target.get( aKey[i].toString() );
		  		iTargetColumnType = oTargetColumn.getColumnType();
		  		sTargetColumnContentType = oTargetColumn.getContentType();
		  		
		  		sTargetColumnName = ((String) oTargetColumn.getColumnName()).toLowerCase();
		  		//sTargetColumnName = ((String) oTargetColumn.getColumnName()).toUpperCase();
		  		
		  		sTargetColumnValue = (String)oResultMap.get( sTargetColumnName );
		  		
		  		// target_column 이름이 tmp 로 시작하면 XML 데이터로 저장하지 않는다.
		  		if ( sTargetColumnName.indexOf("tmp") == 0 ) {
		  			if ( iTargetColumnType == ESPTypes.LONGTEXT ) {	
		  				if ( sTargetColumnValue != null && sTargetColumnValue.length() > 0 ) {
			  				oBodyFile = new File(sTargetColumnValue );
			  				if ( ! oBodyFile.delete()) {
								logger.warn( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0003", oBodyFile.getPath() ) );
							}
			  			}
		  			}
		  			continue;
		  		}
		  		
		  		if ( sTargetColumnValue != null ) {
		  			/* 타겟(검색엔진) 컬럼의 타입이 LONGTEXT(본문) 인경우 */
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
							throw new Exception( Localization.getMessage( JsonESWriter.class.getName() + ".Exception.0001", sTargetColumnValue ) );
						}
						
						if ( ! oBodyFile.delete()) {
							logger.warn( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0003", oBodyFile.getPath() ) );
						}

					/* 타겟(검색엔진)의 타입이  날짜 인 경우 */
					} else if ( iTargetColumnType == ESPTypes.DATE ) {	
						// fast date foramt으로 변경
						sTargetColumnValue = ESPTypes.dateFormatString(
							Timestamp.valueOf(sTargetColumnValue), "yyyy-MM-dd HH:mm:ss").replaceAll(" ", "T");
					} /*else {
						logger.info("sTargetColumnName : " + sTargetColumnName + " / sTargetColumnValue : " + sTargetColumnValue);
					} */
					
					
					/* 타겟(검색엔진) 컬럼의  <<<컨텐트 타입>>>이  html/namo 인경우  */
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
					if ( sTargetColumnName.equalsIgnoreCase(BulkData.INDEX_ID)) {
			  			bulkData.setIndexId(sTargetColumnValue);  
			  			// index 명을 입력 할 때 사용 가능 
			  			//bulkData.setIndexName("_indexname");
					} else {
						
						if(sTargetColumnName.equalsIgnoreCase("attachments")) {
							List<Object> attachedList = new ArrayList<Object>();
							
							if(sTargetColumnValue != null && sTargetColumnValue.length() > 0) {
								Map<String, Object> attachedMap = null;
								String[] attachedArray = sTargetColumnValue.split("\\|");
								
								if(attachedArray != null && attachedArray.length > 0) {
									for(int index = 0; index < attachedArray.length; index++) {
										attachedMap = new HashMap<String, Object>();
										String [] attachedData = attachedArray[index].split("\\^");
										
										if(attachedData != null && attachedData.length > 1) {
											attachedMap.put("filename", attachedData[0]);
											attachedMap.put("data", attachedData[1]);
										}
										
										attachedList.add(attachedMap);
									}
								}
							} 
							
							bulkData.addUpdateDataObject(sTargetColumnName, attachedList);
						} else {
							if(sTargetColumnValue != null && sTargetColumnValue.length() > 0) {
								bulkData.addUpdateData(sTargetColumnName, sTargetColumnValue);
							}
						}
					}
					
					//Scope Search 를 위한 XML 생성
		  		} /*else {
		  			if ( iTargetColumnType == ESPTypes.LONGTEXT ) {	
		  				logger.info("sTargetColumnName : " + sTargetColumnName);
		  			}
		  		} */ 
		  	} // for 
		  	
		  	//bulkData.toJson(System.out);
		  	
		  	this.mo_dataList.add(bulkData);
		  	//bReturn = true;
		  	
		  	mb_modifed = true;
        } catch (Exception e) {
            logger.error( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0004" ), e );
            bReturn = false;
        } finally {
        	if ( br != null ) try { br.close(); } catch( Exception e) {}
        	if ( isr != null ) try { isr.close(); } catch( Exception e) {}
        	if ( fis != null ) try { fis.close(); } catch( Exception e) {}
        }
        return bReturn;
	}

	
	public boolean saveFile( String sDestFileName, int ms_resulFileSize ) {
		boolean bReturn = true;		
		OutputStream os = null;
		File chkFile = null;
		
		// 변경되지 않았다면 저장하지 않는다.
		if ( ! mb_modifed )   {
			return true;
		}

		try {
			int listSize = this.mo_dataList.size();
			
			for(int index =0; index <listSize; index++) {
				
				if(index == 0) {
					os = new FileOutputStream( sDestFileName );
					chkFile = new File(sDestFileName);
					
					this.mo_dataList.get(index).toJson(os);
				} else {
					if(chkFile.length() > (1024*(1024*ms_resulFileSize))) {
						
						String sOriginName = sDestFileName.substring(0, sDestFileName.lastIndexOf("."));
						String sExt = sDestFileName.substring(sDestFileName.lastIndexOf("."), sDestFileName.length());
						os = new FileOutputStream(sOriginName + "_" + index + sExt);
						chkFile = new File(sOriginName + "_" + index + sExt);
						
						this.mo_dataList.get(index).toJson(os);
					} else {
						this.mo_dataList.get(index).toJson(os);
					}
				}
			}

			mb_modifed = false;

		} catch ( Exception  e ) {
			logger.error( Localization.getMessage( JsonESWriter.class.getName() + ".Logger.0002" ), e);
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
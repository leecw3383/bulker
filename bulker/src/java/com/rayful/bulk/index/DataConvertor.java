/**
 *******************************************************************
 * 파일명 : DataConvertor.java
 * 파일설명 : 데이터소스로 부터 조회된 결과를 색인할 수 있도록 변환하는
 * 						클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/31   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.rayful.bulk.io.FileAppender;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.RdbmsDataReader;
import com.rayful.io.FileCharacterSetDetector;
import com.rayful.io.namo.NamoDocument;
import com.rayful.io.namo.NamoParser;
import com.rayful.localize.Localization;

/**
 * 데이터소스로 부터 조회된 결과를 색인할 수 있도록 변환한다.
 */
public class DataConvertor {
	String ms_dataSourceName;					// 데이터소스명
	/**
	 * @uml.property  name="mo_columnMatching"
	 * @uml.associationEnd  
	 */
	ColumnMatching mo_columnMatching;	// 컬럼매칭정보 객체
	
	// logger 지정
	static Logger logger = RayfulLogger.getLogger(DataConvertor.class.getName(), Config.LOG_PATH );
		
	
	/**
	 * 생성자 <p>
	 * Parameter 없는 생성자 반드시 필요
	 * <p>
	*/
	public DataConvertor ( String sDataSourceName, ColumnMatching oColumnMatching ) 
	{
		ms_dataSourceName = sDataSourceName;
		mo_columnMatching = oColumnMatching;
	}
	
	
	/**
	 * 조회된 결과를 색인컬럼데이터로 변환한다.
	 * 변환과정 중 SS의 컬럼 Type이 APVARCHAR인 컬럼과 매칭된 조회데이터는 내용파일로 저장한다.
	 * <p>
	 * @param	oReaderMap	조회한 결과데이터
	 * @param	oFileAppender	내용파일의 FileAppender객체
	 * @return 색인할 수 있도록 컨버전된 데이터 
	*/	
	public Map<String, Object> convertData ( Map<String, Object> oReaderMap )
	throws Exception
	{
		Map<String, Object> oResultMap = new LinkedHashMap<String, Object>();
		String sSSColumnName = null;
		String sRegx = null;
		String sValue = null;
		
		// sSSColumnName을 어떻게 매칭시킬건가?
		Object [] aKey = mo_columnMatching.target.keySet().toArray();
		String sSourceColumnNames = null;
		String sSourceColumnName = null;
		String sColumnText = null;
		String sContentType = null;
		File oBodyFile = null;
		
		for ( int i=0; i< aKey.length ; i++ ) {
			TargetColumn oTargetColumn = (TargetColumn )mo_columnMatching.target.get( aKey[i] );
			sSourceColumnNames = oTargetColumn.getSourceColumnNames();
			sColumnText = oTargetColumn.getColumnText();
			sContentType = oTargetColumn.getContentType();
			sSSColumnName = (String ) aKey[i];
			int iTargetColumnType = oTargetColumn.getColumnType();
			
			//logger.info("sSSColumnName : " + sSSColumnName);
			
			if ( (sSourceColumnNames != null)  && (sSourceColumnNames.length() > 0) ) {
				
				// 관련 소스컬럼명을 알아낸다
				StringTokenizer oSt = new StringTokenizer ( sSourceColumnNames, "||" );
				while ( oSt.hasMoreElements() ) {
					sSourceColumnName = oSt.nextToken();
					
					//SourceColumn oSourceColumn = (SourceColumn )mo_columnMatching.source.get( sSourceColumnName );
					sValue = this.getData( sSourceColumnName, sSSColumnName, oReaderMap );
					
					if ( iTargetColumnType == com.rayful.bulk.ESPTypes.LONGTEXT ) {
						// 본문내용일경우 파일로 저장		
						if ( sValue != null ) {
							oBodyFile = new File ( sValue );
							if ( ! oBodyFile.exists() ) {
								throw new Exception( Localization.getMessage( DataConvertor.class.getName() + ".Exception.0001" ) );
							} else {
								// NAMO Content를 처리하기 위해서 추가 - 20100622
								// 이 부분에서 파일의 내용을 읽어서 Namo Type인지 확인할 것.
								if ( sContentType != null && sContentType.equalsIgnoreCase(ColumnMatching.CONTENT_TYPE_NAMO) ) {
									// namo 가 2개 필드로 정의되어 있는 경우 아래 로직에서 에러 발생.
									try {
										NamoDocument namoDoc = NamoParser.parse(sValue);
										oBodyFile = namoDoc.saveBodyFile( namoDoc.getPath() );
										sValue = oBodyFile.toString();
									} catch(Exception e) {
										
									}
								}
							}
						}
						
						// 본문 내용이 여러 개의 컬럼 정보에서 읽어온 경우 처리
						//System.out.println("########### sColumnText : " + sColumnText + " / sSSColumnName : " + sSSColumnName + " / sSourceColumnNames : " + sSourceColumnNames);
						if ( sColumnText != null && (new File ( sColumnText )).exists() ) {
							sColumnText = mergeBodyFile(sColumnText,sValue);
						} else {
							sColumnText = sValue;
						}
						
					}	else {
					// 본문내용이 아닐경우
						try {
							if ( sValue != null ) {
								//치환문자열에 \\가 포함된 경우 \\\\로 변경한다.
								//[중요]이 문자를 $보다 먼저 처리할 것 
								//그 이유는 $처리시 value에 \\문자가 포함되기 때문이다.
								if ( sValue.indexOf ( "\\" ) >= 0 ) {
									sValue = sValue.replaceAll( "\\\\", "\\\\\\\\" );
								}
								
								//치환문자열에 $가 포함된 경우 \$로 변경한다.
								if ( sValue.indexOf ( "$" ) >= 0 ) {
									sValue = sValue.replaceAll( "[$]", "\\\\\\$" );
								}
								
							} else {
								sValue = "";
							}
							
							if ( sSourceColumnName != null ) {
								if ( sSourceColumnName.indexOf ( "$" ) >= 0 ) {
									sSourceColumnName = sSourceColumnName.replaceAll( "[$]", "\\\\\\$" );
								}
							}
							
							//컬럼Text값에 DB조회값 부분을 치환한다.
							// opensky
							//sRegx = "\\[%" +  sSourceColumnName + "%\\]";
							sRegx = "\\[%" +  sSourceColumnName + "%\\]";
							sColumnText = sColumnText.replaceAll( sRegx, sValue );
						} catch ( IndexOutOfBoundsException iobe ) {
							logger.error( Localization.getMessage( DataConvertor.class.getName() + ".Logger.0001" ), iobe);
							logger.info ( Localization.getMessage( DataConvertor.class.getName() + ".Logger.0002", sRegx ) );
							logger.info ( Localization.getMessage( DataConvertor.class.getName() + ".Logger.0003", sValue ) );
							throw iobe;
						}
							
					} //else
				} //while
			} // if

			if ( sColumnText != null ) {
				if ( sColumnText.trim().length() == 0 ) {
					sColumnText = null;
				}
			}
			
			if ( sColumnText == null ) {
				if ( oTargetColumn.isNotNull() ) {
					String sErrMsg = Localization.getMessage( DataConvertor.class.getName() + ".Logger.0003", sSSColumnName );
					logger.error ( sErrMsg );
					throw new Exception ( sErrMsg );
				}
			}
			
			oResultMap.put ( sSSColumnName, sColumnText ) ;
		} //for

		return oResultMap;
	}
	
	
	/**
	 * 색인할 수 있도록 데이터를 컨버전한다.
	 * <p>
	 * @param	sSourceColumnName	색인대상 Column명
	 * @param	sSSColumnName	SearchServer Column명
	 * @param	oReaderMap	조회한 데이터
	 * @return 색인할 수 있도록 컨버전된 데이터 
	*/	
	protected String getData (  String sSourceColumnName, 
															String sSSColumnName, 
															Map<String, Object> oReaderMap )
	{
		String  oData = "";
		
		if ( sSourceColumnName != null ) {
			if ( sSourceColumnName.length() > 0 ) {
				oData = (String) oReaderMap.get( sSourceColumnName );
			}
		}
		return oData;
	}
	
	/**
	 * 두 개의 임시 파일을 하나의 임시 파일로 만들기 
	 * @param sFirstFile
	 * @param sSecondFile
	 * @return
	 */
	protected String mergeBodyFile(String sFirstFile, String sSecondFile)
	{
		String sMergeFile = "";
		//String sReadValue = null;
		
		try {
		
			File tmpFile = File.createTempFile("rdbms_1" , ".tmp", new File(Config.DOWNLOAD_PATH) );
			FileAppender oFileAppender = new FileAppender( tmpFile.getPath() );
			File oBodyFile = new File(sFirstFile);
			
			if ( oBodyFile.exists() ) {
//				String sReadLine = "";
//				StringBuffer osbBody = new StringBuffer();
				
				FileCharacterSetDetector oEncodeDetector = new FileCharacterSetDetector( oBodyFile );
				String sFileCharSet = oEncodeDetector.getCharaterSetName();
				logger.info( "\t" + sFileCharSet + ":" + oBodyFile.getPath() );
				
				FileInputStream fis = new FileInputStream( oBodyFile );
				InputStreamReader isr = new InputStreamReader (fis, sFileCharSet);	// utf-8으로 읽는다.

				//isr = new InputStreamReader (fis );	// utf-8으로 읽는다.
				BufferedReader br  = new BufferedReader( isr );
				
//				while ( (sReadLine = br.readLine()) != null  ) {
//					osbBody.append( sReadLine ).append("\r").append("\n");
//				}
				
				oFileAppender.append ( br );
				
				br.close();
				isr.close();
				fis.close();
				
				oBodyFile.delete();
			} 
			
			if ( sSecondFile !=  null ) {
				oBodyFile = new File(sSecondFile);
				if ( oBodyFile.exists() ) {
	//				String sReadLine = "";
	//				StringBuffer osbBody = new StringBuffer();
					
					FileCharacterSetDetector oEncodeDetector = new FileCharacterSetDetector( oBodyFile );
					String sFileCharSet = oEncodeDetector.getCharaterSetName();
					logger.info( "\t" + sFileCharSet + ":" + oBodyFile.getPath() );
					
					FileInputStream fis = new FileInputStream( oBodyFile );
					InputStreamReader isr = new InputStreamReader (fis, sFileCharSet);	// utf-8으로 읽는다.
	
					//isr = new InputStreamReader (fis );	// utf-8으로 읽는다.
					BufferedReader br  = new BufferedReader( isr );
					
	//				while ( (sReadLine = br.readLine()) != null  ) {
	//					osbBody.append( sReadLine ).append("\r").append("\n");
	//				}
					
					oFileAppender.append(new StringReader(" \r\n "));
					oFileAppender.append ( br );
					
					br.close();
					isr.close();
					fis.close();
					
					oBodyFile.delete();
				}
			}
			
			sMergeFile = tmpFile.getPath();
			
		} catch ( FileNotFoundException fnfe ) {
			logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0011" ) );
		} catch ( IOException ie ) {
			logger.error ( Localization.getMessage( RdbmsDataReader.class.getName() + ".Logger.0012" ) );
		}
		
		return sMergeFile;
	}
}
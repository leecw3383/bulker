package com.rayful.bulk.index.indexer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.rayful.bulk.ESPTypes;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.KeyData;
import com.rayful.bulk.index.fastsearch.XmlWriter;
import com.rayful.bulk.io.Attachments;

public class RdbmsIndexerWorkOrder extends RdbmsIndexer {

	/** 
	* 업무지시용 커스텀 로직 
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

			// DataSource가 oracle이고,  8.1.6이전일때 Unicode변환을 사용하도록 세팅
			// 그외의 경우는 Unicode변환을 사용하지 않음
			initUnicodeConvertor();
		
			// DataConvertor객체 로드 및 설정
			initDataConvertor();
			
			// ConvertCustomizer 객체 로드 및 설정
			initConvertCustomizer();

			// XmlWriter 설정.
			oXmlWriter = new XmlWriter();
			oXmlWriter.setColumnMatching ( mo_columnMatching ); 
			
			// 색인대상 Key Query
			oKeyData = queryIndexKey();
			// Key데이터의 로우수를 알아낸다.
			mi_totalCnt = oKeyData.size();
			
			// 색인데이터 결과를 담는 객체
			//Map<String, Object> oDataReaderMap = new HashMap<String, Object>();
			// 색인데이터 결과를 담는 리스트 객체
			List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
		
			// 컨버팅 결과를 담는 객체
			Map<String, Object> oResultMap = null;
			
			logger.info ( "색인대상 전체건수 : " + mi_totalCnt );
			for (int i=0; i< mi_totalCnt; i++) { 
				
				//----------------------------------------------------------------------------------------
				// ### 에러로직 ###
				// [치명]적인 에러가 발생하면 색인프로그램을 종료
				// [일반]적인 에러가 발생하면 해당 로우를 Skip...
				// 따라서 여기서 호출되는 함수는 내부에서 Exception처리를 하고 성공여부를 리턴
				// 또는 치명적인 경우는 Excetion을 thorw 통해 전달
				//----------------------------------------------------------------------------------------
				mi_tryCnt ++;			
				bWarned = false;
				
				// 결과 xml 파일의 이름을 만든다.
				// success 카운트를 기준으로 한다.
				if ( mi_successCnt % mi_resultMaxRows  == 0 ) {
					if ( mi_successCnt > 0 ) {
						if ( oXmlWriter.saveFile(oResultFile.getPath()) == false ) {
							throw new Exception ( "Result Xml을 저장할 수 없습니다.");
						}
						
						// --------------------------------------------------------
						// 커넥션을 재설정한다.
						// DB 커넥션이 너무 오래 동안 사용하면  간혼 끊기는 경우가 발생하므로
						// xml을 write하는 시점에서 커넥션을 다시 맺는다.
						// --------------------------------------------------------
						// 강제로 DB 커넥션을 종료한다. : true
						this.closeSourceConnection(true);
						// DB에 다시 접속한다.
						this.connectSourceConnection();
						// DB작업을 하는 DataReader의 커넥션을 재설정한다.
						mo_dataReader.setConnection(mo_sourceConnection, mo_sourceConnector, ms_dataSourceId );
						// DB작업을 하는 EtcReader의 커넥션을 재설정한다.
						mo_etcReader.setConnection(mo_sourceConnection);
						// --------------------------------------------------------
						
						// XmlWriter 설정.
						oXmlWriter = new XmlWriter();
						oXmlWriter.setColumnMatching ( mo_columnMatching ); 
					}
					
					sCurrentTime = ESPTypes.dateFormatString( new java.util.Date(), "yyyyMMddHHmmss");
					oResultFile = new File( Config.RESULT_PATH, ms_resultFileName + "_" + sCurrentTime + ".xml");					
				}
				
				// Key데이터 한 Row를 맵에 담는다.
				mo_curKeyMap = oKeyData.get(i);

				// 색인외부파일 정보를 담을 객체를 생성한다.
				Attachments oAttachments = new Attachments();
				// 색인외부파일객체
				//FileAppender oExternalFileAppender = null;

				
				// 쿼리에 필요한 Parameter들을 설정한다.
				initDataQueryParameter();

				// Key값으로 색인데이터를 조회한다. 
				// oDataReaderMap을 얻는다.
				//oDataReaderMap = queryIndexData();
				listMap = queryIndexData();
				
				for(Map<String, Object> oDataReaderMap : listMap) {
					if ( oDataReaderMap == null ) {
						logger.error( "queryIndexData()");
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}
					
					if ( logger.isDebugEnabled() ) {
						logger.debug( "<조회 Map>" );
						logger.debug( oDataReaderMap );
					}
					
					// 조회된 색인데이터를 변환 ( oDataReaderMap을 => oResultMap )
					// 본문내용이 있으면 oExternalFileAppender을 이용 본문파일에 추가
					oResultMap = convertIndexData( oDataReaderMap  );
					if ( oResultMap == null ) {
						logger.error( "convertIndexData()");
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}
					
					/*
					if ( getBodyFilePath(oExternalFileAppender, oResultMap) == false ) {
						logger.error( "getBodyFilePath()");
						writeIndexLog ( XmlWriter.FAILED );
						continue;					
					}
					*/
					
					// customizer가 동작
					if ( customize (  oDataReaderMap, oResultMap ) == false ) {
						logger.error( "customize()");
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}				
	
					// Key값으로 첨부파일정보를 조회한다.
					// oAttachments에 추가
					if ( queryFileInfo( oAttachments ) == false ) {
						logger.error( "queryFileInfo()");
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}
				
					
					// 첨부파일 경로정보를 포함한다.
					if ( addAttachFileInfo( oXmlWriter, oResultMap, oAttachments ) == false ) {
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}					
					
					// 첨부파일 경로정보를 포함한다.
					if ( addAttachFileInfo( oXmlWriter, oResultMap, oAttachments ) == false ) {
						writeIndexLog ( XmlWriter.FAILED );
						continue;
					}				
					
					// --------------------------------------------------------------- CUSTOM LOGIC START
					oResultMap.put("code001", "0"); // 지시업무
					// --------------------------------------------------------------- CUSTOM LOGIC END
					
					// 본문쿼리, 파일쿼리 외에 추가 쿼리를 수행한다.
					if ( queryEtcData ( oDataReaderMap, oResultMap ) == false ) {
						writeIndexLog ( XmlWriter.FAILED );
						continue;					
					}
					
					// xml에 추가
					iState = addResultData( oXmlWriter, oResultMap);
					writeIndexLog ( iState, bWarned );
					
					// --------------------------------------------------------------- CUSTOM LOGIC START
					oResultMap.put("code001", "1");	// 수명업무 
					// 본문쿼리, 파일쿼리 외에 추가 쿼리를 수행한다.
					if ( queryEtcData ( oDataReaderMap, oResultMap ) == false ) {
						writeIndexLog ( XmlWriter.FAILED );
						continue;					
					}
					
					// xml에 추가
					iState = addResultData( oXmlWriter, oResultMap);
					// --------------------------------------------------------------- CUSTOM LOGIC END
				
					writeIndexLog ( iState, bWarned );
					
					if ( logger.isDebugEnabled() ) {
						logger.debug( "<결과 Map>" );
						logger.debug( oResultMap );
					}
				}

			} // for

			//
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
			logger.error ( "현재 데이터소스의 색인을 강제 종료합니다", e );
			
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
			logger.info ( "====================================================================================" );	
			
			if ( oXmlWriter != null ) {
				if ( oResultFile == null ) {
					oXmlWriter.saveFile( null );
				} else {
					oXmlWriter.saveFile( oResultFile.getPath() );
				}
			}
			
			//색인대상과의 연결을 종료합니다.
			closeSourceConnection();
		} // finally
	}	// run()
}

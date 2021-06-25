package com.sql.etcreader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.rayful.bulk.sql.RdbmsEtcReader;
import com.rayful.bulk.util.Utils;

public class SSManualBoard extends RdbmsEtcReader {

	Utils utils = new Utils();
	
	public SSManualBoard( Connection oConnection, Map<String, String> mapEtcQuery ) {
		super( oConnection, mapEtcQuery );
	}
	
	@Override
	/**
	 * 본문/파일 쿼리로 조회할 수 없는 부분을 추가로 쿼리한다.
	 * 이 함수는 반드시 재정의 해야 한다.
	 * dataMap으로 부터 본문 쿼리에서 조회된 데이터를 알 수 있고
	 * resultMap에 세팅해서 이 함수에서 조회한 데이터를 색인에 반영할 수 있다.
	 * <p>
	 * @param	dataMap	본문쿼리로 조회된 데이터
	 * @param	resultMap	본문데이터를 색인데이터 기준으로 변환한 데이터 (색인데이터)
	 * @return 	true : 조회성공 / false : 에러발생 
	 */
	public boolean executeEtcQuery(Map<String, Object> dataMap, Map<String, Object> resultMap) {
		boolean bReturn = true;
		try {
		    fileInfoData( dataMap, resultMap );
		
		} catch ( Exception e) {
			bReturn = false;
			logger.error ( "Fail Etc Query", e);
		}
		
		return bReturn;
	}	
	
	/**
	 * 첨부파일 정보 색인
	 * @param dataMap	본문 데이터
	 * @param resultMap	색인 데이터
	 * @throws Exception
	 */
	public void fileInfoData(Map<String, Object> dataMap, Map<String, Object> resultMap)	throws Exception	{
		String sSql = null;	
		PreparedStatement oPstmt = null;
		ResultSet oRset = null;
		int iRowCnt = 0;
		String sBoardNo = (String)dataMap.get("DOCID");
		
		if(sBoardNo != null & sBoardNo.length() > 0) {
			sSql = map_EtcQuery.get("query_manual_board_file");
		
		} else {
			logger.info( "	>>>no key value");
            return;
		}
		
		if ( sSql == null || sSql.length() < 1) {
            logger.error(" Etc Query is Empty - query_oldspec or query_newspec" );
            return;
        }
		
		logger.info( "	>>>fileInfoData Key: " + sBoardNo);
		
		try {
			oPstmt = mo_sourceConnection.prepareStatement(sSql);
			oPstmt.setString(1, (String)dataMap.get("DOCID"));	// 컬럼명 대문자임에 주의 
			oRset = oPstmt.executeQuery();
			
			StringBuffer osbFileNameList = new StringBuffer();
			StringBuffer osbFilePathList = new StringBuffer();
			
			String sFileNameList = null;
			String sFilePathList = null;
			
			while ( oRset.next() ) {
				iRowCnt ++;
				
				sFileNameList = oRset.getString("FILE_ORGNAME");
				sFilePathList = oRset.getString("FILE_SAVPATH");
				
				if( osbFileNameList != null && osbFileNameList.length() > 0 ) {
					osbFileNameList.append(";").append(sFileNameList);
				} else {
					osbFileNameList.append(sFileNameList);
				}
				
				if( osbFilePathList != null && osbFilePathList.length() > 0 ) {
					osbFilePathList.append(";").append(sFilePathList);
				} else {
					osbFilePathList.append(sFilePathList);
				}
			}

			logger.info( "	iRowCnt=" + iRowCnt );

			//resultMap.put( "prdspeclist", utils.getTextFromHtml(osbSpecList.toString()));
			resultMap.put( "attachedname", osbFileNameList.toString());
			resultMap.put( "attachedpath", osbFilePathList.toString());
			
		} catch ( Exception e) {
			logger.error("Error :" + e.toString());
			throw e;
		
		} finally {
			if ( oRset != null ) try { oRset.close(); } catch ( SQLException se ) {}
			if ( oPstmt != null ) try { oPstmt.close(); } catch ( SQLException se ) {}
		}
	}
}

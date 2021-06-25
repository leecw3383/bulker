package com.rayful.bulk.io;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.KeyData;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.localize.Localization;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class ErrorFile {

	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger( ErrorFile.class.getName(), Config.LOG_PATH );
	
	/** 에러파일 객체 */
	File mo_errorFile = null;
	/** 키컬럼명 배열 ( 데이터 키컬럼명 + 파일 키컬럼명 ) */
	Object [] mo_keyColumnNames = null;
	/** 키컬럼간 구분문자 */
	final String ms_gubunChar = "|";
	
	/*
	 * 생성자
	 * @param	sBasePath	경로정보
	 * @param	sDataSourceId	데이터소스ID
	 * @param	sDataKeyColumnNames	데이터 키컬럼명 배열
	 * @param	sFileKeyColumnNames	파일 키컬럼명 배열
	 */	
	public ErrorFile ( String sBasePath, 
						String sDataSourceId, 
						String [] sDataKeyColumnNames, 
						String [] sFileKeyColumnNames ) 
	{
		if ( sBasePath != null ) {
			if ( sDataSourceId != null ) {
				String sFileName = "error_" + sDataSourceId + ".log";
				mo_errorFile = new File ( sBasePath, sFileName );
				
				// 데이터 키컬럼명들과 파일 키컬럼명들을 병합한다.
				ArrayList<String> oKeyList = new ArrayList<String>();
				
				// 데이터 키컬럼명을 얻는다.
				for ( int i=0; i <sDataKeyColumnNames.length ; i++ ) {
					if ( ! oKeyList.contains( sDataKeyColumnNames[i] )) {
						oKeyList.add(sDataKeyColumnNames[i]);
					}
				}
				
				// 파일 키컬럼명을 얻는다.	
				if ( sFileKeyColumnNames != null ) {
					for ( int i=0; i <sFileKeyColumnNames.length ; i++ ) {
						// 중복은 제거
						if ( ! oKeyList.contains( sFileKeyColumnNames[i] )) {
							oKeyList.add(sFileKeyColumnNames[i]);
						}
					} // for
				} // if ( sFileKeyColumnNames != null ) {
				
				if ( oKeyList.size() > 0 ) {
					mo_keyColumnNames = oKeyList.toArray();
				}
			} // if ( sDataSourceId != null ) {
		} // if ( sBasePath != null ) {
	}

	/*
	 * Key값을 에러파일에 기록한다.
	 * @param	oKeyMap	하나의 키값을  에러파일에 기록
	 */
	public void write( Map<String, String> oKeyMap )
	throws FileNotFoundException, IOException
	{
		if ( mo_errorFile != null ) {
			FileOutputStream oFos = new FileOutputStream ( mo_errorFile, true );
			DataOutputStream oDos = new DataOutputStream ( oFos );
			
			for ( int i=0; i< mo_keyColumnNames.length; i++ ) {
				oDos.writeBytes( (String) oKeyMap.get( (String)mo_keyColumnNames[i]));
				
				if ( i != 0 ) {
					oDos.writeBytes( ms_gubunChar );
				}				
			}
			oDos.writeBytes("\r\n");
			oDos.close();
		}
	}
	
	/*
	 * 여러 Key값들을 에러파일에 기록한다.
	 * @param	oKeyData	여러 키값들을 에러파일에 기록
	 */
	public void write( KeyData oKeyData, int iStartIdx )
	throws FileNotFoundException, IOException
	{
		if ( mo_errorFile != null ) {
			int iRows = oKeyData.size();
			Map<String, String> oKeyMap = null;

			if ( iRows > iStartIdx ) {
				FileOutputStream oFos = new FileOutputStream ( mo_errorFile, true );
				DataOutputStream oDos = new DataOutputStream ( oFos );				
				for ( int j=iStartIdx; j< iRows; j++) {
					oKeyMap = oKeyData.get(j);
	
					for ( int i=0; i< mo_keyColumnNames.length; i++ ) {
						oDos.writeBytes( (String) oKeyMap.get( (String)mo_keyColumnNames[i]));
						if ( i != 0 ) {
							oDos.writeBytes( ms_gubunChar );
						}				
					}
					
					oDos.writeBytes("\r\n");
				}
				oDos.close();
			}
		}
	}
	
	
	/*
	 * 에러파일을 읽어 KeyData객체에 넣는다.
	 * 에러파일은 "," 를 구분자로 키값간에  구분되어져 있다.
	 * @param	oKeyData	KeyQuery의 결과를 담는 객체
	 * @param	iKeyCnt	Key들의 갯수
	 */	
	public void load ( KeyData oKeyData )
	throws FileNotFoundException, IOException
	{
		if ( mo_errorFile != null ) {
			String sKeys = null;
			String [] arrKeyValues = null;
			
			FileInputStream oFis = new FileInputStream ( mo_errorFile );
			InputStreamReader oIsr = new InputStreamReader( oFis );
			BufferedReader obr = new BufferedReader ( oIsr );
			int iCnt = 0;
			while (  (sKeys = obr.readLine()) != null){
				arrKeyValues = sKeys.split( "\\" + ms_gubunChar );
				Map<String, String> oKeyDataMap = new LinkedHashMap<String, String>();
				
				if ( arrKeyValues.length != mo_keyColumnNames.length) {
					throw new IOException ( Localization.getMessage( ErrorFile.class.getName() + ".Exception.0001" ) );
				}
				
				for ( int i=0; i< mo_keyColumnNames.length; i++ ) {
					oKeyDataMap.put( (String)mo_keyColumnNames[i], arrKeyValues[i] );
				}
				
				if ( Config.KEYQUERY_METHOD == Config.KEY_ALL ) {
					oKeyData.add( oKeyDataMap, true );
				} else {
					oKeyData.add( oKeyDataMap );
				}
				
				System.out.print ( "." );
				iCnt ++;

				if ( iCnt % 1000 == 0 ) {
					System.out.println ( Localization.getMessage( ErrorFile.class.getName() + ".Console.0001", iCnt ) );
				}
			}
			System.out.println (" ");
			logger.info ( Localization.getMessage( ErrorFile.class.getName() + ".Logger.0001", iCnt ) );
			obr.close();
		}
	}

	/*
	 * 에러파일을 삭제한다.
	 */
	public void delete (){
		if ( mo_errorFile != null ) {
			mo_errorFile.delete();
		}
	}
	
	public String getPath() {
		String sPath = null;
		
		if ( mo_errorFile != null ) {
			sPath = mo_errorFile.getPath();
		}
		
		return sPath;
	}
}

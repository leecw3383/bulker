package com.rayful.bulk.sql;

import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.RayfulLogger;

/**
 * 이 클래스는 본문 쿼리로 색인 데이터를 모두 조회할 수 없는 경우
 * 추가의 쿼리를 위해 제공하는 추상클래스이다.
 * 추가의 색인 데이터터를 조회하기 위해 개발자는 이 클래스를 상속받아 구현한다.
 * 
 * executeEtcQuery ()  함수는 반드시 재정의한다.
 * 이 함수에  모든 추가 쿼리를 수행하도록 구현한다.
 * 이 함수의 매개변수는 다음 두 가지이다.
 * oDataMap : 본문조회 후 쿼리결과를 가지고있는 Map
 * oResultMap : 색인컬럼 기준으로 조회데이터를 가지고 있는 Map
 * 만일 쿼리 결과를 다음 쿼리에 넘겨야 한다면 oDataMap에 넣은 후 뽑아서 사용하면되고, 
 * 색인값으로 사용하려고 한다면 oResultMap에 담으면 된다.
 * 
 * @author webeng
 */
abstract public class NotesEtcReader  
{
	/** logger 지정 */
	protected static Logger logger = RayfulLogger.getLogger( NotesEtcReader.class.getName(), Config.LOG_PATH );
	
	/** Notes 데이터소스 커넥션 */
	//protected lotus.domino.Session mo_sourceConnection = null;
	/** Notes 데이터소스 커넥터 */
	protected NotesConnector notesConnector = null;
	
	public NotesEtcReader() {}
	/**
	 * 생성자
	 * @param oConnection	데이터소스 커넥션
	 */
	public NotesEtcReader( /*lotus.domino.Session oConnection,*/ NotesConnector notesConnector ) {
		//mo_sourceConnection = oConnection;
		this.notesConnector = notesConnector;
	}
	
//	/**
//	 * 커넥션 객체를 지정한다.
//	 * @param oConnection	데이터소스 커넥션
//	 */	
//	public void setConnection( lotus.domino.Session oConnection ) {
//		mo_sourceConnection = oConnection;
//	}
	
	/**
	 * 본문쿼리, 파일쿼리외에 기타 쿼리를 수행하여
	 * 쿼리데이터 혹은 색인 데이터에 반영한다.
	 * @param oDataMap	쿼리 데이터
	 * @param oResultMap	색인 데이터
	 * @return
	 */
	abstract public boolean executeEtcQuery( Map<String, Object> oDataMap, Map<String, Object> oResultMap ) ;

}

/**
 *******************************************************************
 * 파일명 : BaseIndexer.java
 * 파일설명 : main()을 갖는 클래스는 이 인터페이스를 구현한 클래스 중
 * 						하나의 클래스의 인스턴스를 생성하여 메인로직을 수행
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성             
 *******************************************************************
*/

package com.rayful.bulk.index.indexer;

import com.rayful.bulk.index.DFLoader;
/**
 * 색인메인로직을 갖는 클래스들의  Base Interface<p>
 * 이 Interface를 implemets하는 클래스는 다음 메서드를 반드시 구현해야 한다.<p>
*/
public interface BaseIndexer 
{
	/**
	* 색인메인로직을 구현한다.
	*	@param	oDFLoader	DFLoader객체
	*	@param	oSSWriter	SSWriter객체
	*/	
	public void run( DFLoader oDFLoader)
	throws Exception;
			
	/**
	* 색인수를 제한한다.
	* <p>
	* @param	iMaxRows	제한된색인수
	*/											
	public void setMaxRows( int iMaxRows );
	
	/**
	* 특정 row만 색인을 수행한다.
	* <p>
	* @param	sKeys	색인하려는 데이터의 키값
	*/		
	public void setKey ( String sKeys );
	
	/** 색인대상 전체건수 리턴	*/
	public int getTotalCount();
	
	/** 색인시도 건수 리턴	*/
	public int getTryCount();	
	
	/** 성공된된 건수 리턴	*/
	public int getSuccessCount();

	/** 실패한 건수 리턴	*/	
	public int getFailCount();
	
	/** 경고대상  건수 리턴	*/	
	public int getWarnCount();	
}
/**
 *******************************************************************
 * 파일명 : DFLoaderException.java
 * 파일설명 : DFLoader에서 발생하는 예외를 담당하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/25   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index;

/**
 * DFLoader에서 발생하는 예외를 담당하는 클래스
*/
public class DFLoaderException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String ms_location = "DFLoader.";	// 에러가 발생한 위치정보 저장
	
	/**
	* 생성자
	* @param	sMessage	에러 메시지
	* @param	sLocation	에러 발생위치 정보
	**/	
	public DFLoaderException( String sMessage, String sLocation)
	{
		super(sMessage);
		ms_location += sLocation ;
	}
	
	/**
	 * 에러가 발생한 위치정보를 알아낸다.
	 * @return	에러 발생위치 정보 스트링
	**/	
	public String getLocation () 
	{
		if ( ms_location != null ) {
			return ms_location;
		} else {
			return "";
		}
	}
	
	/**
	* 에러위치및 메시지를 스트링으로 변환한다.
	* @return	에러내용
	*/			
	public String toString () {
		 return "[위치:" + ms_location + "]" + super.toString();
	}
	
}
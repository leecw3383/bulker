package com.rayful.io.namo;


@SuppressWarnings("serial")
public class NamoParserException extends Exception 
{
	/**
	* 생성자
	* @param	sMessage	에러 메시지
	**/	
	public NamoParserException( String sMessage)
	{
		super(sMessage);
	}
	
	
	/**
	* 에러위치및 메시지를 스트링으로 변환한다.
	* @return	에러내용
	*/			
	public String toString () {
		 return super.toString();
	}
		
}

/**
 *******************************************************************
 * 파일명 : HangulConversion.java
 * 파일설명 : 각종 한글관련 컨버전을 수행하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/15   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.util;

import java.sql.*;

import com.rayful.localize.Localization;

import java.io.*;

/**
 * Characterset및 Unicode 컨버전을 수행하는 클래스
*/
public class HangulConversion
{
	
	private static boolean CHARCTERSET_CONVERT = false;
	private static boolean UNICODE_CONVERT = false;

	/** 
	* Characterset 변환여부를 설정<p>
	* @param	bConvert	true: 변환 / false: 변환하지 않음
	*/
	public static void setCharatersetConvert ( boolean bConvert ){
		CHARCTERSET_CONVERT = bConvert;
	}
	
	/** 
	* Unicode 변환여부를 설정<p>
	* @param	bConvert	true: 변환 / false: 변환하지 않음
	*/	
	public static void setUnicodeConvert ( boolean bConvert ){
		UNICODE_CONVERT = bConvert;
	}
	
//	/**
//	*	Unicode2.0 --> Unicode 1.2<p>
//	* 오라클의 
//	* @param	uni20	Unicode2.0스트링
//	* @return	Unicode1.2스트링
//	*/
//	private static String toDB( String uni20 ) throws SQLException 
//	{
//		return toDB( uni20, UNICODE_CONVERT );
//	}
//	
//	/**
//	*	Unicode2.0 --> Unicode 1.2로 변환<p>
//	* @param	uni20	Unicode2.0스트링
//	* @param	convert	true: 변환시행 / false :변환하지 않음
//	* @return	Unicode1.2스트링
//	*/	
//	public static String toDB( String uni20, boolean convert ) throws SQLException 
//	{
//		if ( convert ) {
//			if ( uni20 == null ) {
//				return null;
//			}
//			
//			int len = uni20.length();
//			char [] out = new char[len];
//			
//			for ( int i=0; i< len; i++ ) {
//				char c = uni20.charAt(i);
//				if ( c<0xac00 || c > 0xd7a3 ) {
//						out[i] = c;
//				} else {
//				// Unicode 2.0 한글코드영역
//					try {
//						byte[] ksc = String.valueOf(c).getBytes("KSC5601");
//						if ( ksc.length != 2 ) {
//							out[i] = '\ufffd';
//							System.out.println ( Localization.getMessage( HangulConversion.class.getName() + ".Console.0002" ) );
//						} else {
//							out[i] = (char) (0x3400 + (( ksc[0] & 0xff ) -0xb0 ) * 94 + ( ksc[1] & 0xff ) - 0xa1 );
//						}
//					} catch ( UnsupportedEncodingException ex ) {
//						throw new SQLException ( ex.getMessage() );
//					}
//				}
//			}
//			
//			return new String ( out );
//		} else {
//			return uni20;
//		}
//	}
	
	/**
	*	Unicode1.2 --> Unicode 2.0로 변환<p>
	* @param	uni12	Unicode1.2스트링
	* @return	Unicode2.0스트링
	*/	
	public static String fromDB ( String uni12 ) throws SQLException 
	{
		return fromDB( uni12, UNICODE_CONVERT );
	}
	
	/**
	*	Unicode1.2 --> Unicode 2.0로 변환<p>
	* @param	uni12	Unicode1.2스트링
	* @param	convert	true: 변환시행 / false : 변환하지 않음
	* @return	Unicode2.0스트링
	*/		
	public static String fromDB ( String uni12, boolean convert ) throws SQLException 
	{
		if ( convert ) {
			if ( uni12 == null ) {
				return null;
			}
			
			int len = uni12.length();
			char [] out = new char[len];
			byte [] ksc = new byte[2];
			for ( int i=0; i<len; i++ ){
				char c = uni12.charAt(i);
				if ( c < 0x3400 || c > 0x4dff ) {
					out[i] = c;
				} else if ( c >= 0x3d2e ) {
					// Unicode 1.2 한글보충영역 A, B
					out[i] = '\ufffd';
					System.out.println ( Localization.getMessage( HangulConversion.class.getName() + ".Console.0001" ) );
				} else {
					// Unicode 1.2의 KSC5601 대응 한글 영역
					try {
						ksc[0] = (byte) (( c-0x3400) /94 + 0xb0 );
						ksc[1] = (byte) (( c-0x3400) %94 + 0xa1 );
						out[i] = new String ( ksc, "KSC5601" ).charAt(0);
					} catch ( UnsupportedEncodingException ex ) {
						throw new SQLException ( ex.getMessage() );
					}
				}
			}
			return new String ( out );
		} else {
			return uni12;
		}
	}
	
	
	/**
	*	"KSC5601" code를 "8859_1" code로 encoding <p>
	* @param	ko	"KSC5601"로 encoding된 스트링
	* @return	"8859_1" code로 encoding된 스트링
	*/		
	public static String toEng ( String ko ) throws SQLException 
	{
		return toEng ( ko, CHARCTERSET_CONVERT );
	}
	
	/**
	*	"KSC5601" code를 "8859_1" code로 encoding <p>
	* @param	ko	"KSC5601"로 encoding된 스트링
	* @param	convert	true: 변환시행 / false : 변환하지 않음
	* @return	"8859_1" code로 encoding된 스트링
	*/	
	public static String toEng ( String ko, boolean convert ) throws SQLException 
	{
		if ( convert ) {
			if ( ko == null ) {
				return null;
			}
			String new_str = null;
			try {
				new_str = new String ( ko.getBytes( "KSC5601"), "8859_1" );
			} catch ( UnsupportedEncodingException ex ) {
				throw new SQLException ( ex.getMessage());
			}
			return new_str;
		} else {
			return ko;
		}
	}
	
	
	/**
	*	"8859_1" code를 "KSC5601" code로 encoding <p>
	* @param	en	"8859_1" code로 encoding된 스트링
	* @return	"KSC5601"로 encoding된 스트링
	*/			
	public static String toKor ( String en) throws SQLException 
	{
		return toKor(en, CHARCTERSET_CONVERT );
	}
	
	/**
	*	"8859_1" code를 "KSC5601" code로 encoding <p>
	* @param	en	"8859_1" code로 encoding된 스트링
	* @param	convert	true: 변환시행 / false : 변환하지 않음	
	* @return	"KSC5601"로 encoding된 스트링
	*/	
	public static String toKor ( String en, boolean convert ) throws SQLException 
	{
		if ( convert ) {
			if ( en == null ) {
				return null;
			}
			
			String new_str = null;
			try {
				new_str = new String ( en.getBytes( "8859_1" ), "KSC5601" );
			} catch ( UnsupportedEncodingException ex ) {
				throw new SQLException ( ex.getMessage() );
			}
			return new_str;
			
		} else {
			return en;
		}
	}
	
}

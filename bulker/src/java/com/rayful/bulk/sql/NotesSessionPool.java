/**
 *******************************************************************
 * 파일명 : NotesSessionPool.java
 * 파일설명 : NOTES의 Session을 일정수 저장하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/08/03   정충열   최초작성 
 * 2005/08/05   정충열   mi_maxSize = 1로 조정
 *******************************************************************
*/

package com.rayful.bulk.sql;

import lotus.domino.*;
import java.util.*;

/**
 * NOTES의 Session을 일정수 저장하여 많은 세션이 맺어지지 않도록한다.
 * 너무 많은 세션을 맺으면 java.lang.OutOfMemoryError가 발생한다.
 * 
 * 세션을 저장해서 재사용하려고 했지만  일정시간 뒤 세션이 끊겨서 문제발생
 * hostName을 정렬하여 DF를 작성, 같은 host에는 한번의 세션만 맺도록 조정 ( mi_maxSize=1로 조정 )
*/
public class NotesSessionPool 
{
	private static TreeMap<String, lotus.domino.Session> mo_sessionMap = new TreeMap<String, lotus.domino.Session>();
	private static int mi_maxSize = 1;
	
	/**
	* Pool에서 Session객체를 찾는다.
	* <p>
	* @param	sHostName	노츠세션을 맺을려는 호스트명
	* @return	NotesSession	노츠세션객체
	*/
	public static lotus.domino.Session getSession ( String sHostName ) {
		return ( lotus.domino.Session ) mo_sessionMap.get( sHostName );
	}
	
	/**
	* Pool에 Session객체를 넣는다.
	* <p>
	* @param	sHostName	노츠세션을 맺을려는 호스트명
	* @param	oSession	Session객체
	*/	
	public static void putSession ( String sHostName, lotus.domino.Session oSession ) {
		if ( sHostName != null && oSession != null ) {
			
			if ( mo_sessionMap.size() == mi_maxSize ) {
				String sLastKey = (String) mo_sessionMap.lastKey();
				lotus.domino.Session oNotesSession = ( lotus.domino.Session ) mo_sessionMap.get( sLastKey );				
				try { oNotesSession.recycle(); 	} catch ( NotesException ne ) {}
				mo_sessionMap.remove( sLastKey );
			}
			
			if ( mo_sessionMap.size() < mi_maxSize ) {
				if ( mo_sessionMap.containsKey( sHostName ) == false ) {
					mo_sessionMap.put ( sHostName, oSession );
				}
			} 
		}
	}
	
	public static void clearSession () {
		Object [] aKeyset = mo_sessionMap.keySet().toArray();
		String sHostName = null;
		lotus.domino.Session oNotesSession;
		
		for ( int i=0; i< aKeyset.length; i++ ) {
			sHostName = aKeyset[i].toString();
			oNotesSession = ( lotus.domino.Session ) mo_sessionMap.get( sHostName );
			try { oNotesSession.recycle(); 	} catch ( NotesException ne ) {}
			mo_sessionMap.remove( sHostName );
		}
	}
	
	/** 
	*	객체소멸시점에서 Session객체를 닫는다.
	*/
	protected void finalize() throws Throwable
	{
		clearSession();
		super.finalize();
	}
}
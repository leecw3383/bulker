package com.rayful.bulk.index;

import java.util.ArrayList;
import java.util.Map;

public class KeyData {
	ArrayList<Map<String, String>> mo_keyList= null;
	
	public KeyData() {
		mo_keyList = new ArrayList<Map<String, String>>();
	}
	
	public void add ( Map<String, String> oKeyDataMap ) {
		add ( oKeyDataMap, false );
	}
		
	public boolean add ( Map<String, String> oKeyDataMap, boolean bCheckDuplicate ) {
		
		boolean bDuplicate = false;
	
		if ( bCheckDuplicate ) {
			//중복제거...
			if ( mo_keyList.contains(oKeyDataMap) == false ) {
				mo_keyList.add( oKeyDataMap );
			} else {
				bDuplicate = true;
			}
		} else {
			mo_keyList.add( oKeyDataMap );
		}
	
		return bDuplicate;
	}
	
	public Map<String, String> get( int index ) {
		return (Map<String, String>)mo_keyList.get( index );
	}
	
	public int size() {
		return mo_keyList.size();
	}
}

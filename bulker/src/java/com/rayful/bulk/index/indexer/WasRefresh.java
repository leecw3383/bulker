package com.rayful.bulk.index.indexer;

import java.net.*;
import java.io.*;

public class WasRefresh {
	public static void main(String[] argv) {
		
		if (argv.length != 1) printUsage();
		
		if ("rabigh".equalsIgnoreCase(argv[0])) {
			refresh("http://search.rpp2.com/search/refresh.do");
		} else if ("jeddah".equalsIgnoreCase(argv[0])) {
			refresh("http://jdsearch.rpp2.com/search/refresh.do");
		} else if ("newcastle".equalsIgnoreCase(argv[0])) {
			refresh("http://uksearch.rpp2.com/search/refresh.do");
		}
		
	}

	public static void refresh(String s1) {
		try {
			URL url = new URL(s1);
			URLConnection conn = url.openConnection();
			conn.setDoOutput(true);
			OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
			wr.write("<!-- *** Was Refresh START ***");
			wr.write(s1);
			wr.write("*** Was Refresh END *** -->");
			wr.flush();
			wr.close();
			String line=null;
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			while ((line = rd.readLine()) != null) {
				System.out.println(line);
			}
			rd.close();
		} catch (Exception e) {e.printStackTrace();}
	}
	
	/**
	 * 프로그램 사용법을 콘솔에 출력한다.
	 */
	private static void printUsage() {
		// Parameter를 지정하지 않으면 프로그램을 종료합니다.
		System.out.println( " ***** WasRefresh.java *****");
		System.out.println( " 기능  : WAS Notes 정보를 갱신한다. ");
		System.out.println( " 사용법: java com.rayful.bulk.index.indexer.WasRefresh [rabigh|jeddah|newcastle] ");
		System.out.println( " ");
	    
		System.exit(1);
	}

}

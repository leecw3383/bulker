package com.rayful.bulk.index;

import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import org.apache.log4j.Logger;

public abstract class ResultWriter {
	/** 색인수행결과 상수 : 등록됨  */
	public static final int SUCCESSED = 1;
	/** 색인수행결과 상수 : 변경작업실패  */
	public static final int FAILED = 4;
	
	public boolean setColumnMatching (ColumnMatching oColumnMatching) {return false;}
	public void setLogPath (Logger oNewLogger) {}
	public boolean addDocument(Map<String, Object> oResultMap) {return false;}
	public boolean saveFile( String sDestFileName ) {return false; }
	public boolean saveFile( String sDestFileName, int mi_resultFileSize ) {return false; }
	public String getFileExt() { return null; }

	/**
     * html tag를 제외한 text만 뽑아서 리턴한다
     * @param htmlText
     * @return tag를 제외한 text
     */
    protected static String getTextFromHtml(String htmlText) {

        final StringBuffer buf = new StringBuffer();
        HTMLEditorKit.ParserCallback callback = new HTMLEditorKit.ParserCallback() {
            boolean isSkip = false;

            public void handleText(char[] data, int pos) {
                if (!isSkip) {
                    //buf.append(new String(data));
                	buf.append(new String(data)).append(" ");
                }
            }

            public void handleStartTag(HTML.Tag t, MutableAttributeSet a, int pos) {
                String tag = t.toString().toLowerCase();

                if ("script".equals(tag) || "head".equals(tag)) {
                    isSkip = true;
                }
            }

            public void handleEndTag(HTML.Tag t, int pos) {

                String tag = t.toString().toLowerCase();

                if("script".equals(tag) || "head".equals(tag)) {
                    isSkip = false;
                }
            }
            
//            public void handleError(String errorMsg, int pos)
//            {
//            	System.out.println("ERROR\t" + new String(errorMsg));
//            	isSkip = true;
//            }
        };
        try {
            Reader reader = new StringReader(htmlText);
            
            new ParserDelegator().parse(reader, callback, true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return buf.toString();

    }
    
    /**
     * html tag에서 Head 위치를 리턴한다.
     * @param htmlText
     * @return Head 위치
     */
    protected static int[] getHeadPositionFromHtml(String htmlText) {

    	// 정상동작하지 않아 아래 로직으로 사용함.
//    	final int HeadPos[] = new int[2];
//        HTMLEditorKit.ParserCallback callback = new HTMLEditorKit.ParserCallback() {
//            
//            public void handleStartTag(HTML.Tag t, MutableAttributeSet a, int pos) {
//                String tag = t.toString().toLowerCase();
//                
//                if ("head".equals(tag)) {
//                    HeadPos[0] = pos;
//                }
//            }
//
//            public void handleEndTag(HTML.Tag t, int pos) {
//
//                String tag = t.toString().toLowerCase();
//
//                if("head".equals(tag)) {
//                    HeadPos[1] = pos;
//                }
//            }
//        };
//        try {
//            Reader reader = new StringReader(htmlText);
//            
//            new ParserDelegator().parse(reader, callback, true);
//        } catch (Exception e) {
//            throw new RuntimeException(e.getMessage());
//        }
    	
    	int HeadPos[] = new int[2];
    	
    	if(htmlText != null && htmlText.length() > 0) {
    		int iSPos = htmlText.toLowerCase().indexOf("<head>");
    		int iEPos = htmlText.toLowerCase().indexOf("</head>");
    		
    		HeadPos[0] = iSPos > 0 ? iSPos : 0;
			HeadPos[1] = iEPos > 0 ? iEPos : 0;
    	}
    	
        return HeadPos;

    }	
}

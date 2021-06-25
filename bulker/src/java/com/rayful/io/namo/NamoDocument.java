package com.rayful.io.namo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64.Decoder;
//java 1.8 에서 지원되지 않아 주석 처리
//import sun.misc.BASE64Decoder;

import com.rayful.localize.Localization;

public class NamoDocument {
	/** Namo 파일명 */
	private String ms_namoFilePathName;
	
	/** 포함파일 헤더 리스트 */
	private Map<String, NamoHeader> mo_AttHeaderList;
	/** 포함파일 내용 리스트 */
	private Map<String, String> mo_AttContentList;
	
	
	/**
	* 생성자
	* <p>
	* @param	sNamoFilePathName	Namo파일의 파일및 경로명
	*/	
	public NamoDocument ( String sNamoFilePathName ) {
		ms_namoFilePathName = sNamoFilePathName;

		mo_AttHeaderList = new HashMap<String, NamoHeader>();
		mo_AttContentList = new HashMap<String, String>();
	}

	
	
	/**
	* Namo 파일 경로정보를 설정
	* <p>
	* @param	sNamoFilePathName	Namo 파일 경로정보
	*/		
	public void setPath ( String sNamoFilePathName ) {
		ms_namoFilePathName = sNamoFilePathName;
	}	
		
	/**
	* Namo파일에 포함되어 있는 첨부파일정보(첨부헤더정보 + 첨부내용)를 추가
	* <p>
	* @param	oNamoHeader	첨부파일에 대한 헤더정보
	* @param	sAttFileContent	첨부파일내용 스트링	
	*/	
	public void addAttachFile ( NamoHeader oNamoHeader, String sAttFileContent ) {
		String sAttFileName = oNamoHeader.getFileName();
		
		if ( sAttFileName != null ) {
			mo_AttHeaderList.put ( sAttFileName, oNamoHeader );
			mo_AttContentList.put ( sAttFileName, sAttFileContent );
		}
	}
	
	/**
	* Namo파일의 경로를 알아낸다.
	* <p>
	* @return	Namo파일의 경로
	*/		
	public String getPath () {
		return ms_namoFilePathName;
	}		
	
	
	/**
	* Namo파일내 포함된 첨부파일의 갯수를 알아낸다.
	* <p>
	* @return	Namo파일내 포함된 첨부파일의 갯수
	*/	
	public int getAttachFileCount() {
		return mo_AttHeaderList.size();
	}		
	
	/**
	* Namo파일내 포함된 첨부파일명들을 알아낸다.
	* <p>
	* @return	Namo파일내 포함된 첨부파일명의 String배열
	*/	
	public String []  getAttachFileNames() {
		Object [] oAttNames = mo_AttHeaderList.keySet().toArray();
		String [] sAttNames = new String [ mo_AttHeaderList.size() ];
		for ( int i=0; i< oAttNames.length; i ++ ) {
			sAttNames [i]= (String) oAttNames [i];
		}
		
		return sAttNames;
	}
	
	/**
	* Namo파일내 포함된 첨부파일명을 키로 해서 
	* 첨부파일 헤더정보를 알아낸다.
	* <p>
	* @param	sAttFileName	Namo파일내 포함된 첨부파일명
	* @return	Namo파일내 포함된 첨부파일의 헤더정보
	*/	
	private NamoHeader getAttachFileHeader( String sAttFileName ) {
		return ( NamoHeader ) mo_AttHeaderList.get ( sAttFileName );
	}
		

	/**
	* Namo파일내 포함된 첨부파일명을 키로 해서 
	* 첨부파일 내용정보를 알아낸다.
	* <p>
	* @param	sAttFileName	Namo파일내 포함된 첨부파일명
	* @return	Namo파일내 포함된 첨부파일의 내용 스트링
	*/	
	private String getAttachFileContent ( String sAttFileName ) {
		return ( String ) mo_AttContentList.get ( sAttFileName );
	}	
	
			
	/**
	* Namo파일내 포함된 첨부파일중 하나를 물리적인 파일로 저장
	* <p>
	* @param	sAttFileName	Namo 파일내 포함된 첨부파일명
	* @param	sSaveFilePathName	저장할 물리적인 파일의 경로및 이름
	* @return	저장된파일에 대한  File 객체
	*/		
	public File saveAttachFile ( String sAttFileName, String sSaveFilePathName ) 
	throws IOException
	{
		// 첨부파일 헤더 정보를 추출
		NamoHeader oNamoHeader = getAttachFileHeader( sAttFileName );
		// 첨부파일 내용 정보를 추출
		//String sAttFileContent = getAttachFileContent ( sAttFileName );
		
		int iContentTransferEncodingCode = oNamoHeader.getContentTransferEncodingCode();
		//String sCharset = oNamoHeader.getCharset();
		//String sContentType= oNamoHeader.getContentType();
		
		// Base64 디코딩 관련...
		//BASE64Decoder oBase64Decoder = new BASE64Decoder();		
		//byte [] bDecodedBytes = null;
		
		if ( iContentTransferEncodingCode == NamoHeader.CTE_BASE_64 ) {
			// 출력파일객체 생성
			OutputStream os = new  FileOutputStream( sSaveFilePathName );
			
			// BASE64 디코딩하여 파일에 출력
			//bDecodedBytes = oBase64Decoder.decodeBuffer( sAttFileContent );
			
			if ( os != null ) {
				os.flush();
				os.close();
				os = null;
			}			
		}	else {
			throw new IOException( Localization.getMessage( NamoDocument.class.getName() + ".Exception.0001" ) );
		}
		
		return new File ( sSaveFilePathName );
	}
	
	/**
	 * 나모문서 내에서 content-type 이 text/html(본문) 만 하나의 파일로 
	 * 저장한다. 
	 * @param sSaveFilePathName	저장할 경로명 ( 파일명포함)
	 * @return	저장한 파일 객체 (FILE)
	 * @throws IOException
	 */
	public File saveBodyFile( String sSaveFilePathName )
	throws IOException	
	{
		String[] sAttachFileNames = this.getAttachFileNames();
		NamoHeader namoHeader = null;
		String sAttFileContent = null;
		OutputStream os  = null;
		String sContentType = null;
		
		// Base64 디코딩 관련...
		// java 1.8 에서 지원되지 않아 주석 처리
		//BASE64Decoder oBase64Decoder = new BASE64Decoder();	
		// java 1.8에서 지원되는 형식으로 변경
		Decoder oBase64Decoder = Base64.getDecoder();
		
		byte [] bDecodedBytes = null;
		
		try {
		// 출력파일객체 생성
			os = new  FileOutputStream( sSaveFilePathName );
			for( int i=0; i< sAttachFileNames.length; i++ ) {
				namoHeader = this.getAttachFileHeader( sAttachFileNames[i]);
				sAttFileContent = this.getAttachFileContent( sAttachFileNames[i] );
				sContentType = namoHeader.getContentType();
				
				if ( sContentType != null && sContentType.equalsIgnoreCase("text/html")) {
					// BASE64 디코딩하여 파일에 출력
					// java 1.8 에서 지원되지 않아 주석 처리
					//bDecodedBytes = oBase64Decoder.decodeBuffer( sAttFileContent );
					// java 1.8에서 지원되는 형식으로 변경
					bDecodedBytes = oBase64Decoder.decode( sAttFileContent );

					os.write ( bDecodedBytes );
				}
			}
		} catch ( IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if ( os!= null ) try {os.close();} catch( IOException ioe){};
		}
		
		return new File ( sSaveFilePathName );
	}
	
	public String getcharSet() {
		String[] sAttachFileNames = this.getAttachFileNames();
		NamoHeader namoHeader = null;
		String sContentType = null;
		String sCharset = null;		
		
		for( int i=0; i< sAttachFileNames.length; i++ ) {
			namoHeader = this.getAttachFileHeader( sAttachFileNames[i]);
			
			if ( sContentType != null && sContentType.equalsIgnoreCase("text/html")) {
				sCharset = namoHeader.getCharset();
				if ( sCharset != null )	break;
			}
		}
		return sCharset;
	}
		
	/**
	* 객체의 멤버변수 내용을 하나의 String만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/			
	public String toString() {
		StringBuffer osb = new StringBuffer();
		String [] sAttFileNames = getAttachFileNames();
		
		osb.append( "\nNamo FILE NAME: " +  ms_namoFilePathName );
		
		for ( int i=0; i< sAttFileNames.length; i ++) {
			osb.append( "\n--------------------------------------" );
			osb.append( "\nNamo ATT HEADER: " );
			osb.append( getAttachFileHeader(sAttFileNames[i] ) );
			osb.append( "\nNamo ATT CONTENT: " );
			osb.append( getAttachFileContent(sAttFileNames[i] ) );
			osb.append( "\n--------------------------------------" );
		}
		
		return osb.toString();
	}
}

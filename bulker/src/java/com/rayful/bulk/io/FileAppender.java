/**
 *******************************************************************
 * 파일명 : FileAppender.java
 * 파일설명 : 일반파일에  파일의 텍스트 내용만을 붙여넣는다.
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성
 * 2005/11/08   정충열    다국어기능 추가
 *******************************************************************
*/

package com.rayful.bulk.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;

import com.rayful.bulk.index.Config;

/**
 * 첨부파일및 데이더들을 파일에 붙여넣는다.<p>
*/
public class FileAppender
{
	String ms_destFileName;	//결과파일명
	
	// 파일 확장자 상수
	/** 색인할 수 없는 확장자 */
	public static final int UNKNOWN = 0;
	/** MS-Word */
	public static final int DOC = 1;
	/** MS-Word 2007 */
	public static final int DOCX = 2;
	/** MS-PowerPoint */
	public static final int PPT = 3;
	/** MS-PowerPoint 2007 */
	public static final int PPTX = 4;
	/** MS-Excel */
	public static final int XLS = 5;
	/** MS-Excel 2007 */
	public static final int XLSX = 6;
	/** MS-Project */
	public static final int MPP = 7;
	/** PDF */
	public static final int PDF = 8;
	/** Text 문서 */
	public static final int TXT = 9;
	/** HTM 문서 */
	public static final int HTM = 10;
	/** HTML 문서 */
	public static final int HTML = 11;
	/** MHT 문서 */
	public static final int	MHT = 12;
	/** XML 문서 */
	public static final int XML = 13;
	/** DOT */
	public static final int DOT= 14;
	/** 한글워드 */
	public static final int HWP = 15;
	/** Auto-Cad DWG */
	public static final int DWG = 16;
	/** ZIP 압축파일  */
	public static final int ZIP = 17;
	/** CSV 파일  */
	public static final int CSV = 18;
	/** MHTML 문서  */
	public static final int MHTML = 19;
	/** XHTML 문서  */
	public static final int XHTML = 20;
	/** OOXML 문서  */
	public static final int XSD = 21;
	/** OLE2 문서  */
	public static final int XLSB = 22;
	/** OpenDocument 문서  */
	public static final int ODT = 23;
	/** iWorks document 문서  */
	public static final int NUMBERS = 24;
	public static final int PAGES = 25;
	public static final int KEY = 26; // KEYNOTE
	/** iWorks document 문서  */
	public static final int WPD = 27;
	/** Electronic Publication 문서  */
	public static final int EPUB = 28;
	/** Rich Text 문서  */
	public static final int RTF = 29;
	/** Compression and packaging 파일  */
	public static final int TAR = 30;
	public static final int SEVENZIP = 31;
	public static final int GZ = 32;
	public static final int BZIP = 33;
	public static final int XZ = 34;
	public static final int RAR = 35;
	/** Feed and Syndication 파일  */
	public static final int RSS = 36;
	/** Help 문서  */
	public static final int CHM = 37;
	/** Audio 파일  */
	public static final int MP3 = 38;
	/** Image 파일  */
	public static final int PNG = 39;
	public static final int GIF = 40;
	public static final int BMP = 41;
	public static final int JPG = 42;
	public static final int JPEG = 43;
	public static final int TIF = 44;
	public static final int TIFF = 45;
	public static final int WEBP = 46;
	/** Video 파일  */
	public static final int MP4 = 47;
	/** Class, Jar 파일  */
	public static final int CLASS = 48;
	public static final int JAR = 49;
	/** Source Code 파일  */
	public static final int C = 50;
	public static final int CPP = 51;
	public static final int GROOVY = 52;
	public static final int JAVA = 53;
	/** Mail 파일  */
	public static final int MSG = 54;
	public static final int EML = 55;
	public static final int PST = 56;
	public static final int MBOX = 57;
	public static final int DAT = 58; // TNEF 
	/** Font 파일  */
	public static final int TTF = 59;
	public static final int AFM = 60;
	/** Scientific 파일  */
	public static final int HDF = 61;
	/** Database 파일  */
	public static final int DBTHREE = 62;
	public static final int DB = 63;
	public static final int MDB = 64;
	public static final int SQLITETHREE = 65;
	public static final int SQLITE = 66;
	/** Crypto 파일  */
	public static final int PSEVENB = 67;
	
	/** encoding */
	String ms_txtEncode;
	String ms_logPath;

	/**
	 * 생성자
	 * <p>
	 * @param	sDestFileName	결과를 저장할 파일명
	*/
	public FileAppender( String sDestFileName ) 
	{
		ms_destFileName = sDestFileName;
		ms_txtEncode = "utf-8";
		ms_logPath = Config.LOG_PATH;

	}

	
	/**
	 * 내용들을 붙일려는 결과파일의 파일명을 알아낸다. (경로포함)
	 * @return	파일명
	*/	
	public String getFileName ()
	{
		return ms_destFileName;
	}

	/**
	 * 스트림(FileInputStream)을 파일에 append한다.
	 * 파일의 내용을 추가할때 사용한다.
	 * 인코딩을 설정하지는 않는다.
	 * <p>
	 * @param	in	FileInputStream
	*/		
	
	public void append( FileInputStream fin ) 
	throws FileNotFoundException, IOException
	{
		BufferedInputStream bin = new BufferedInputStream( fin );
		
		FileOutputStream fout = new FileOutputStream( ms_destFileName, true );
		BufferedOutputStream bout = new BufferedOutputStream( fout );
		
		int buf;
		while( ( buf = bin.read() ) != -1 ) {
			bout.write( buf );
		}
		bout.close() ;
		bin.close();
	}

	/**
	 * 스트림을 파일에 append한다.
	 * RdbmsDataReader에서DB의 본문컬럼( LONGVARBINARY, BLOB 타입 컬럼)을 
	 * 읽어 파일로 저장할 때 사용한다. 
	 * 저장시 인코딩변환을 시도하지 않는다.
	 * <p>
	 * @param	in	InputStream 객체
	*/		
	public void append( InputStream in ) 
	throws FileNotFoundException, IOException
	{
		BufferedInputStream bin = new BufferedInputStream( in );
		
		FileOutputStream fout = new FileOutputStream( ms_destFileName, true );
		//OutputStreamWriter osw = new OutputStreamWriter ( fout, ms_txtEncode );
		//BufferedWriter bw = new BufferedWriter ( osw );
		BufferedOutputStream bw = new BufferedOutputStream( fout);
		
		int buf;
		while( ( buf = bin.read() ) != -1 ) {
			bw.write( buf );
		} 
		bin.close() ;
		bw.close();
	}
	
	/**
	 * Reader객체를 이용해 파일에 append한다.
	 * RdbmsDataReader에서 DB의 본문컬럼(LONGVARCHAR, CLOB 타입 컬럼)을 
	 * 읽어 파일로 저장할 때 사용한다. 
	 * 저장시 UTF-8으로 인코딩한다.
	 * <p>
	 * @param	oReader	Reader 객체
	*/		
	public void append( Reader oReader ) 
	throws FileNotFoundException, IOException
	{
		if ( oReader == null ) return;

		FileOutputStream fout = new FileOutputStream( ms_destFileName, true );
		OutputStreamWriter osw = new OutputStreamWriter ( fout, ms_txtEncode );
		BufferedWriter bw = new BufferedWriter ( osw );
		
		int n = 0; 
		while ((n = oReader.read()) != -1)
		{ 
			bw.write ( n );
		}
		
		oReader.close();
		bw.close();
		
		/*
		// NIO 테스트 중
		char[] byt = new char[10];
		
		FileChannel fchout = fout.getChannel();
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		
		while ((oReader.read(byt)) != -1)
		{
			buf = ByteBuffer.allocateDirect(byt.length);
			buf.put(String.valueOf(byt).getBytes());
			buf.clear();
			fchout.write(buf);
		}
		
		fchout.close();
		
		oReader.close();
		bw.close();
		*/
	}	
	
	/**
	 * 문자열을 파일에 append한다.
	 * 저장시 UTF-8으로 인코딩한다
	 * <p>
	 * @param	sString	문자열
	*/		
	public void append( String sString ) 
	throws FileNotFoundException, IOException
	{
		if ( sString != null && sString.length() > 0) {
			FileOutputStream fout = new FileOutputStream( ms_destFileName, true );
			BufferedOutputStream bout = new BufferedOutputStream( fout );
			OutputStreamWriter osw = new OutputStreamWriter ( bout, ms_txtEncode );
			osw.write ( sString , 0, sString.length() );
			osw.close();
		}
	}
	
	/**
	 * 문자열을 파일에 overwrite 한다.
	 * 저장시 UTF-8으로 인코딩한다
	 * <p>
	 * @param	sString	문자열
	*/		
	public void write( String sString ) 
	throws FileNotFoundException, IOException
	{
		if ( sString != null && sString.length() > 0) {
			FileOutputStream fout = new FileOutputStream( ms_destFileName, false );
			BufferedOutputStream bout = new BufferedOutputStream( fout );
			OutputStreamWriter osw = new OutputStreamWriter ( bout, ms_txtEncode );
			osw.write ( sString , 0, sString.length() );
			osw.close();
		}
	}
	
	
	/**
	 * 파일명으로 부터 확장자를 알아낸다.
	 * <p>
	 * @param	sFileName	파일명
	 * @return	파일확장자명
	*/	
	public static String getFileExtentionName( String sFileName ) 
	{
		int iPosition;
		String sExtName ="";
		
		if ( sFileName == null ) {
			return sExtName;
		}
		
		iPosition = sFileName.lastIndexOf(".");
		if ( iPosition > -1 ) {
			sExtName = sFileName.substring( iPosition + 1);
		}
		
		return sExtName.toUpperCase();
	}
	
	/**
	 * 파일명으로 부터 확장자를 알아낸다.
	 * <p>
	 * @param	sFileName	파일명
	 * @return 파일확장자타입
	*/	
	public static int getFileExtentionType( String sFileName ) 
	{
		int iFileExtType = 0;
		
		String sFileExtName= FileAppender.getFileExtentionName( sFileName );
		
		if ( sFileExtName.equals( "DOC" ) ) {
			iFileExtType = FileAppender.DOC;
		} else if ( sFileExtName.equals( "DOCX" ) ) {
			iFileExtType = FileAppender.DOCX;
		} else if ( sFileExtName.equals( "PPT" ) ) {
			iFileExtType = FileAppender.PPT;
		} else if ( sFileExtName.equals( "PPTX" ) ) {
			iFileExtType = FileAppender.PPTX;
		} else if ( sFileExtName.equals( "XLS" ) ) {
			iFileExtType = FileAppender.XLS;
		} else if ( sFileExtName.equals( "XLSX" ) ) {
			iFileExtType = FileAppender.XLSX;
		} else if ( sFileExtName.equals( "MPP" ) ) {
			iFileExtType = FileAppender.MPP;
		} else if ( sFileExtName.equals( "PDF" ) ) {
			iFileExtType = FileAppender.PDF;
		} else if ( sFileExtName.equals( "TXT" ) ) {
			iFileExtType = FileAppender.TXT;
		} else if ( sFileExtName.equals( "HTM" ) ) {
			iFileExtType = FileAppender.HTM;
		} else if ( sFileExtName.equals( "HTML" ) ) {
			iFileExtType = FileAppender.HTML;
		} else if ( sFileExtName.equals( "MHT" ) ) {
			iFileExtType = FileAppender.MHT;
		} else if ( sFileExtName.equals( "XML" ) ) {
			iFileExtType = FileAppender.XML;
		} else if ( sFileExtName.equals( "DOT" ) ) {
			iFileExtType = FileAppender.DOT;
		} else if ( sFileExtName.equals( "HWP" ) ) {
			iFileExtType = FileAppender.HWP;
		} else if ( sFileExtName.equals( "DWG" ) ) {
			iFileExtType = FileAppender.DWG;
		} else if ( sFileExtName.equals( "ZIP" ) ) {
			iFileExtType = FileAppender.ZIP;
		} else if ( sFileExtName.equals( "CSV" ) ) {
			iFileExtType = FileAppender.CSV;
		} else if ( sFileExtName.equals( "MHTML" ) ) {
			iFileExtType = FileAppender.MHTML;
		} else if ( sFileExtName.equals( "XHTML" ) ) {
			iFileExtType = FileAppender.XHTML;
		} else if ( sFileExtName.equals( "XSD" ) ) {
			iFileExtType = FileAppender.XSD;
		} else if ( sFileExtName.equals( "XLSB" ) ) {
			iFileExtType = FileAppender.XLSB;
		} else if ( sFileExtName.equals( "ODT" ) ) {
			iFileExtType = FileAppender.ODT;
		} else if ( sFileExtName.equals( "NUMBERS" ) ) {
			iFileExtType = FileAppender.NUMBERS;
		}  else if ( sFileExtName.equals( "PAGES" ) ) {
			iFileExtType = FileAppender.PAGES;
		}  else if ( sFileExtName.equals( "KEY" ) ) {
			iFileExtType = FileAppender.KEY;
		}  else if ( sFileExtName.equals( "WPD" ) ) {
			iFileExtType = FileAppender.WPD;
		} else if ( sFileExtName.equals( "EPUB" ) ) {
			iFileExtType = FileAppender.EPUB;
		} else if ( sFileExtName.equals( "RTF" ) ) {
			iFileExtType = FileAppender.RTF;
		} else if ( sFileExtName.equals( "TAR" ) ) {
			iFileExtType = FileAppender.TAR;
		} else if ( sFileExtName.equals( "7Z" ) ) {
			iFileExtType = FileAppender.SEVENZIP;
		} else if ( sFileExtName.equals( "GZ" ) ) {
			iFileExtType = FileAppender.GZ;
		} else if ( sFileExtName.equals( "BZ2" ) ) {
			iFileExtType = FileAppender.BZIP;
		} else if ( sFileExtName.equals( "XZ" ) ) {
			iFileExtType = FileAppender.XZ;
		} else if ( sFileExtName.equals( "RAR" ) ) {
			iFileExtType = FileAppender.RAR;
		} else if ( sFileExtName.equals( "RSS" ) ) {
			iFileExtType = FileAppender.RSS;
		} else if ( sFileExtName.equals( "CHM" ) ) {
			iFileExtType = FileAppender.CHM;
		} else if ( sFileExtName.equals( "MP3" ) ) {
			iFileExtType = FileAppender.MP3;
		} else if ( sFileExtName.equals( "PNG" ) ) {
			iFileExtType = FileAppender.PNG;
		} else if ( sFileExtName.equals( "GIF" ) ) {
			iFileExtType = FileAppender.GIF;
		} else if ( sFileExtName.equals( "BMP" ) ) {
			iFileExtType = FileAppender.BMP;
		} else if ( sFileExtName.equals( "JPG" ) ) {
			iFileExtType = FileAppender.JPG;
		} else if ( sFileExtName.equals( "JPEG" ) ) {
			iFileExtType = FileAppender.JPEG;
		} else if ( sFileExtName.equals( "TIF" ) ) {
			iFileExtType = FileAppender.MP3;
		} else if ( sFileExtName.equals( "TIFF" ) ) {
			iFileExtType = FileAppender.MP3;
		} else if ( sFileExtName.equals( "WEBP" ) ) {
			iFileExtType = FileAppender.WEBP;
		} else if ( sFileExtName.equals( "MP4" ) ) {
			iFileExtType = FileAppender.MP4;
		} else if ( sFileExtName.equals( "CLASS" ) ) {
			iFileExtType = FileAppender.CLASS;
		} else if ( sFileExtName.equals( "JAR" ) ) {
			iFileExtType = FileAppender.JAR;
		} else if ( sFileExtName.equals( "C" ) ) {
			iFileExtType = FileAppender.C;
		} else if ( sFileExtName.equals( "CPP" ) ) {
			iFileExtType = FileAppender.CPP;
		} else if ( sFileExtName.equals( "GROOVY" ) ) {
			iFileExtType = FileAppender.GROOVY;
		} else if ( sFileExtName.equals( "JAVA" ) ) {
			iFileExtType = FileAppender.JAVA;
		} else if ( sFileExtName.equals( "MSG" ) ) {
			iFileExtType = FileAppender.MSG;
		} else if ( sFileExtName.equals( "EML" ) ) {
			iFileExtType = FileAppender.EML;
		} else if ( sFileExtName.equals( "PST" ) ) {
			iFileExtType = FileAppender.PST;
		} else if ( sFileExtName.equals( "MBOX" ) ) {
			iFileExtType = FileAppender.MBOX;
		} else if ( sFileExtName.equals( "DAT" ) ) {
			iFileExtType = FileAppender.DAT;
		} else if ( sFileExtName.equals( "TTF" ) ) {
			iFileExtType = FileAppender.TTF;
		} else if ( sFileExtName.equals( "AFM" ) ) {
			iFileExtType = FileAppender.AFM;
		} else if ( sFileExtName.equals( "HDF" ) ) {
			iFileExtType = FileAppender.HDF;
		} else if ( sFileExtName.equals( "DB3" ) ) {
			iFileExtType = FileAppender.DBTHREE;
		} else if ( sFileExtName.equals( "DB" ) ) {
			iFileExtType = FileAppender.DB;
		} else if ( sFileExtName.equals( "MDB" ) ) {
			iFileExtType = FileAppender.MDB;
		} else if ( sFileExtName.equals( "SQLITE3" ) ) {
			iFileExtType = FileAppender.SQLITETHREE;
		} else if ( sFileExtName.equals( "SQLITE" ) ) {
			iFileExtType = FileAppender.SQLITE;
		} else if ( sFileExtName.equals( "P7B" ) ) {
			iFileExtType = FileAppender.PSEVENB;
		} 
		
		else {
			iFileExtType = FileAppender.UNKNOWN;
		} 
		
		return iFileExtType;
	}	
	
}
package com.rayful.io.namo;

import com.rayful.localize.Localization;

import java.io.*;
import java.util.StringTokenizer;

public class NamoParser {
	/**
	* 주어진 경로의 Namo파일을 파싱하여 정보들을 NamoDocument인스턴스에 저장한 후 	
	* NamoDocument 인스턴스를 리턴
	* <p>
	* @param	sNamoFilePathName	Parsing할 Namo파일의 파일및 경로명
	* @return	NamoDocument 인스턴스
	*/			
	public static NamoDocument parse ( String sNamoFilePathName )
	throws NamoParserException
	{
		
		BufferedReader bin = null;
		String sFileLine;
		boolean bFileHeader = false;	// true임...
		boolean bFileBody = false;
		
		int iBodyCnt = 0; // 메인헤더가 읽혀졌는지 여부를 표시
		
		StringBuffer osbAttFileHeader = null;
		StringBuffer osbAttFileContent = null;
		
		NamoHeader oNamoHeader = null;
		NamoDocument oNamoDocument = new NamoDocument ( sNamoFilePathName );
		

		File oNamoFile = new File ( sNamoFilePathName );
		String sNamoMimeVersion = "MIME-Version:";
		String sNamoBoundary = "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
		
		try {	
			bin = new BufferedReader ( new FileReader( oNamoFile ) );
			
			osbAttFileHeader = new StringBuffer ();
			osbAttFileContent = new StringBuffer ();
			
			while ( ( sFileLine = bin.readLine() ) != null ) {

				/* ---------------------------------------------------------------------------*/
				/* MIME-Version: 경계 부분 찾기 */
				/* ---------------------------------------------------------------------------*/
				
				if ( sFileLine.trim().startsWith( sNamoMimeVersion ) ||
						sFileLine.trim().startsWith( "--" + sNamoBoundary ) ) {
				// 읽은 라인이 Namo 경계부분인 경우 ( MIME-Version: )
					
					// Header정보가 null이 아니면 첨부파일이라고 가정하고 
					// 이전에 읽혀진 정보( 첨부헤더 + 첨부본문 )을 나모문서에 추가한다.
					if ( oNamoHeader != null ) {
						// 첨부파일정보를 문서에 추가
						oNamoDocument.addAttachFile( oNamoHeader, osbAttFileContent.toString() );
						oNamoHeader = null;
					}

					
					bFileHeader = true;	// 다음 부터 읽혀질 부분은 헤더임을 표시
					bFileBody = false;	// 다음 부터 읽혀질 부분은 본문이 아님을 표시
					
					osbAttFileHeader = new StringBuffer ();	// 새로운 헤더를 담을 버퍼를 생성
					osbAttFileContent = new StringBuffer ();	// 새로운 본문을 담을 버퍼를 생성
					
					// MIME-Version: ... 정보를 추가 
					osbAttFileHeader.append( sFileLine ).append( " " );
					
				/* ---------------------------------------------------------------------------*/
				/* 파일Header 처리*/
				/* ---------------------------------------------------------------------------*/
				} else if ( bFileHeader ) {
					
					if ( sFileLine.length() > 0 ) {
						
						// 읽은 라인이 길이가 0보다 크면 헤더버퍼에 넣는다.
						osbAttFileHeader.append( sFileLine ).append( " " );
					}	else {
						
						// 읽은 라인이 길이가 0라몀 헤더정보가 다 읽혀젔으므로 
						// 헤더정보룰 파싱한다.
						oNamoHeader = parseNamoHeader ( osbAttFileHeader.toString() );
						
						if (  oNamoHeader.getBoundary()!= null ) {
							sNamoBoundary = oNamoHeader.getBoundary();
							
							oNamoHeader = null;		// 헤더정보는 무시한다.
							bFileHeader = false;	 
							bFileBody = false;
							
						} else {
							bFileHeader = false;	// 다음 부터 읽혀질 부분은 헤더가 아님을 표시
							bFileBody = true;		// 다음 부터 읽혀질 부분은 본문임을 표시		
							
							if ( oNamoHeader.getFileName() == null ) {
								// 본문이면 파일이름이 없으므로 겹치지 않게 세팅한다.
									iBodyCnt ++;
									oNamoHeader.setFileName ( "Namobody" + iBodyCnt + ".html" );
							}							
						}

					}
					
				/* ---------------------------------------------------------------------------*/
				/* 파일본문 처리*/
				/* ---------------------------------------------------------------------------*/
				} else if ( bFileBody ) {
					if ( sFileLine.length() > 0 ) {
						
						if ( oNamoHeader.getContentTransferEncodingCode() == NamoHeader.CTE_BASE_64 ) {
							osbAttFileContent.append( sFileLine );
				
						} else {
							//에러를 유발시킨다.
							throw new NamoParserException ( Localization.getMessage( NamoParser.class.getName() + ".Exception.0001" ) );
						}
					}
				} else {
					throw new NamoParserException ( Localization.getMessage( NamoParser.class.getName() + ".Exception.0002" ) );
				}
				
			}	// while Loop
			
			/* ---------------------------------------------------------------------------*/
			/* (MIME-Version: 관련 헤더 정보가 없음) 경계가 없는경우 처리 */
			/* ---------------------------------------------------------------------------*/
			if ( oNamoHeader != null ) {
				if ( osbAttFileContent.length() > 0 ) {
						// 첨부파일정보를 문서에 추가
						oNamoDocument.addAttachFile( oNamoHeader, osbAttFileContent.toString() );
						oNamoHeader = null;
				} else if ( osbAttFileContent.length() == 0) {
					//에러를 유발시킨다.
					throw new NamoParserException ( Localization.getMessage( NamoParser.class.getName() + ".Exception.0003" ) );
				}
			}
		} catch ( NamoParserException Namoe ) {
			throw Namoe;
		} catch ( Exception e ) {
			//에러를 전달한다.
			throw new NamoParserException( e.toString() );
		} finally {
			if ( bin!= null ) try {bin.close();} catch( IOException ioe){};
		}
		
		return oNamoDocument;
	}


	/**
	* 파일헤더정보를 Parsing하여 NamoHeader 인스턴스에 저장한 후
	* NamoHeader 인스턴스를 리턴
	* <p>
	* @param	sHeaderInfo	
	* @return	Namo파일의 헤더정보(NamoHeader 인스턴스)
	*/		
	static private NamoHeader parseNamoHeader( String sHeaderInfo ) 
	{
		NamoHeader oNamoHeader = new NamoHeader();
		
		// 구분문자 : 공백, 탭, ", ;
		StringTokenizer ost = new StringTokenizer( sHeaderInfo, " \t\";" );
		String sToken;
		String sFilePath, sFileName;
		int iPosition;
		
		while ( ost.hasMoreElements() ) {
			sToken = ost.nextToken();
			
			if ( sToken.equalsIgnoreCase( "MIME-Version:" ) ) {
				oNamoHeader.setMimeVersion( ost.nextToken() );
			} else if ( sToken.equalsIgnoreCase( "X-Generator:" ) ) {
				oNamoHeader.setXGenerator( ost.nextToken() + " " +  ost.nextToken() );
			} else if ( sToken.equalsIgnoreCase( "boundary=" ) ) {
				oNamoHeader.setBoundary(ost.nextToken() );				
			} else if ( sToken.equalsIgnoreCase( "Content-Type:" ) ) {
				oNamoHeader.setContentType ( ost.nextToken() );
			} else if ( sToken.equalsIgnoreCase( "charset=" ) ) {	
				oNamoHeader.setCharset( ost.nextToken() );	
			} else if ( sToken.equalsIgnoreCase( "Content-Transfer-Encoding:" ) ) {
				oNamoHeader.setContentTransferEncodingName ( ost.nextToken() );
			// 혹시 이 부분을 나모에서 사용할까???? - 확인요망
			} else if ( sToken.equalsIgnoreCase( "Content-ID:" ) ) {
				oNamoHeader.setContentId ( ost.nextToken() );
			} else if ( sToken.equalsIgnoreCase( "Content-Location:" ) || 
				sToken.equalsIgnoreCase( "name=" ) ) {
					
				// 파일명을 파싱하여 세팅
				sFilePath = ost.nextToken();
				sFileName = null;
				iPosition = sFilePath.lastIndexOf( File.separatorChar );
				if ( iPosition > -1 ) {
					sFileName = sFilePath.substring( iPosition + 1);
				} else {
					iPosition = sFilePath.lastIndexOf( "/" );
					if ( iPosition > -1 ) {
						sFileName = sFilePath.substring( iPosition + 1);
					} else {
						sFileName = sFilePath;
					}
				}
				oNamoHeader.setFileName ( sFileName );
			} 
		}
		return oNamoHeader;
	}
}

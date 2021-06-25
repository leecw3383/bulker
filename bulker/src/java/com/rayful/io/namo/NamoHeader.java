package com.rayful.io.namo;


public class NamoHeader 
{
	/** Namo 파일명 */
	private String ms_fileName;

	/** Namo Mime-version (1.0) */
	private String ms_mimeVersion;	
	/** Namo Content-Type명 (text/html) */
	private String ms_contentType;
	/** Namo charset 명 (euc-kr) */
	private String ms_charset;	
	/** Namo X-Generator ( NamoMIME 6.0.0.9 ) */
	private String ms_xGenerator;
	
	private String ms_boundary;
	private String ms_contentId;
	
	
	/** Namo 파일명 Content-Transfer-Encoding명 */
	private String ms_contentTransferEncodingName;
	/** Namo 파일명 Content-Transfer-Encoding 종류 */
	private int mi_contentTransferEncodingCode;	
	
	// Content Transfer Encoding 관련 상수
	/** Content-Transfer-Encoding : 알수없음 */
	public static final int CTE_NONE = 0;
	/** Content-Transfer-Encoding : base64 */
	public static final int CTE_BASE_64 = 1;

	
	/**
	* 생성자
	*/	
	public NamoHeader() {
		ms_fileName = null;
		ms_mimeVersion = null;	
		ms_contentType = null;
		ms_charset = null;
		ms_xGenerator= null;
		ms_boundary = null;
		ms_contentId = null;
		
		
		mi_contentTransferEncodingCode = 0;		
		ms_contentTransferEncodingName = "none";
	}
	
	/**
	* 생성자
	* <p>
	* @param	sFileName	파일명
	*/	
	public NamoHeader( String sFileName ) {
		ms_fileName = null;
		ms_mimeVersion = null;	
		ms_contentType = null;
		ms_charset = null;
		ms_xGenerator= null;
		
		mi_contentTransferEncodingCode = 0;		
		ms_contentTransferEncodingName = "none";			
	}	
	
	/**
	* 파일명을 설정한다.
	* <p>
	* @param	sFileName	파일명
	*/		
	public void setFileName ( String sFileName ){
		if ( sFileName != null ) {
			ms_fileName = sFileName;
		}
	}
	
	/**
	* Mime-Version을 설정한다
	* <p>
	* @param	sMimeVersion	Mime-Version
	*/		
	public void setMimeVersion ( String sMimeVersion  ) {
		if ( sMimeVersion != null ) {
			ms_mimeVersion = sMimeVersion;
		}
	}	
		
	
	/**
	* Content-Type명을 설정한다
	* <p>
	* @param	sContentType	Content-Type명
	*/		
	public void setContentType ( String sContentType  ) {
		if ( sContentType != null ) {
			ms_contentType = sContentType;
		}
	}	
		
	/**
	* Charset명을 (Character-Set)을 설정한다
	* <p>
	* @param	sContentEncoding	Charset명
	*/			
	public void setCharset ( String sCharset ){
		if ( sCharset != null ) {
			ms_charset  = sCharset;
		}
	}	
	
	/**
	 * X-Generator 정보를 설정한다.
	 * @param xGenerator	X-Generator 값
	 */
	public void setXGenerator ( String xGenerator ) {
		if ( xGenerator != null ) {
			ms_xGenerator = xGenerator;
		}		
	}
	
	/**
	* Content-Transfer-Encoding 명을 설정한다
	* <p>
	* @param	sContentTransferEncodingName	"base64"
	*/	
	public void setContentTransferEncodingName ( String sContentTransferEncodingName ){
		if ( sContentTransferEncodingName != null ) {
			if ( sContentTransferEncodingName.equalsIgnoreCase( "base64" ) ) {
				mi_contentTransferEncodingCode  = CTE_BASE_64;
				ms_contentTransferEncodingName = "base64";
			} else {
				mi_contentTransferEncodingCode  = CTE_NONE;
				ms_contentTransferEncodingName = "none";					
			}
		}
	}
	
	/**
	 * Boundary 정보를 설정한다.
	 * @param bounday	boudary 스트링
	 */
	public void setBoundary ( String boundary ) {
		if ( boundary != null ) {
			ms_boundary = boundary;
		}		
	}
	
	/**
	 * content-id 정보를 설정한다.
	 * @param contentId	content-id값
	 */
	public void setContentId ( String contentId ) {
		if ( contentId != null ) {
			ms_contentId = contentId;
		}		
	}	
	
	
	/**
	* 파일명을 알아낸다.
	* <p>
	* @return	Namo헤더정보가 설명하고 있는 파일의 파일명
	*/	
	public String getFileName () {
		return ms_fileName;
	}
	
	/**
	* Mime-Version 값을 알아낸다.
	* <p>
	 * @return	 Mime-Version 값
	 */
	public String getMimeVirsion () {
		return ms_mimeVersion;
	}		
	
	/**
	* Content-Type명을 알아낸다.
	* 예) text/html, image/gif ...
	* <p>
	* @return	Content-Type명
	*/		
	public String getContentType () {
		return ms_contentType;
	}	
	
	/**
	* Character-Set을 알아낸다.
	* 예) euc-kr, utf-8, ...
	* <p>
	* @return	Character-Set
	*/		
	public String getCharset () {
		return ms_charset;
	}
	
	/**
	* X-Generator값을  알아낸다.
	* 예) NamoMIME 6.0.0.9
	 * @return	X-Generator값
	 */
	public String getXGenerator () {
		return ms_xGenerator;
	}	
	
	/**
	*  boundary 스트링을 알아낸다.
	* <p>
	* @return	boundary
	*/		
	public String getBoundary () {
		return ms_boundary;
	}	
	
	/**
	* Content-id값 을 알아낸다.
	* <p>
	* @return	Content-Id값
	*/		
	public String getContentId () {
		return ms_contentId;
	}	
	
	/**
	* Content-Transfer-Encoding 상수값을 알아낸다.
	* 예) CTE_BASE_64
	* <p>
	* @return	Content-Transfer-Encoding 상수값
	*/		
	public int getContentTransferEncodingCode () {
		return mi_contentTransferEncodingCode;
	}
	
	/**
	* Content-Transfer-Encoding명을 알아낸다.
	* 예) base64
	* <p>
	* @return	Content-Transfer-Encoding명
	*/	
	public String getContentTransferEncodingName () {
		return ms_contentTransferEncodingName;
	}	
	
	
	/**
	* 객체의 멤버변수 내용을 하나의 String 만든다.
	* @return	객체의 멤버변수내용을 담고있는 String
	*/			
	public String toString() {
		StringBuffer oSb = new StringBuffer();
		oSb.append( "\nFileName:" + ms_fileName );
		oSb.append( "\nMIME-Version:" + ms_mimeVersion  );
		oSb.append( "\nContent-Type:" + ms_contentType  );
		oSb.append( "\nCharset:" + ms_charset  );
		oSb.append( "\nContentTransferEncodingCode:" + mi_contentTransferEncodingCode );
		oSb.append( "\nContentTransferEncodingName:" + ms_contentTransferEncodingName );			
		oSb.append( "\nX-Generator:" + ms_xGenerator );		

		return oSb.toString();
	}
}
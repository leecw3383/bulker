/**
 *******************************************************************
 * 파일명 : Attachemet.java
 * 파일설명 : 색인파일의 정보를 저장하는 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/27   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.io;

/**
* 색인파일의 정보를 저장하는 클래스 
*/
public class Attachment
{
	/** file_id */
	String ms_fileId;
	/** 원본저장파일경로 */
	String ms_sourceFilePath;
	/** 원본저장파일명(내부관리용 파일명) */
	String ms_sourceFileName;
	/** 실제파일명(화면에 출력되는 파일명) */
	String ms_originalFileName;
	/** 임시파일명 ( 경로포함) */
	String ms_tempFilePathName;
	/** 파일저장여부 */
	boolean mb_saved;
	
	/** 
	 * 첨부파일 여부 : 2006/12/20 정충열
	 * 색인대상파일이긴 하지만, 색인되는 첨부파일수, 첨부파일명에서는 제외하기 위해
	 * 구분할는 변수가 필요함 
	 * ex) 본문파일이 별도로 존재하는경우 (LG EDMS)
	 * */
	boolean mb_attachFile;
	
	/** 
	* 생성자
	*/	
	public Attachment() 
	{
		ms_fileId = null;
		ms_sourceFilePath = null;
		ms_sourceFileName = null;
		ms_originalFileName = null;
		ms_tempFilePathName = null;
		mb_saved = false;
		mb_attachFile = true;	
	}

	/** 
	* 생성자
	* <p>
	* @param	sFileId 파일ID
	* @param	sSourceFilePath 원본파일경로
	* @param	sSourceFileName 원본파일명 (실제저장된 파일명)
	* @param	sOriginalFileName Original 파일명 (화면에 보이는 파일명)
	* @param	sTempFilePathName 임시(Temp) 파일경로/파이명
	*/	
	public Attachment ( String sFileId,
						String sSourceFilePath, 
						String sSourceFileName, 
						String sOriginalFileName, 
						String sTempFilePathName )
	{
		ms_fileId = sFileId;
		ms_sourceFilePath = sSourceFilePath;
		ms_sourceFileName = sSourceFileName;
		ms_originalFileName = sOriginalFileName;
		ms_tempFilePathName = sTempFilePathName;
		mb_saved = false;
		mb_attachFile = true;
	}
	
	/** 
	* 생성자
	* Rdbms file_query에서 얻은 다운로드 목록을 Attachment객체 세팅할때 사용
	* <p>
	* @param	sParams	파일ID, 원본파일경로, 원본파일명, 저장할 파일명을 갖고있는 배열
	* sParams[0] : 파일ID
	* sParams[1] : 원본파일경로
	* sParams[2] : 원본파일명
	* sParams[3] : 저장할 파일명
	*/	
	public Attachment ( String [] sParams )
	{
		ms_fileId = sParams[0];
		ms_sourceFilePath = sParams[1];
		ms_sourceFileName = sParams[2];
		ms_originalFileName = sParams[3];
		ms_tempFilePathName = null;
		mb_saved = false;
		mb_attachFile = true;
	}
	
	/** 
	* 생성자
	* 본문에 파일내용이 포함된경우의  파일목록을 Attachment객체 세팅할때 사용
	* <p>
	* @param	sFileId 파일ID
	* @param	sOrginalFileName 저장할 파일명
	* @param	sTempFilePathName 임시(Temp) 파일경로/파일명	
	*/	
	public Attachment ( String sFileId,
						String sOrginalFileName,
						String sTempFilePathName )
	{
		ms_fileId = sFileId;
		ms_sourceFilePath = null;
		ms_sourceFileName = null;
		ms_originalFileName = sOrginalFileName;
		ms_tempFilePathName = sTempFilePathName;
		mb_saved = false;
		mb_attachFile = true;
	}
	

	/**
	*	파일정보의 파일이 저장되었는지 여부를 알아낸다.
	* <p>
	* @return	true:이미저장됨 / false:저장안됨
	*/
	public boolean isSaved()
	{
		return mb_saved;
	}
	
	/**
	*	다운로드가 필요한 파일정보인지 여부를 알아낸다.
	* <p>
	* @return	true:다운로드 필요 / false: 다운로드 필요없음
	*/	
	public boolean isRequireDownload() {
		if ( ms_sourceFileName != null ) {
			return true;
		} else {
			return false;
		}
	}
	
	
	/**
	* 원본파일의 Full 경로를 알아낸다.
	* @return 대상파일의 경로
	*/
	public String getFileId()
	{
		return ms_fileId;
	}
	
	/**
	* 원본파일의 Full 경로를 알아낸다.
	* @return 대상파일의 경로
	*/
	public String getSourceFilePath()
	{
		if ( mb_saved ) {
			return "";
		} else {
			return ms_sourceFilePath;
		}
	}
	
	/**
	* 원본파일의 파일명을 리턴 
	* @return 대상파일의 파일명
	*/
	public String getSourceFileName()
	{
		if ( mb_saved ) {
			return "";
		} else {
			return ms_sourceFileName;
		}
	}	
	
	/**
	* 저장될 파일명을 알아낸다.
	* @return 저장될 파일명 ( 파일ID + '_' + 실제파일명 )
	*/	
	public String getTargetFileName()
	{
		String sSaveFileName;
		//if ( ms_fileId.length() > 0  ) {
			sSaveFileName = ms_fileId + "." + com.rayful.bulk.io.FileAppender.getFileExtentionName( ms_originalFileName ) ;
		//} else {
		//	sSaveFileName = ms_originalFileName;
		//}
		
		return sSaveFileName;
	}
	
	/**
	* 실제(Original) 파일명을 알아낸다.
	* @return 원본파일명
	*/	
	public String getOriginalFileName()
	{
		return ms_originalFileName;
	}	
	
	/**
	* 임시(Temp) 파일명을 알아낸다.
	* @return 임시파일명
	*/	
	public String getTempFilePathName()
	{
		return ms_tempFilePathName;
	}
	
	/**
	* 첨부파일인지 여부를 리턴
	* 2006/12/20 정추열 추가
	* <p>
	* @return	true:첨부파일 / false: 첨부파일아님
	*/
	public boolean isAttachFile()
	{
		return mb_attachFile;
	}
	
	
	/**
	*	파일정보의 파일이 저장되었는지 여부를 설정한다.
	* <p>
	* @param	bSaved	true:이미 저장됨 / false: 저장안됨
	*/
	public void setSaved( boolean bSaved )
	{
		mb_saved = bSaved;
	}	
		
	
	/**
	* 실제(Original) 파일명을 세팅한다.
	* @param sFileName	원본파일명
	*/	
	public void setOriginalFileName( String sFileName )
	{
		ms_originalFileName = sFileName;
	}	
	
	
	/**
	* 첨부파일인지, 아닌지를 설정한다.
	* 2006/12/20 정추열 추가
	* 첨부파일이 아니라고 설정하면 색인시 첨부파일수, 첨부파일명에서 제외된다.
	* <p>
	* @param	bAttachFile	true:첨부파일/ false: 첨부파일아님
	*/
	public void setIsAttachFile( boolean bAttachFile )
	{
		mb_attachFile = bAttachFile;
	}
	
}
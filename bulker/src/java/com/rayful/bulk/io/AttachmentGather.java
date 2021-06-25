/**
 *******************************************************************
 * 파일명 : AttachmentGather.java
 * 파일설명 : 첨부파일들을 다운로드 받아 하나로 취합하는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/16   정충열    최초작성           
 * 2006/04/19   정충열    로직변경
 * 2006/12/24	정충열	 gatter -> gather
 *******************************************************************
*/
package com.rayful.bulk.io;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;

import org.apache.log4j.Logger;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.localize.Localization;

/**
 * 첨부파일들이 romote 서버에 있을 경우 다운로드는다. <p>
 */
public class AttachmentGather
{
	/**
	 * 파일접근 객체
	 * @uml.property  name="mo_filesAccessor"
	 * @uml.associationEnd  
	 */
	FilesAccessor mo_filesAccessor;	
	
	/** logger 정의 */ 
	static Logger logger = RayfulLogger.getLogger( AttachmentGather.class.getName(), Config.LOG_PATH );
	
	/**
	*	생성자
	*/
	public AttachmentGather ( FilesAccessor oFilesAccessor )
	{
		mo_filesAccessor = oFilesAccessor;
	}

	
	/**
	*	첨부파일들을 하나의 텍스트파일로 묶는다.
	* @param	oAttachments	첨부파일들의 목록
	*/	
	public int gather( Attachments oAttachments ) 
	{
		return this.gather ( oAttachments, Config.DOWNLOAD_PATH );
	}
		
	/**
	*	첨부파일들을 하나의 텍스트파일로 묶는다.
	* @param	oAttachments	첨부파일들의 목록
	* @param	sDownloadPath	다운로드 파일들의 Base경로
	* @return	정상처리된수 리턴
	*/	
	public int gather( Attachments oAttachments, String sDownloadPath ) 
	{
		File oDownloadedFile = null;
		int iFileExtType = 0;
		Attachment oAttachment = null;
		int iSuccessCnt = 0;
	
		for (int i=0; i< oAttachments.getAttachmentCount(); i++) {
			// 첨부파일정보객체를 얻는다.
			oAttachment = oAttachments.getAttachment(i);
			// 저장할 파일의 경로및 이름 
			oDownloadedFile = new File ( sDownloadPath,  oAttachment.getTargetFileName());
			// 다운로드할 파일의 확장자 Type
			iFileExtType = com.rayful.bulk.io.FileAppender.getFileExtentionType( oDownloadedFile.getPath());
			
			
			if ( iFileExtType == com.rayful.bulk.io.FileAppender.UNKNOWN ) {
				// -------------------------------------------------------
				// 색인대상파일이 아닌경우
				// -------------------------------------------------------				
				logger.warn ( Localization.getMessage( AttachmentGather.class.getName() + ".Logger.0001", oAttachment.getSourceFileName() ) );	
				//[주의]실패가 아님 - iSuccessCnt를 증가...
				iSuccessCnt ++;		
				
				// DB에 파일자체가 있는경우 파일이 이미 색인서버에 있으므로 삭제시도한다.
				if ( oDownloadedFile.exists() ) {
					oDownloadedFile.delete();
				}
				
				continue;	
			} else {
				// -------------------------------------------------------
				// 첨부파일이 아직 원격지에 존재할경우  다운로드.
				// -------------------------------------------------------
				if ( oAttachment.isRequireDownload() ) {
			
					logger.info ( Localization.getMessage( AttachmentGather.class.getName() + ".Logger.0002", oDownloadedFile.getPath() ) );
					try {
						mo_filesAccessor.downloadFile ( oAttachment.getSourceFilePath(),
														oAttachment.getSourceFileName(),
														oDownloadedFile.getPath() );
					} catch ( UnsupportedEncodingException uee ) {	
						logger.warn ( Localization.getMessage( AttachmentGather.class.getName() + ".Logger.0003" ), uee );
						continue;						
					} catch ( MalformedURLException mue ) {
						logger.warn ( Localization.getMessage( AttachmentGather.class.getName() + ".Logger.0004" ), mue );
						continue;
					} catch ( IOException ioe ) {
						logger.warn ( Localization.getMessage( AttachmentGather.class.getName() + ".Logger.0004" ), ioe );
						continue;
					}
				}

			} // iFileExtType != com.yess.ss.index.io.FileAppender.UNKNOWN 
		} // for
		
		return iSuccessCnt;
	} // function

} // class
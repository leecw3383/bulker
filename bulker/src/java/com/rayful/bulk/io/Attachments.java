/**
 *******************************************************************
 * 파일명 : Attachemets.java
 * 파일설명 : 색인파일의 목록를 정의한 클래스를 정의
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/27   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.io;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

/**
* 색인파일의 목록를 정의한 클래스
* ArrayList를 상속받아 재정의하여 구현되었습니다.
*/
@SuppressWarnings({ "serial", "unchecked" })
public class Attachments extends ArrayList
{
	/** 
	* 생성자
	*/	
	public Attachments ()
	{
		super();
	}
	
	/** 
	* 첨부파일의 갯수를 알아낸다.
	* Attachment객체의 수를 모두 리턴
	* @return	첨부파일 갯수
	*/		
	public int getAttachmentCount() 
	{
		return this.size();
	}
	
	/** 
	* 첨부파일의 갯수를 알아낸다.
	* Attachment객체중 첨부파일인것의 갯수만 리턴
	* @return	첨부파일 갯수
	*/		
	public int getIndexAttachmentCount() 
	{
		int iAttCnt=0;
		Attachment oAttachment = null;
		for ( int i=0; i< this.size(); i++) {
			
			oAttachment = (Attachment)this.get(i);
			if ( oAttachment.isAttachFile() ) {
			// 첨부파일이면  isAttachFile() == true...
				iAttCnt ++;
			}
		}
		return iAttCnt;
	}	
	
	/** 
	* 하나의 첨부파일객체를 목록에 추가한다.
	* @param	oAttachment	첨부파일정보객체
	* @return	성공여부
	*/		
	public boolean addAttachment( Attachment oAttachment )
	{
		return this.add( oAttachment );
	}

	/** 
	* 하나의 첨부파일객체를 목록에 추가한다.
	* @param	sAttachment	구분자(:)로 구분된 첨부파일정보스트링
	* @return	성공여부
	*/		
	public boolean addAttachment( String sAttachment )
	{
		if ( sAttachment != null ) {
			String [] aAttachment = sAttachment.split(":");
			
			if ( aAttachment.length == 4 ) {
				if ( aAttachment[3].length() > 0 ) {
								
					Attachment oAttachment = new Attachment ( aAttachment );
					return this.add( oAttachment );
				}
			
			}
		}
		return true;
	}	
	
	
	/** 
	* 첨부파일들을 한번에 여러개 추가한다.
	* @param	sAttachments	구분자(|)로 구분된 첨부파일목록정보
	* @return	성공여부
	*/		
	public boolean addAttachments( String sAttachments )
	{
		boolean bReturn = true;
		
		if ( sAttachments != null ) {		
			String [] aAttachments = sAttachments.split("\\|");
			for ( int i=0; i< aAttachments.length ; i++ ) {
				if ( this.addAttachment( aAttachments[i] ) == false ) {
					bReturn = false;
					break;
				}
			}
		}
		return bReturn;
	}	
	
	/** 
	* 목록의 특정위치의 첨부파일객체를 리턴한다.
	* <p>
	* @param	i	목록상의 위치
	* @return	Attachment객체
	*/
	public Attachment getAttachment( int i) 
	{
		Object obj = this.get(i);
		if ( obj != null ) {
			return ( Attachment ) obj;
		} else {
			return null; 
		}
	}

	/** 
	* 첨부파일의 목록을 리턴한다.
	* 내부적으로 getAttachmentNames("|")로 호출
	* @return 구분자("|")로 구분하는 파일명 스트링을 리턴.	
	*/
	public String getAttachmentNames() 
	{
		return this.getAttachmentNames( "|" );
	}
	
	/** 
	* 첨부파일의 목록을 리턴한다.
	* @param	sDelimiter	파일명을 구분하는 구분자
	* @return Parameter로 주어진 구분자로 구분하는 파일명 스트링을 리턴.
	*/
	public String getAttachmentNames( String sDelimiter ) 
	{
		StringBuffer oSb = new StringBuffer();
		int iListCnt = this.size();
		Attachment oAttachment = null;
		int iAttCnt = 0;
			
		for ( int i=0; i< iListCnt; i++ ) {
			oAttachment = (Attachment)this.get(i);
			if ( oAttachment.isAttachFile()) {
			//첨부파일일 경우 isAttachFile() == true ...
				iAttCnt ++;
				
				if ( iAttCnt != 1 ) {
					oSb.append( sDelimiter );
				}
				oSb.append( oAttachment.getOriginalFileName());
			}
		}
		
		return oSb.toString();
	}	
	

	/** 
	* Orignial 첨부파일명에 null값이 있는 첫번째 Attachment를 알아낸다.
	* return Attachment객체 , 그런객체가 없다면 null을 리턴
	*/
	public Attachment getFirstNotValidAttachment () 
	{
		int iListCnt = this.size();
		
		for ( int i=0; i< iListCnt; i++ ) {
			if ( this.getAttachment(i).getOriginalFileName() == null ) {
				return  this.getAttachment(i);
			}
		}
		return null;
	}
	
	/** 
	* Orignial 첨부파일명에 null값이 있는 Attachment가 있는지 체크
	* return true : null값없음/ false : null값있음
	*/
	public boolean isValid () 
	{
		int iListCnt = this.size();
		boolean  bReturn = true;
		
		for ( int i=0; i< iListCnt; i++ ) {
			if ( this.getAttachment(i).getOriginalFileName() == null ) {
				bReturn = false;
				break;
			}
		}
		return bReturn;
	}	
	

	// 파일 읽어드리는 함수
	public byte[] getFileBinary(String filepath) {
		
		File file = new File(filepath);
		byte[] data = new byte[(int) file.length()];
		
		try (FileInputStream stream = new FileInputStream(file)) {
			stream.read(data, 0, data.length);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	
		return data;
	}	
	
}
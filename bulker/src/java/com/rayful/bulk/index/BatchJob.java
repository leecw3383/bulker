/**
 *******************************************************************
 * 파일명 : BatchJob.java
 * 파일설명 : 색인프로그램에서 실행해야할 Batchjob을 기술
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/06/27   정충열    최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index;

import java.io.*;

import com.rayful.bulk.util.StreamGobbler;

/**
* 색인프로그램에서 실행해야할 Batchjob을 기술
*/
public class BatchJob
{
	/** BatchJob Name **/
	String ms_jobName;
	/** 실행명령어 **/
	String ms_jobCommand;
	/** 에러발생시 처리 **/
	int mi_errorProcessType;
	/** DF Reload 필요요부 **/
	boolean mb_dfReload;	
	
	/** 알수없음 **/
	public static final int ERRORPROCESS_TYPE_UNKNOWN= 0;
	/** Batchjob 실행시 에러가 발생해도 색인프로그램을 수행한다. **/
	public static final int ERRORPROCESS_TYPE_CONTINUE = 0;
	/** Batchjob 실행시 에러가 발생하면 색인프로그램을 종료한다. **/
	public static final int ERRORPROCESS_TYPE_STOP = 1;
	
	
	/** 
	* 생성자
	* 
	* <p>
	* @param	sJobName Batch작업명
	* @param	sJobCommand Batch작업 실행 명령어
	* @param	iErrorProcessType 에러발생시 처리방식
	* @param	bDfReload 에러발생시 처리방식	
	*/	
	public BatchJob ( String sJobName,
											String sJobCommand, 
											int iErrorProcessType,
											boolean bDfReload )
	{
		ms_jobName = sJobName;
		ms_jobCommand = sJobCommand;
		mi_errorProcessType = iErrorProcessType;
		mb_dfReload = bDfReload;
	}

	/** 
	* Batch작업명을 알아낸다.
	* <p>
	* @return	Batch작업명
	*/	
	public String  getJobName ()
	{
		return ms_jobName;
	}
	
	/** 
	*  Batch작업 실행 명령어를 알아낸다.
	* <p>
	* @return	 Batch작업 실행 명령어를 배열로 리턴.
	*/	
	public String [] getJobCommand ()
	{
		return ms_jobCommand.split( " " );
	}	
	
	/** 
	*  Batch작업중 에러발생시 처리방식을 리턴
	* <p>
	* @return	 Batch작업 실행 명령어를 배열로 리턴.
	*/	
	public int getErropProcess ()
	{
		return mi_errorProcessType;
	}
	
	/** 
	*  BatchJob 수행후 DF Reload 여부를 알아낸다.
	* <p>
	* @return	 df reload여부
	*/	
	public boolean isDfReload ()
	{
		return mb_dfReload;
	}
	
	/** 
	*  Batch작업을 수행한다.
	* <p>
	* @return	 1:비정상종료/0:정상종료
	*/		
	public int execute() {
		int iReturn = 0;
		String [] aCommand = this.getJobCommand();
		
		if ( aCommand != null ) {
			try {
				Process oProcess = Runtime.getRuntime().exec( aCommand );

		       	// any error message?
		        StreamGobbler errorGobbler = new StreamGobbler(oProcess.getErrorStream(), "ERROR");            
		        
		        // any output?
		        StreamGobbler outputGobbler = new StreamGobbler(oProcess.getInputStream(), "OUTPUT");
		        
		        // any output?
		        //StreamGobbler outputGobbler = new StreamGobbler(oProcess.getOutputStream());	            
		    
						// kick them off
		        errorGobbler.start();
		        outputGobbler.start();
        
				iReturn = oProcess.waitFor();
				
			} catch ( IOException ie ) {
				System.out.println ( ie.toString() );
				iReturn = 1;
			} catch ( InterruptedException  ire ) {
				System.out.println ( ire.toString() );
				iReturn = 1;
			}
		}
		return iReturn;
	}

}
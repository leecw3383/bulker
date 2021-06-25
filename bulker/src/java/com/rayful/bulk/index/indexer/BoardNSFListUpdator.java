/**
 *******************************************************************
 * 파일명 : BoardNSFListUpdator.java
 * 파일설명 : 동적으로 생성되는 게시판의 대상 DB를 TB_NSFLIST 테이블에 업데이트 하기 위한 모듈
 *           동적으로 관린되는 게시판 NSF를 관리하고 있는 Notes NSF : emate_app/bbsmng.nsf
 *******************************************************************
 * 작성일자		작성자	내용
 * -----------------------------------------------------------------
 * 2011/01/01	주현필	최초작성             
 *******************************************************************
*/
package com.rayful.bulk.index.indexer;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import lotus.domino.Database;
import lotus.domino.Document;
import lotus.domino.DocumentCollection;
import lotus.domino.NotesException;
import lotus.domino.NotesFactory;
import lotus.domino.Session;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.sql.RdbmsConnector;

/**
 * 동적 게시판 대상 DB를 TB_NSFLIST에 업데이트 하는 클래스
 * <p>
 * main()함수가 정의됨
 */
public class BoardNSFListUpdator
{
	
	public static void main  ( String [] argv ) 
	{
				
		// 변수선언 및 초기화
		// 노츠 Session 변수
		lotus.domino.Session oSession = null;
		// META_DB(색인 및 Notes 정보를 관리하기 위한 DB) Connector 변수
		RdbmsConnector oConnector = null;
		// RDBMS Connection 변수
		Connection mo_connection = null;
		
		try {
			
			//=============================================================================
			// properites 파일로 부터 색인에 필요한 설정정보를 알아내어 Config에 세팅.
			//=============================================================================	 
			String sPropFileName = "Indexer.properties" ;

			ClassLoader cl;
			cl = Thread.currentThread().getContextClassLoader();
			if( cl == null )
				cl = ClassLoader.getSystemClassLoader();                

			java.net.URL oUrl = cl.getResource( sPropFileName );

			if ( oUrl == null ) {
				throw new FileNotFoundException("Could not found Property File(Indexer.properties)");
			}
		  	
			sPropFileName = oUrl.getPath();
			Config.load( sPropFileName, "BoardNSFList" );
			
			// 메타 DB 접속 정보는 Indexer.properties 파일에 정의되어 있음
			oConnector = new RdbmsConnector( 
					Config.METADB_DRIVERCLASS,
					Config.METADB_URL,
					Config.METADB_USER,
					Config.METADB_PASS );
			
			mo_connection = oConnector.getConnection();
			
			System.out.println("Notes Server : " + Config.NOTES_SERVER);
			// Session을 맺는다.
			oSession= NotesFactory.createSession( Config.NOTES_SERVER, Config.NOTES_USER, Config.NOTES_PASS );
			
			if ( oSession == null ) {
				throw new Exception ( "Session is null");
			}
			
			// Notes에서 정보를 읽어들여 TB_NSFLIST 에 정보를 기입하거나 수정한다.
			getBoardNSFList(mo_connection, oSession);
			
		} catch ( Exception e ) { 
			System.out.println ( e.getMessage() );
			e.printStackTrace();
		}	finally {
			if ( oSession != null ) { try { oSession.recycle(); } catch( Exception e ) {} }
			
			if ( mo_connection != null ) { try { mo_connection.close(); } catch( Exception e ) {} }
		}
		
		System.exit(1);
		
	}
	
	/**
	 * Notes에서 정보를 읽어들여 TB_NSFLIST에 정보를 갱신한다.
	 * <p>
	 * @param mo_connection
	 * @param oSession
	 * @throws NotesException
	 * @throws Exception
	 */
	private static void getBoardNSFList(Connection mo_connection, Session oSession)
	throws NotesException, Exception
	{
		Database oDatabase = null;
		DocumentCollection oDocumentCollection = null;
		Document oDocument = null;
		
		String sNSFName = "emate_app/bbsmng.nsf";
		int iMaxRows = 0;
		
		boolean bDelProcess = true;
		
		// Database를 가져온다.
		oDatabase = oSession.getDatabase( "", sNSFName );
		if ( oDatabase == null ) {
			throw new Exception ( "oDatabase is null");
		}
		
 		if ( oDatabase.isOpen() == false ) {
			throw new Exception ( "Could not open Database Object");
		}
 		
 		oDocumentCollection = oDatabase.search( "SELECT Status=\"Active\"", null, 0 );
		
		System.out.println ( "Document Collection count : " + oDocumentCollection.getCount() );
		iMaxRows = oDocumentCollection.getCount();
		
		for (int iNext = 1; iNext <= iMaxRows; iNext++ ) {
			System.out.println ( "=========================================================================");
			System.out.println ( "iNext=" + iNext  );
			
			//oDocument = oDocumentCollection.getNthDocument(iNext);
			
			if ( oDocument != null ) {
				recycle( oDocument );
			}
			oDocument = getNthDocument(oDocumentCollection,iNext);
			
			if (oDocument == null ) {
				//throw new Exception ( iNext + ". 노츠 Document객체가 null입니다." );
				System.out.println( iNext + "  Document is null : document looping close." );
				break;
			}
			
			if ( getIsNotesItem( oDocument, "bbsDbPath" ) ) {
	      		sNSFName = getNotesText( oDocument, "bbsDbPath" );
			}
			
			insertBoardNSFList(mo_connection, sNSFName, bDelProcess );
			
			if (bDelProcess) bDelProcess = false;
			
		}
		
	}

	private static void insertBoardNSFList(Connection mo_connection, String sNSFName, boolean bDelProcess)
	throws SQLException, InterruptedException
	{
		String sSelectSql = null, sDelFlagSql = null;
		
		int iCount = 0;
		
		Statement oStmt = null;
		
		StringBuffer sbSql = new StringBuffer();
		
		/* Board 정보를 등록하거나 수정하기 전에 DEL_FLAG 처리를 한다. */
		/* board 정보를 등록하거나 수정할 때 DEL_FLAG를 N으로 변경한다. */
		/* DEL_FLAG = 'Y' 처리는 Board 정보를 변경할 때 한 번만 수행하게 한다. */
		if (bDelProcess) {
			sDelFlagSql = "UPDATE TB_NSFLIST SET DEL_FLAG = 'Y' WHERE CATEGORYTOP = 'collaboration' AND CATEGORYMID = 'board'";
		
			oStmt = mo_connection.createStatement();
			
			if (oStmt != null) {
				oStmt.execute(sDelFlagSql);
				
				oStmt.close();
			}
		}
		
		sSelectSql = "  SELECT COUNT(NSF) AS CNT FROM TB_NSFLIST ";
		sSelectSql += "  WHERE CATEGORYTOP = 'collaboration' AND CATEGORYMID = 'board' AND NSF = ?";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sNSFName);
		
		ResultSet oResult = oPstmt.executeQuery();
		
		if ( oResult != null ) {
			if ( oResult.next() == false ) {
				oResult.close();
				return;
			}
				
			iCount = oResult.getInt("CNT");
			
			oResult.close();
		
			// Statement 생성
			oStmt = mo_connection.createStatement();
			
			if (iCount > 0 ) {
				sbSql.append("UPDATE TB_NSFLIST SET ")
					.append(" DEL_FLAG = 'N' ")
					.append(" WHERE CATEGORYTOP = 'collaboration' AND CATEGORYMID = 'board' AND NSF = N'")
					.append(sNSFName)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_NSFLIST(NSF, CATEGORYTOP, CATEGORYMID, DEL_FLAG) VALUES(N'")
					.append(sNSFName).append("', 'collaboration', 'board', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
		
	}
	  
	private static void recycle( Document pDoc )
	{
		try {
			pDoc.recycle();
		} catch( NotesException e ) {
			e.printStackTrace();
		}
	}
	
	private static Document getNthDocument( DocumentCollection pDc, 
				int pIndex )
	{
		try {
			if ( pDc == null ) {
				return null;
			}
			else {
				return pDc.getNthDocument( pIndex );
			}
		} catch( NotesException e ) {
			System.out.println( "Dc.getNthDocument : " + e.id + " " + e.text );
			e.printStackTrace();
			return null;
		}
	}
	
	private static boolean getIsNotesItem( Document pDoc, 
				String pItemStr ) 
	{

		try {
			if ( pDoc == null ) {
				return false;
			}
			else {
				if ( pDoc.hasItem( pItemStr ) ) {
					return true;
				}
				else {
					return false;
				}
			}
		} catch( NotesException e ) {
			System.out.println( e.id + " " + e.text );
			e.printStackTrace();
			return false;
		}

	}
	
	private static String getNotesText( Document pDoc, 
				String pItemStr ) 
	{

		String sNotesText = "";

		try {
			if ( pDoc == null ) {
				return "";
			}
			else {
				sNotesText = pDoc.getFirstItem( pItemStr ).getText();

				return sNotesText.trim(); 
			}	    
		} catch( NotesException e ) {
			System.out.println( e.id + " " + e.text );      
			e.printStackTrace();

			return "";
		}
	}
	
//	// never used locally
//	private static void recycle( Session pSess )
//	{ 	
//		try {
//			pSess.recycle();
//		} catch( NotesException e ) {
//			e.printStackTrace();  		
//		}
//	}
//	
//	// never used locally
//	private static void recycle( Database pDb )
//	{
//		try {
//			pDb.recycle();
//		} catch( NotesException e ) {  		
//			e.printStackTrace();
//		}
//	}
//
//	// never used locally
//	private static void recycle( View pView )
//	{
//		try {
//			pView.recycle();
//		} catch( NotesException e ) {
//			e.printStackTrace();
//		}
//	}
//	
//	// never used locally
//	private static void recycle( DocumentCollection pDc )
//	{
//		try {
//			pDc.recycle();
//		} catch(NotesException e) {
//			e.printStackTrace();
//		}
//	}
	
}

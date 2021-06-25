package com.rayful.bulk.index.indexer;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import lotus.domino.ACL;
import lotus.domino.ACLEntry;
import lotus.domino.Database;
import lotus.domino.Document;
import lotus.domino.DocumentCollection;
import lotus.domino.NotesException;
import lotus.domino.NotesFactory;
import lotus.domino.Session;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.sql.RdbmsConnector;

public class NotesDBACL
{
	
	public static void main  ( String [] argv ) 
	{
		
		// 변수선언 및 초기화
		lotus.domino.Session oSession = null;
		
		RdbmsConnector oConnector = null;
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
			Config.load( sPropFileName, "NotesDBACL" );
			
			// Config에서 읽어오도록 수정할 것
			oConnector = new RdbmsConnector( 
					Config.METADB_DRIVERCLASS,
					Config.METADB_URL,
					Config.METADB_USER,
					Config.METADB_PASS );
			
			mo_connection = oConnector.getConnection();
			
			// Session을 맺는다.
			oSession= NotesFactory.createSession( Config.NOTES_SERVER, Config.NOTES_USER, Config.NOTES_PASS );
			
			if ( oSession == null ) {
				throw new Exception ( "Session is null");
			}
			
			// 자세한 설명은 사용자별 역할 및 그룹정보 설정 테이블 설명.pptx 참조한다.
			// names.nsf에서 사용자 / 그룹 정보를 가지고 온다.
			getNotesUserGroup(mo_connection, oSession);
			
			//1.tb_groupinfo
			//2.tb_usremailnsf
			//3.tb_nsflist : 다른 모듈에서 삭제로직 만들어야 함 > DelIndexUpdatorNotes에서 삭제 처리함
			deleteFlagYes(mo_connection, "GROUPINFO");
			
			// TB_NSFLIST에 있는 NotesDB에서 ACL를 가지고 온다.
			getNotesDBACL(mo_connection, oSession);
			
			//4.tb_nsfrrole
			deleteFlagYes(mo_connection, "NSFROLE");
			
			//4-1.tb_nsfrole ( board Authority )
			getBBSNotesDBACL(mo_connection, oSession);
			
			//4.tb_nsfrrole
			deleteFlagYes(mo_connection, "NSFROLE");
			
			// TB_GROUPINFO에서 정보를 분류하여 TB_USERGROUP 테이블을 설정한다.
			setNotesUserGroup(mo_connection);
			
			//5.tb_usergroup
			deleteFlagYes(mo_connection, "USERGROUP");
			
			// TB_USERROLE 정보를 설정한다.
			setUserRole(mo_connection);
			
			//6.tb_userrole
			deleteFlagYes(mo_connection, "USERROLE");
			
		} catch ( Exception e ) { 
			System.out.println ( e.getMessage() );
			e.printStackTrace();
		}	finally {
			//if ( oDocument != null ) { try { oDocument.recycle(); } catch( Exception e ) {} }
			//if ( oDocumentCollection != null ) { try { oDocumentCollection.recycle(); } catch( Exception e ) {} }
			//if ( oDatabase != null ) { try { oDatabase.recycle(); } catch( Exception e ) {} }
			if ( oSession != null ) { try { oSession.recycle(); oSession = null;} catch( Exception e ) {} }
			
			//if ( oRset != null ) { try { oRset.close(); } catch( Exception e ) {} }
			//if ( oPstmt != null ) { try { oPstmt.close(); } catch( Exception e ) {} }
			if ( mo_connection != null ) { try { mo_connection.close(); } catch( Exception e ) {} }
		}
		
		System.exit(1);
		
	}
	
	/**
	 * 동적으로 관리되는 게시판의 DB 권한을 체크하기 위한
	 * emate_app/bbsmng.nsf 에서 관리되는 NSF의 권한 정보를 가지고 온다.<br>
	 * getNotesDBACL처럼 NSF의 권한 정보를 처리하지만 권한에 대한 처리가 다르므로 가져오는 정보도 다르다.<br>
	 * Members, Roles, User Type, Level 정보를 가져온다.
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @param oSession Notes Session
	 * @throws NotesException
	 * @throws Exception
	 */
	private static void getBBSNotesDBACL(Connection mo_connection, Session oSession)
	throws NotesException, Exception
	{
		Database oDatabase = null;
		DocumentCollection oDocumentCollection = null;
		Document oDocument = null;
		
		// 동적으로 생성되는 게시물을 관리하는 DB Path
		// 여기에 존재하지 않으면 삭제된 DB이므로 색인에서도 삭제를 해야 한다.
		String sNSFName = "emate_app/bbsmng.nsf";
		String sRoles = "";
		int iMaxRows = 0, iUserType=3;
		boolean bDelProcess = true;
		
		String sUserTarget = null, sDBPath = null;
		
		String sMembers = null, sStatus = null;
				
		// Database를 가져온다.
		oDatabase = oSession.getDatabase( "", sNSFName );
		if ( oDatabase == null ) {
			throw new Exception ( "oDatabase is null");
		}
		
 		if ( oDatabase.isOpen() == false ) {
			throw new Exception ( "Could not open Database");
		}
 		
 		oDocumentCollection = oDatabase.search( "SELECT @ALL", null, 0 );
		
		System.out.println ( "Document Collection count : " + oDocumentCollection.getCount() );
		iMaxRows = oDocumentCollection.getCount();
		
		for (int iNext = 1; iNext <= iMaxRows; iNext++ ) {
			System.out.println ( "=========================================================================");
			System.out.println ( "iNext=" + iNext  );
			sMembers = "";
			
			if ( oDocument != null ) {
				recycle( oDocument );
			}
			oDocument = getNthDocument(oDocumentCollection,iNext);
			
			if (oDocument == null ) {
				System.out.println( iNext + " Document is null : document looping close." );
				break;
			}
				
			sUserTarget = "";
			
			// UserTarget 
			// G : Group / P : User / M : administrator
			// 그룹인 경우에는 처리하지 않는다.
			if ( getIsNotesItem( oDocument, "UserTarget" ) ) {
				sUserTarget = getNotesText( oDocument, "UserTarget" );
				if ( "G".equalsIgnoreCase(sUserTarget) ) {
					continue;
				} else if ( "P".equalsIgnoreCase(sUserTarget) ) {
					sRoles = "users";
					iUserType = 5;
				} else if ( "M".equalsIgnoreCase(sUserTarget) ) {
					sRoles = "sysadmin;appadmin";
					iUserType = 6;
				}
			}
			
			if ( getIsNotesItem( oDocument, "bbsDbPath" ) ) {
				sDBPath = getNotesText( oDocument, "bbsDbPath" );
				if ( sDBPath == null || sDBPath.trim().length() < 1 ) continue;
			}
			
			if ( getIsNotesItem( oDocument, "Status" ) ) {
				sStatus = getNotesText( oDocument, "Status" );
			}
			
			if ( getIsNotesItem( oDocument, "Member" ) ) {
				sMembers = getNotesText( oDocument, "Member" );
				
				System.out.println("GroupName : " + sMembers);
	      		
	      		String[] aGroupName = sMembers.split(";");
	      		
	      		bDelProcess = true;
	      		
	      		for(int iLoop=0; iLoop<aGroupName.length; iLoop++) {
	      			
	      			insertBBSNSFROLE(mo_connection, sDBPath, aGroupName[iLoop], sRoles, iUserType, sStatus, bDelProcess);
	      			
	      			if (bDelProcess) bDelProcess = false;
	      		}
	      		
			}
				
		}
		
		if ( oDocument != null ) { try { oDocument.recycle(); oDocument = null; } catch( Exception e ) {} }
		if ( oDocumentCollection != null ) { try { oDocumentCollection.recycle(); oDocumentCollection = null; } catch( Exception e ) {} }
		if ( oDatabase != null ) { try { oDatabase.recycle(); oDatabase = null; } catch( Exception e ) {} }
		
	}
	
	/**
	 * names.nsf에서 사용자 / 그룹 정보를 가지고 온다.<br>
	 * 그룹인 경우에는 TB_GROUPINFO 테이블에 정보를 갱신하고 사용자인 경우에는 TB_USERMAILNSF 테이블에 정보를 갱신한다.<br>
	 * 그리고, 사용자인 경우에는 TB_NSFLIST 테이블에 정보를 갱신한다.
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @param oSession Notes Session
	 * @throws NotesException
	 * @throws Exception
	 */
	private static void getNotesUserGroup(Connection mo_connection, Session oSession)
	throws NotesException, Exception
	{
		Database oDatabase = null;
		DocumentCollection oDocumentCollection = null;
		Document oDocument = null;
		
		String sNSFName = "names.nsf";
		int iMaxRows = 0;
		
		String sForm = null, sListName = null, sListCategory = null;
		String sMembers = null, sGroupMembers = null, sGroupName = null;
		String sUserName = null, sMailFile = null, sMailServer = null;
		boolean bPersonDelProcess = true, bGroupDelProcess = true;
		
		// Database를 가져온다.
		oDatabase = oSession.getDatabase( "", sNSFName );
		if ( oDatabase == null ) {
			throw new Exception ( "oDatabase is null");
		}
		
 		if ( oDatabase.isOpen() == false ) {
			throw new Exception ( "Could not open oDatabase");
		}
 		
 		oDocumentCollection = oDatabase.search( "SELECT Form=\"Group\" | Form=\"Person\"", null, 0 );
		
		System.out.println ( "Document Collection count : " + oDocumentCollection.getCount() );
		iMaxRows = oDocumentCollection.getCount();
		
		for (int iNext = 1; iNext <= iMaxRows; iNext++ ) {
			System.out.println ( "=========================================================================");
			System.out.println ( "iNext=" + iNext  );
			sForm = "";
			sListName = "";
			sMembers = "";
			
			sUserName = "";
			sMailFile = "";
			sMailServer = "";
			
			sGroupMembers = "";
			sGroupName = "";
			//sUser = "";
			
			//oDocument = oDocumentCollection.getNthDocument(iNext);
			
			if ( oDocument != null ) {
				recycle( oDocument );
			}
			oDocument = getNthDocument(oDocumentCollection,iNext);
			
			if (oDocument == null ) {
				//throw new Exception ( iNext + ". 노츠 Document객체가 null입니다." );
				System.out.println( iNext + " Document is null : document looping close" );
				break;
			}
			
			if ( getIsNotesItem( oDocument, "Form" ) ) {
	      		sForm = getNotesText( oDocument, "Form" );
			}
			
			if ( sForm.length() > 0 ) {
				if ( "GROUP".equalsIgnoreCase(sForm) ) {
					
					sGroupMembers = "";
					sListName = "";
					sListCategory = "";
					
					System.out.println("Form : " + sForm);
					
					if ( getIsNotesItem( oDocument, "ListCategory" ) ) {
						sListCategory = getNotesText( oDocument, "ListCategory" );
						if ( "ADMINISTRATION".equalsIgnoreCase(sListCategory) ) continue;
					}
					
					if ( getIsNotesItem( oDocument, "ListName" ) ) {
			      		sListName = getNotesText( oDocument, "ListName" );
			      		
			      		String[] aGroupName = sListName.split(";");
			      		
			      		if ( aGroupName.length > 0 ) {
			      			sGroupName = aGroupName[0];
			      		}
					}
					
					if ( getIsNotesItem( oDocument, "Members" ) ) {
			      		sMembers = getNotesText( oDocument, "Members" );
			      		
			      		String[] aMember = sMembers.split(";");
			      		
			      		for(int iLoop=0; iLoop<aMember.length; iLoop++) {
			      			//sGroupMemberNames = getUserName(aMember[iLoop]);
			      			//System.out.println("Member : " + aMember[iLoop]);
			      			
			      			// 그룹명과 멤버가 동일하면 즉 자기가 자기를 포함하면 skip
			      			if ( sGroupName.equalsIgnoreCase(aMember[iLoop]) ) continue;
			      			
			      			if (sGroupMembers.length() > 0) {
			      				sGroupMembers = sGroupMembers + ";" + aMember[iLoop];
			      			} else {
			      				sGroupMembers = aMember[iLoop];
			      			}
			      		}
			      		
					}
					
					sGroupName = sGroupName.replaceAll("'", "''");
					sGroupMembers = sGroupMembers.replaceAll("'", "''");
					
					System.out.println("GroupName : " + sGroupName + " / Members : " + sGroupMembers);
					
					if ( sGroupName.length() > 0 && sGroupMembers.length() > 0 )
						insertGROUPInfo(mo_connection, sGroupName, sGroupMembers, bGroupDelProcess);
					
					if (bGroupDelProcess) bGroupDelProcess = false;
					
				} else if ( "PERSON".equalsIgnoreCase(sForm) ) {
					//System.out.println("Form : " + sForm);
					if ( getIsNotesItem( oDocument, "FullName" ) ) {
			      		sUserName = getNotesText( oDocument, "FullName" );
					}
					if ( getIsNotesItem( oDocument, "MailFile" ) ) {
			      		sMailFile = getNotesText( oDocument, "MailFile" );
					}
					if ( getIsNotesItem( oDocument, "MailServer" ) ) {
			      		sMailServer = getNotesText( oDocument, "MailServer" );
					}
					
					if ( sUserName.length() > 0 ) {
						
						sUserName = sUserName.split(";")[0];
						sUserName = sUserName.replaceAll("'", "''");
						
						System.out.println("UserName : " + sUserName + " / MailFile : " + sMailFile + " / MailServer : " + sMailServer);
						
						insertUSERMailNSF(mo_connection, sUserName, sMailFile, bPersonDelProcess);
					
						if ( bPersonDelProcess ) bPersonDelProcess = false;
					}
					
				}
			}
		}
		
		if ( oDocument != null ) { try { oDocument.recycle(); oDocument = null; } catch( Exception e ) {} }
		if ( oDocumentCollection != null ) { try { oDocumentCollection.recycle(); oDocumentCollection = null; } catch( Exception e ) {} }
		if ( oDatabase != null ) { try { oDatabase.recycle(); oDatabase = null; } catch( Exception e ) {} }
		
	}
	
//	private static String getUserName(String sMember)
//	{
//		String[] aUser = sMember.split("/OU=");
//		
//		String sUserName = "";
//		if ( aUser.length == 2 ) {
//			if ( aUser[1].indexOf("/O=SEC") > -1 ) {
//				
//				sUserName = aUser[1].substring(0,aUser[1].indexOf("/O=SEC"));
//				
//				System.out.println("User : " + sUserName);
//				
//			}
//			else {
//				System.out.println("User : " + aUser[1]);
//				sUserName = aUser[1];
//			}
//		} else if ( aUser.length == 1 ) {
//			sUserName = aUser[0];
//			
//			if ( sUserName.indexOf("/O=SEC") > 0 ) {
//				if ( sUserName.indexOf("CN=") > -1 )
//					sUserName = sUserName.substring(sUserName.indexOf("CN=")+3, sUserName.indexOf("/O=SEC"));
//				else
//					sUserName = sUserName.substring(0,sUserName.indexOf("/O=SEC"));
//			}
//			System.out.println("Group : " + sUserName);
//		}
//			
//		return sUserName;
//	}
	
	/**
	 * TB_NSFLIST에 있는 NotesDB에서 ACL를 가지고 온다.<br>
	 * TB_NSFLIST에는 색인 대상의 Notes의 NSF 경로를 갖고 있는데 해당 NSF의 ACL(Access Control List) 정보를 가져온다.<br>
	 * Entry, Roles, User Type, Level 정보를 가져온다.
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @param oSession Notes Session
	 * @throws NotesException
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private static void getNotesDBACL(Connection mo_connection, Session oSession)
	throws SQLException, NotesException
	{
		Database oDatabase = null;
		
		String ms_Sql = "SELECT NSF,CATEGORYMID FROM TB_NSFLIST WHERE DEL_FLAG = 'N'";
		String sNSFName = null;
		boolean bDelProcess = true;
		
		// PreparedStatement 생성
		PreparedStatement oPstmt = mo_connection.prepareStatement( ms_Sql );
	  	
	  	//oPstmt.setString ( 1, "N" );
	  	
  		// 조회
  		ResultSet oRset = oPstmt.executeQuery();	
  		
		while ( oRset.next() ) {
	  		
	  		sNSFName = oRset.getString("NSF");
	  		
			System.out.println ( "DB Name : " + sNSFName );
			
			try {
				if ( oDatabase != null ) { try { oDatabase.recycle(); } catch( Exception e ) {} }
				
				// Database를 가져온다.
				oDatabase = oSession.getDatabase( "", sNSFName );
				if ( oDatabase == null ) {
					continue;
					//throw new Exception ( "oDatabase객체가 null입니다");
				}
				
				//System.out.println ( "3" );
				if ( oDatabase.isOpen() == false ) {
					continue;
					//throw new Exception ( "oDatabase객체가 Open되지 않았습니다");
				}
			} catch(NotesException e) {
				System.out.println(e);
				continue;
			}
			
			ACL acl = oDatabase.getACL();
			ACLEntry entry = acl.getFirstEntry();
			System.out.println("\n---------------- [" + sNSFName + "] --------------");
			do {
				if ( entry.getUserType() == ACLEntry.TYPE_SERVER || entry.getUserType() == ACLEntry.TYPE_SERVER_GROUP ) continue;
				//if ( entry.getRoles().isEmpty() ) continue;
				
				System.out.println("\nEntry : " + entry.getName());
				System.out.print("\tRoles : " + entry.getRoles());
				System.out.print("\tUser Type : " + entry.getUserType());
				System.out.print("\tget Level : " + entry.getLevel());
				
				insertNSFROLE(mo_connection, sNSFName, entry.getName(), entry.getRoles(), entry.getUserType(), bDelProcess);
				
				if (bDelProcess) bDelProcess = false;
				
			} while ((entry = acl.getNextEntry(entry)) != null);
			
		}
		
		System.out.println("\nACLEntry.TYPE_MIXED_GROUP : " + ACLEntry.TYPE_MIXED_GROUP);
		System.out.println("ACLEntry.TYPE_PERSON : " + ACLEntry.TYPE_PERSON);
		System.out.println("ACLEntry.TYPE_PERSON_GROUP : " + ACLEntry.TYPE_PERSON_GROUP);
		
		System.out.println("ACLEntry.TYPE_SERVER : " + ACLEntry.TYPE_SERVER);
		System.out.println("ACLEntry.TYPE_SERVER_GROUP : " + ACLEntry.TYPE_SERVER_GROUP);
		System.out.println("ACLEntry.TYPE_UNSPECIFIED : " + ACLEntry.TYPE_UNSPECIFIED); 
		
		if ( oRset != null ) { try { oRset.close(); } catch( Exception e ) {} }
		if ( oPstmt != null ) { try { oPstmt.close(); } catch( Exception e ) {} }
		
	}
	
	private static void insertBBSNSFROLE(
			Connection mo_connection, String sNSFName, String sGroupID, 
			String sRole, int iUserType, String sStatus, boolean bDelProcess)
	throws SQLException
	{
		String sSelectSql = null, sDelFlagSql = null;
		int iCount = 0;
		String sNSFRole = null;
		
		Statement oStmt = null;
		
		if (bDelProcess || ( "Active".equalsIgnoreCase(sStatus) == false ) ) {
			sDelFlagSql = "UPDATE TB_NSFROLE SET DEL_FLAG = 'Y' WHERE NSF = N'" + sNSFName + "'";
			
			oStmt = mo_connection.createStatement();
				
			oStmt.execute(sDelFlagSql);
					
			oStmt.close();
		}
		
		if ( "Active".equalsIgnoreCase(sStatus) ) {
		
			StringBuffer sbSql = new StringBuffer();
			
			sSelectSql = "SELECT COUNT(NSF) AS CNT FROM TB_NSFROLE WHERE NSF = ? AND GROUPID = ?";
			
			PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
			
			oPstmt.setString(1, sNSFName);
			oPstmt.setString(2, sGroupID);
			
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
					sNSFRole = getNSFRole(mo_connection,sNSFName,sGroupID, sRole);
					sbSql.append("UPDATE TB_NSFROLE SET ")
						.append("ROLE = '")
						.append(sNSFRole)
						.append("' , TYPE = ")
						.append(iUserType)
						.append(" , DEL_FLAG = 'N' ")
						.append("WHERE NSF = N'")
						.append(sNSFName)
						.append("' AND GROUPID = N'")
						.append(sGroupID)
						.append("'");
				} else {
					sbSql.append("INSERT INTO TB_NSFROLE(NSF, GROUPID, ROLE, TYPE, DEL_FLAG) VALUES(N'")
						.append(sNSFName).append("', N'")
						.append(sGroupID).append("', N'")
						.append(sRole).append("', ")
						.append(iUserType).append(", 'N')");
				}
				
				if ( oStmt != null ) {
					// 등록
					oStmt.execute(sbSql.toString());
					
					oStmt.close();
				}
			}
		}
	}

	private static String getNSFRole(Connection oCon, String sNSFName, String sGroupID, String sRole)
	throws SQLException
	{
		ArrayList<String> listNSFRole = new ArrayList<String>();
		String sNSFRole = "";
		String sSelectSql = "SELECT ROLE FROM TB_NSFROLE WHERE NSF = ? AND GROUPID = ?";
		
		PreparedStatement oPstmt = oCon.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sNSFName);
		oPstmt.setString(2, sGroupID);
		
		ResultSet oResult = oPstmt.executeQuery();
		
		if ( oResult != null ) {
			if ( oResult.next() == false ) {
				oResult.close();
				return sNSFRole;
			}
			
			sNSFRole = oResult.getString("ROLE");
			
			if ( sNSFRole != null && sNSFRole.trim().length() > 0 ) {
				String aNSFRole[] = sNSFRole.split(";");
				for(int iLoop=0; iLoop<aNSFRole.length; iLoop++) {
					if ( listNSFRole.contains(aNSFRole[iLoop]) == false ) {
						listNSFRole.add(aNSFRole[iLoop]);
					}
				}
			}
			
			if ( sRole != null && sRole.trim().length() > 0 ) {
				String aRole[] = sRole.split(";");
				for(int iLoop=0; iLoop<aRole.length; iLoop++) {
					if ( listNSFRole.contains(aRole[iLoop]) == false ) {
						listNSFRole.add(aRole[iLoop]);
					}
				}
			}
			
			sNSFRole = "";
			for(int iLoop=0; iLoop<listNSFRole.size(); iLoop++) {
				sNSFRole = sNSFRole.length() == 0 ? sNSFRole = listNSFRole.get(iLoop) : sNSFRole + ";" + listNSFRole.get(iLoop);
			}
		}
		
		if ( oPstmt != null ) { try { oPstmt.close();} catch(Exception e) {} }
		if ( oResult != null ) { try { oResult.close();} catch(Exception e) {} }
		
		return sNSFRole;
	}
	
	private static void insertNSFROLE(
								Connection mo_connection, 
								String sNSFName, 
								String sEntryName, 
								Vector<String> vEntryRoles, 
								int iUserType,
								boolean bDelProcess)
	throws SQLException
	{
		String sSelectSql = null, sDelFlagSql = null;
		String sRoles = null, sEntryRoles = null;
		int iCount = 0;
		
		Statement oStmt = null;
		
		for(int iLoop=0; iLoop < vEntryRoles.size(); iLoop++) {
			sRoles = (String)vEntryRoles.get(iLoop);
			
			sRoles = sRoles.substring(sRoles.indexOf("[")+1, sRoles.indexOf("]"));
			
			if (sEntryRoles == null) {
				sEntryRoles = sRoles;
			} else { 
				sEntryRoles = sEntryRoles + ";" + sRoles;
			}
		}
		
		System.out.println("Entry Roles : " + sEntryRoles);
		
//		if ( sEntryName.indexOf("/O=SEC") > 0 )
//			if ( sEntryName.indexOf("CN=") > -1 )
//				sEntryName = sEntryName.substring(sEntryName.indexOf("CN=")+3, sEntryName.indexOf("/O=SEC"));
		
		if (bDelProcess) {
			sDelFlagSql = "UPDATE TB_NSFROLE SET DEL_FLAG = 'Y'";
		
			oStmt = mo_connection.createStatement();
			
			oStmt.execute(sDelFlagSql);
				
			oStmt.close();
		}
		
		StringBuffer sbSql = new StringBuffer();
		
		sSelectSql = "SELECT COUNT(NSF) AS CNT FROM TB_NSFROLE WHERE NSF = ? AND GROUPID = ?";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sNSFName);
		oPstmt.setString(2, sEntryName);
		
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
				sbSql.append("UPDATE TB_NSFROLE SET ")
					.append("ROLE = '")
					.append(sEntryRoles)
					.append("' , TYPE = ")
					.append(iUserType)
					.append(" , DEL_FLAG = 'N' ")
					.append("WHERE NSF = N'")
					.append(sNSFName)
					.append("' AND GROUPID = N'")
					.append(sEntryName)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_NSFROLE(NSF, GROUPID, ROLE, TYPE, DEL_FLAG) VALUES(N'")
					.append(sNSFName).append("', N'")
					.append(sEntryName).append("', N'")
					.append(sEntryRoles).append("', ")
					.append(iUserType).append(", 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
	}
	
	private static void insertGROUPInfo(Connection mo_connection, String sGroupName, String sMembers, boolean bDelProcess)
	throws SQLException
	{
		String sSelectSql = null, sDelFlagSql = null;
		int iCount = 0;
		
		PreparedStatement oPstmt = null;
		Statement oStmt = null;
		
		if (bDelProcess) {
			sDelFlagSql = "UPDATE TB_GROUPINFO SET DEL_FLAG = 'Y'";
		
			oStmt = mo_connection.createStatement();
			
			oStmt.execute(sDelFlagSql);
			
			oStmt.close();
		}
		
		StringBuffer sbSql = new StringBuffer();
		
		sSelectSql = "SELECT COUNT(GROUPID) AS CNT FROM TB_GROUPINFO WHERE GROUPID = ?";
		
		oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		//oPstmt.setString(1, sGroupName.replaceAll("''", "'"));
		oPstmt.setString(1, sGroupName);
		
		if ( sGroupName.trim().equalsIgnoreCase("1.proejct''s name") )
		{
			System.out.println("---------- members : " + sMembers);
			// CN=yongjoo/OU=yongjoo/O=SEC;CN=sk1 user/OU=u0001/O=SEC;CN=Abraham Zaied/OU=zaied/O=SEC
			// CN=Abraham Zaied/OU=zaied/O=SEC
		}
		
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
			
			if (iCount > 10 ) {
				sbSql.append("UPDATE TB_GROUPINFO SET ")
					.append("MEMBERS = N'")
					.append(sMembers)
					.append("' , DEL_FLAG = 'N' ")
					.append("WHERE GROUPID = N'")
					.append(sGroupName)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_GROUPINFO(GROUPID, MEMBERS, DEL_FLAG) VALUES(N'")
					.append(sGroupName).append("', N'")
					.append(sMembers).append("', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
	}
	
	private static void insertUSERMailNSF(Connection mo_connection, String sUserName, String sMailNSF, boolean bDelProcess)
	throws SQLException, InterruptedException
	{
		String sSelectSql = null, sDelFlagSql = null;
		
		int iCount = 0;
		
		Statement oStmt = null;
		
		StringBuffer sbSql = new StringBuffer();
		
		/* 사용자 정보를 등록하거나 수정하기 전에 DEL_FLAG 처리를 한다. */
		/* 사용자 정보를 등록하거나 수정할 때 DEL_FLAG를 N으로 변경한다. */
		/* DEL_FLAG = 'Y' 처리는 사용자 정보를 변경할 때 한 번만 수행하게 한다. */
		if (bDelProcess) {
			sDelFlagSql = "UPDATE TB_USERMAILNSF SET DEL_FLAG = 'Y'";
		
			oStmt = mo_connection.createStatement();
			
			if (oStmt != null) {
				oStmt.execute(sDelFlagSql);
				
				/* 사용자 정보를 읽어오면서 메일 NSF 정보를 가지고 온다 */
				/* TB_NSFLIST에 업데이트 하기 전에 메일 관련 DEL_FLAG 처리를 한다. */
				sDelFlagSql = "UPDATE TB_NSFLIST SET DEL_FLAG = 'Y' WHERE CATEGORYTOP = 'mail'";
				oStmt.execute(sDelFlagSql);
				
				oStmt.close();
			}
		}
		
		sSelectSql = "SELECT COUNT(USERID) AS CNT FROM TB_USERMAILNSF WHERE USERID = ?";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sUserName.replaceAll("''","'"));
		
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
				sbSql.append("UPDATE TB_USERMAILNSF SET ")
					.append("MAILNSF = N'")
					.append(sMailNSF)
					.append("' , DEL_FLAG = 'N' WHERE USERID = N'")
					.append(sUserName)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_USERMAILNSF(USERID, MAILNSF, DEL_FLAG) VALUES(N'")
					.append(sUserName).append("', N'")
					.append(sMailNSF).append("', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
		
		/*------------------------------------------------------------*/
		sbSql.delete(0, sbSql.length());
		sSelectSql = "SELECT COUNT(NSF) AS CNT FROM TB_NSFLIST WHERE CATEGORYTOP = 'mail' AND NSF = ?";
		
		oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sMailNSF);
		
		oResult = oPstmt.executeQuery();
		
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
					.append("DEL_FLAG = 'N' , CATEGORYMID = '' WHERE CATEGORYTOP = 'mail' AND NSF = N'")
					.append(sMailNSF)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_NSFLIST(NSF, CATEGORYTOP, CATEGORYMID, DEL_FLAG) VALUES(N'")
					.append(sMailNSF).append("', 'mail', '', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
	}
	
	/**
	 * TB_GROUPINFO에서 정보를 분류하여 TB_USERGROUP 테이블에 정보를 설정<br>
	 * TB_GROUPINFO 테이블에는 그룹별 포함되어 있는 사용자 정보가 있는데 <br>
	 *              이를 User 와 Group 정보로 각각 매핑하여 분류하기 위한 작업을 수행
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @throws SQLException
	 */
	private static void setNotesUserGroup(Connection mo_connection)
	throws SQLException
	{
		String sSelectSql = null;
		String sGroupID = null, sMembers = null, sUserName = null;
		
		sSelectSql = "SELECT GROUPID, MEMBERS FROM TB_GROUPINFO WHERE DEL_FLAG = 'N'";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		ResultSet oResult = oPstmt.executeQuery();
		HashMap<String,ArrayList<String>> aGroupMap = new HashMap<String,ArrayList<String>>();
		
		ArrayList<String> aList = new ArrayList<String>();
		ArrayList<String> aSubList = new ArrayList<String>();
		
		while(oResult.next()) {
			sGroupID = oResult.getString("GROUPID");
			sMembers = oResult.getString("MEMBERS");
			
			String[] aMembers = sMembers.split(";");
			
			for(int iLoop=0; iLoop<aMembers.length; iLoop++) {
				aList = getGroupMembers(mo_connection, aMembers[iLoop]);
				
				if ( aList.size() == 1) {
					if ( aSubList.contains(aList.get(0)) == false )
						aSubList.add(aList.get(0));
				} else if ( aList.size() < 1) {
					if ( aSubList.contains(aMembers[iLoop]) == false )
						aSubList.add(aMembers[iLoop]);
				} else {
					for(int jLoop=0; jLoop<aList.size(); jLoop++) {
						if ( aSubList.contains(aList.get(jLoop)) == false )
							aSubList.add(aList.get(jLoop));
					}
				}
				
			}
			
			if (sGroupID != null && sGroupID.length() > 0) {
				if (aSubList.size() > 0)
					aGroupMap.put(sGroupID, aSubList);
			} 
			
			aSubList = new ArrayList<String>();
			
		}
		
		Set<String> aSetKey = aGroupMap.keySet();
		
		Iterator<String> aitKey = aSetKey.iterator();
		
		String sKey = null;
		ArrayList<String> aListUser = new ArrayList<String>();
		
		// 업데이트 된 정보를 확인하기 위해서 사용
		boolean bDelProcess = true;
		
		while(aitKey.hasNext()) {
			sKey = aitKey.next();
			
			aListUser = aGroupMap.get(sKey);
			
			for(int iLoop=0; iLoop<aListUser.size(); iLoop++) {
				System.out.println(aListUser.get(iLoop));
				
				sGroupID = sKey.replaceAll("'", "''");
				sUserName = aListUser.get(iLoop).replaceAll("'", "''");
				
				System.out.println("GroupID : " + sGroupID + " / UserName : " + sUserName);
				insertUSERGROUPInfo(mo_connection, sGroupID, sUserName, bDelProcess);
				if (bDelProcess) bDelProcess = false;
			}
			
		}
		
	}

	private static ArrayList<String> getGroupMembers(Connection mo_connection, String sGroupID)
	throws SQLException
	{
		ArrayList<String> aList = new ArrayList<String>();
		ArrayList<String> aSubList = new ArrayList<String>();
		String sMembers = null;
		boolean bExistData = false;
		
		String sSelectSql = null;
		
		sSelectSql = "SELECT GROUPID, MEMBERS FROM TB_GROUPINFO WHERE GROUPID = ? AND DEL_FLAG = 'N'";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sGroupID);
		
		ResultSet oResult = oPstmt.executeQuery();
		
		while(oResult.next()) {
			bExistData = true;
			sMembers = oResult.getString("members");
			
			System.out.println("sMemebers : " + sMembers);
			String[] aMembers = sMembers.split(";");
			
			for(int iLoop=0; iLoop<aMembers.length; iLoop++) {
				
				aSubList = getGroupMembers(mo_connection, aMembers[iLoop]);
				
				if (aSubList.size() < 1) {
					aList.add(aMembers[iLoop]);
				}
				
				for(int jLoop=0; jLoop<aSubList.size(); jLoop++) {
					aList.add(aSubList.get(jLoop));
				}
			}
		}
		
		if ( bExistData == false ) {
			aList.add(sGroupID);
		}
		
		if ( oResult != null ) try { oResult.close(); } catch(Exception e) {}
		if ( oPstmt != null ) try { oPstmt.close(); } catch(Exception e) {}
		
		return aList;
	}
	
	private static void insertUSERGROUPInfo(Connection mo_connection, String sGroupID, String sUserName, boolean bDelProcess)
	throws SQLException
	{
		String sSelectSql = null, sDelFlagSql = null;
		
		int iCount = 0;
		
		Statement oStmt = null;
		
		StringBuffer sbSql = new StringBuffer();
		
		/* 사용자 정보를 등록하거나 수정하기 전에 DEL_FLAG 처리를 한다. */
		/* 사용자 정보를 등록하거나 수정할 때 DEL_FLAG를 N으로 변경한다. */
		/* DEL_FLAG = 'Y' 처리는 사용자 정보를 변경할 때 한 번만 수행하게 한다. */
		if (bDelProcess) {
			sDelFlagSql = "UPDATE TB_USERGROUP SET DEL_FLAG = 'Y'";
		
			oStmt = mo_connection.createStatement();
			
			if (oStmt != null) {
				oStmt.execute(sDelFlagSql);
				
				oStmt.close();
			}
		}
		
		sSelectSql = "SELECT COUNT(USERID) AS CNT FROM TB_USERGROUP WHERE USERID = ? AND GROUPID = ?";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sUserName);
		oPstmt.setString(2, sGroupID);
		
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
				sbSql.append("UPDATE TB_USERGROUP SET ")
					.append("DEL_FLAG = 'N' WHERE USERID = N'")
					.append(sUserName)
					.append("' AND GROUPID = N'")
					.append(sGroupID)
					.append("'");
			} else {
				sbSql.append("INSERT INTO TB_USERGROUP(USERID, GROUPID, DEL_FLAG) VALUES(N'")
					.append(sUserName).append("', N'")
					.append(sGroupID).append("', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
				
				System.out.println("     SQL : " + sbSql.toString());
			}
		}
		
	}
	
	/**
	 * TB_USERROLE 정보를 설정한다<br>
	 * TB_USERMAILNSF 테이블에서 USERID를 가져와 TB_USERGROUP, TB_NSFLIST, TB_NSFROLE 테이블에서 관련된 정보를 구한다.<br>
	 * 그리고 나서 TB_USERROLE 테이블에 정보를 설정한다.
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @throws SQLException
	 */
	private static void setUserRole(Connection mo_connection)
	throws SQLException
	{
		String sSelectSql = null;
		String sUserName = null;
		String sGroupID = null, sRole = null, sNSF = null, sCategoryTop = null, sCategoryMid = null;
		String sTotGroupID = null, sTotRole = null;
		boolean bDelProcess = true;
		
		ArrayList<String> aListUser = new ArrayList<String>();
		
		StringBuffer sbSql = new StringBuffer();
		
		sSelectSql = "SELECT USERID FROM TB_USERMAILNSF WHERE DEL_FLAG = 'N'";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		ResultSet oResult = oPstmt.executeQuery();
		
		while(oResult.next()) {
			sUserName = oResult.getString("USERID");
			aListUser.add(sUserName);
		}
		
		if ( oResult != null ) try { oResult.close(); } catch(Exception e) {}
		if ( oPstmt != null ) try { oPstmt.close(); } catch(Exception e) {}
		
		sbSql.append("SELECT ")
		 	 .append("	A.NSF, A.GROUPID, A.ROLE, B.CATEGORYTOP, B.CATEGORYMID ")
		 	 .append("FROM ")
		 	 .append("	TB_NSFROLE A, ")
		 	 .append("	TB_NSFLIST B ")
		 	 .append("WHERE ")
		 	 .append("	( ")
		 	 .append("		A.GROUPID IN ")
		 	 .append("		( ")
		 	 .append("			SELECT ")
		 	 .append("				GROUPID ")
		 	 .append("			FROM ")
		 	 .append("				TB_USERGROUP ")
		 	 .append("			WHERE ")
		 	 .append("				USERID IN (?, '*/O=SEC') ")
		 	 .append("				AND DEL_FLAG = 'N' ")
		 	 .append("		) ")
		 	 .append("		OR A.GROUPID = ? ")
		 	 .append("	) ")
		 	 .append("	AND A.NSF = B.NSF ")
		 	 .append("	AND B.CATEGORYTOP != 'mail' ")
		 	 .append("	AND B.DEL_FLAG = 'N'")
		 	 .append("ORDER BY B.CATEGORYTOP, B.CATEGORYMID");
			
		if ( aListUser.size() > 0 ) {
			oPstmt = mo_connection.prepareStatement(sbSql.toString());
			
			for(int iLoop=0; iLoop<aListUser.size(); iLoop++) {
				sUserName = aListUser.get(iLoop);
				if ( sUserName != null && sUserName.length() > 0 ) {
					oPstmt.setString(1, sUserName);
					oPstmt.setString(2, sUserName);
					
					oResult = oPstmt.executeQuery();
					
					sGroupID = "";
					sRole = "";
					sNSF = "";
					sCategoryTop = "";
					sCategoryMid = "";
					
					sTotGroupID = "";
					sTotRole = "";
					sNSF = "";
					
					ArrayList<String> aListNSF = new ArrayList<String>();
					
					System.out.println("\nUser Name : " + sUserName);
					
					while(oResult.next()) 
					{
						sGroupID = oResult.getString("GROUPID"); 
						sRole = oResult.getString("ROLE"); 
						sNSF = oResult.getString("NSF"); 
						sCategoryTop = oResult.getString("CATEGORYTOP");
						sCategoryMid = oResult.getString("CATEGORYMID");
						
						if ( aListNSF.contains(sGroupID) == false ) {
							sTotGroupID = sTotGroupID.length() < 1 ? sGroupID : sTotGroupID + ";" + sGroupID;
							sTotRole = sTotRole.length() < 1 ? sRole : sTotRole + ";" + sRole;
							aListNSF.add(sGroupID);
						}
						System.out.println("NSF : " + sNSF + " / Group : " + sTotGroupID + " / Role : " + sTotRole + 
									" / CategoryTop : " + sCategoryTop + " / CategoryMid : " + sCategoryMid);
						
						insertUSERROLE(mo_connection, sUserName, sNSF, sTotGroupID, sTotRole, sCategoryTop, sCategoryMid, bDelProcess);
						
						if (bDelProcess) bDelProcess = false;
					}
					
					if ( oResult != null ) try { oResult.close(); } catch(Exception e) {}					
				}	
			}
			if ( oPstmt != null ) try { oPstmt.close(); } catch(Exception e) {}
		}
		
		
	}
	
	/**
	 * TB_USERROLE 테이블에 정보를 갱신한다.
	 * <p>
	 * @param mo_connection RDBMS Connection
	 * @param sUserName User Name(Canonical Type)
	 * @param sNSF NSF Path
	 * @param sGroupID Group ID
	 * @param sRole Role Information(세미콜론으로 멀티 값 구분)
	 * @param sCategoryTop 대분류 값
	 * @param sCategoryMid 중분류 값
	 * @param bPersonDelProcess 업데이트 처리 여부<br>
	 *                          true 이면 TB_USERROLE.DEL_FLAG = 'Y'로 업데이트
	 * @throws SQLException
	 */
	private static void insertUSERROLE(
									Connection mo_connection,
									String sUserName, 
									String sNSF, 
									String sGroupID, 
									String sRole, 
									String sCategoryTop, 
									String sCategoryMid, 
									boolean bPersonDelProcess)
	throws SQLException
	{
		String sSelectSql = null, sDelFlagSql = null;
		
		int iCount = 0;
		
		Statement oStmt = null;
		
		StringBuffer sbSql = new StringBuffer();
		
		/* 사용자 정보를 등록하거나 수정하기 전에 DEL_FLAG 처리를 한다. */
		/* 사용자 정보를 등록하거나 수정할 때 DEL_FLAG를 N으로 변경한다. */
		/* DEL_FLAG = 'Y' 처리는 사용자 정보를 변경할 때 한 번만 수행하게 한다. */
		if (bPersonDelProcess) {
			sDelFlagSql = "UPDATE TB_USERROLE SET DEL_FLAG = 'Y'";
		
			oStmt = mo_connection.createStatement();
			
			if (oStmt != null) {
				oStmt.execute(sDelFlagSql);
				
				oStmt.close();
			}
		}
		
		sSelectSql = "SELECT COUNT(USERID) AS CNT FROM TB_USERROLE WHERE USERID = ? AND NSF = ?";
		
		PreparedStatement oPstmt = mo_connection.prepareStatement(sSelectSql);
		
		oPstmt.setString(1, sUserName);
		oPstmt.setString(2, sNSF);
		
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
			
			sUserName = sUserName.replaceAll("'", "''");
			sGroupID = sGroupID.replaceAll("'", "''");
			
			if (iCount > 0 ) {
				sbSql.append("UPDATE TB_USERROLE SET ")
					.append("DEL_FLAG = 'N' , ")
					.append("GROUPID = N'").append(sGroupID).append("' , ")
					.append("ROLE = N'").append(sRole).append("' , ")
					.append("CATEGORYTOP = '").append(sCategoryTop).append("' , ")
					.append("CATEGORYMID = '").append(sCategoryMid).append("' ")
					.append("WHERE ")
					.append("USERID = N'").append(sUserName).append("'")
					.append("AND NSF = N'").append(sNSF).append("'");
			} else {
				sbSql.append("INSERT INTO TB_USERROLE(USERID, GROUPID, ROLE, NSF, CATEGORYTOP, CATEGORYMID, DEL_FLAG) VALUES(N'")
					.append(sUserName).append("', N'")
					.append(sGroupID).append("', N'")
					.append(sRole).append("', N'")
					.append(sNSF).append("', '")
					.append(sCategoryTop).append("', '")
					.append(sCategoryMid).append("', 'N')");
			}
			
			if ( oStmt != null ) {
				// 등록
				oStmt.execute(sbSql.toString());
	  		
				oStmt.close();
			}
		}
		
	}
	
	/**
	 * 테이블 정보를 업데이트하고 업데이트 되지 않은 항목은 삭제 처리하기 위한
	 * <p>
	 * @param oCon RDBMS Connection
	 * @param sCategory 대상 구분<br>
	 *                  GROUPINFO : TB_GROUPINFO 테이블에서 DEL_FLAG = 'Y'인 항목 삭제<br>
	 *                              TB_USERMAILNSF 테이블에서 DEL_FLAG = 'Y'인 항목 삭제<br>
	 *                  NSFROLE : TB_NSFROLE 테이블에서 DEL_FLAG = 'Y'인 항목 삭제<br>
	 *                  USERGROUP : TB_USERGROUP 테이블에서 DEL_FLAG = 'Y'인 항목 삭제<br>
	 *                  USERROLE : TB_USERROLE 테이블에서 DEL_FLAG = 'Y'인 항목 삭제
	 * @throws SQLException
	 */
	private static void deleteFlagYes(Connection oCon, String sCategory)
	throws SQLException
	{
		String sDeleteSql = "";
		
		Statement oStmt = null;
		
		if ("GROUPINFO".equalsIgnoreCase(sCategory)) {
			oStmt = oCon.createStatement();
			
			sDeleteSql = "DELETE FROM TB_GROUPINFO WHERE DEL_FLAG = 'Y'";
			oStmt.execute(sDeleteSql);
			
			sDeleteSql = "DELETE FROM TB_USERMAILNSF WHERE DEL_FLAG = 'Y'";
			oStmt.execute(sDeleteSql);
			oStmt.close();
		} else if ("NSFROLE".equalsIgnoreCase(sCategory)) {
			oStmt = oCon.createStatement();
			
			sDeleteSql = "DELETE FROM TB_NSFROLE WHERE DEL_FLAG = 'Y'";
			oStmt.execute(sDeleteSql);
			oStmt.close();
		} else if ("USERGROUP".equalsIgnoreCase(sCategory)) {
			oStmt = oCon.createStatement();
			
			sDeleteSql = "DELETE FROM TB_USERGROUP WHERE DEL_FLAG = 'Y'";
			oStmt.execute(sDeleteSql);
			oStmt.close();
		} else if ("USERROLE".equalsIgnoreCase(sCategory)) {
			oStmt = oCon.createStatement();
			
			sDeleteSql = "DELETE FROM TB_USERROLE WHERE DEL_FLAG = 'Y'";
			oStmt.execute(sDeleteSql);
			oStmt.close();
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

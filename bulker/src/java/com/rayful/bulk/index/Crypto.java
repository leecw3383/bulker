/**---------------------------------------------------------------
 * File  Name  : Crypto.java
 * Description : 
 * Special Logics :
 * @author : 
 * @version   1.0,  2005/03/01
 *---------------------------------------------------------------
 * History :
 *  DATE            AUTHOR              DESCRIPTION
 *  ------------    --------------      -------------------------
 *  2005.03.01                          Initial Release
 *  -------------------------------------------------------------
 * Copyright(c) 2005 2005 SK-Networks,  All rights reserved.
 *
 * NOTICE !      You can copy or redistribute this code freely,
 * but you should not remove the information about the copyright notice
 * and the author.
 */
package com.rayful.bulk.index;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import javax.crypto.IllegalBlockSizeException;

/**
 * @author  fast51
 */
public class Crypto
{
	
	public static final String defaultkeyfileurl = "defaultkey.key";
	
	/**
	 * @uml.property  name="pKey"
	 */
	private static java.security.Key pKey = null;
	
	/**
	 * @uml.property  name="crypto"
	 * @uml.associationEnd  
	 */
	private static Crypto crypto = null;
	
	/** 파일 처리용 버퍼 크기 */
	private static int BUFFER_SIZE = 8192;

	public static final byte[] keyByteArr = 
	{ -84, -19, 0, 5, 115, 114, 0, 30, 
	   99, 111, 109, 46, 115, 117, 110, 
	   46, 99, 114, 121, 112, 116, 111, 
	   46, 112, 114, 111, 118, 105, 100, 
	   101, 114, 46, 68, 69, 83, 75, 101, 
	   121, 107, 52, -100, 53, -38, 21, 104, 
	   -104, 2, 0, 1, 91, 0, 3, 107, 101, 
	   121, 116, 0, 2, 91, 66, 120, 112, 
	   117, 114, 0, 2, 91, 66, -84, -13, 
	   23, -8, 6, 8, 84, -32, 2, 0, 0, 
	   120, 112, 0, 0, 0, 8, -15, -5, 
	   38, 59, 26, 98, 55, -51 };
	
	public static synchronized Crypto getInstace() throws Exception
	{
		if (crypto == null) {
			crypto = new Crypto(); 
		}
		return crypto;
	}
	
	private Crypto() throws Exception
	{
		javax.crypto.KeyGenerator generator = javax.crypto.KeyGenerator.getInstance("DES");
		generator.init(new java.security.SecureRandom());
		Crypto.getKey();
	}

	private  String encrypt(String ID) throws Exception {
		String rtn = null;
		
		if (ID == null || ID.length() == 0)
		{
			rtn = null;
			throw new Exception();
		}
		javax.crypto.Cipher cipher = javax.crypto.Cipher
				.getInstance("DES/ECB/PKCS5Padding");
		cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, Crypto.pKey);
		String amalgam = ID;

		byte[] inputBytes1 = amalgam.getBytes("UTF8");
		byte[] outputBytes1 = cipher.doFinal(inputBytes1);
		
		
		// java 1.8 에서 지원되지 않아 주석 처리
		//sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
		//rtn = encoder.encode(outputBytes1);
		
		
		// java 1.8에서 지원되는 형식으로 변경
		Encoder encoder = Base64.getEncoder();
		rtn = encoder.encodeToString(outputBytes1);

		//		System.out.println (Const.SYS_LOG_TITLE);
//		System.out.println(Const.SYS_LOG_FIRST + "ID			:	"+ID);
//		System.out.println(Const.SYS_LOG_FIRST + "SecurityID	:	"+rtn);
//		System.out.println(Const.SYS_LOG_FIRST + "pKey		:	"+Crypto.pKey);
//		System.out.println (Const.SYS_LOG_TITLE);
		return rtn;
	}

	private String decrypt(String codedID) throws Exception {
		
		if (codedID == null || codedID.length() == 0)
			return "";
		
		String strResult = null;
		try {
			javax.crypto.Cipher cipher = javax.crypto.Cipher
					.getInstance("DES/ECB/PKCS5Padding");
			cipher.init(javax.crypto.Cipher.DECRYPT_MODE, Crypto.pKey);
			
			// java 1.8 에서 지원되지 않아 주석 처리
			//sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
			// java 1.8에서 지원되는 형식으로 변경
			Decoder decoder = Base64.getDecoder();
			
			// java 1.8 에서 지원되지 않아 주석 처리
			//byte[] inputBytes1 = decoder.decodeBuffer(codedID);
			// java 1.8에서 지원되는 형식으로 변경
			byte[] inputBytes1 = decoder.decode(codedID);
			
			byte[] outputBytes2 = cipher.doFinal(inputBytes1);
			
			strResult = new String(outputBytes2, "UTF8");
		}catch (IllegalBlockSizeException e) {
			strResult = codedID;
		}
		return strResult;
			
	}

	private static void getKey() throws Exception {
		if (pKey == null) {
			java.io.ObjectInputStream in = new java.io.ObjectInputStream(
						new java.io.ByteArrayInputStream(keyByteArr));
			pKey = (java.security.Key) in.readObject();
			in.close();
		}
	}
	
	/**
	 * @return
	 * @uml.property  name="pKey"
	 */
	public java.security.Key getPKey() {
		return Crypto.pKey;
	}

	public static void main(String[] ars) throws Exception
	{
//		for(int i=0; i<=10; i++)
//		{
			String	userID	= "aaaaAAA";
			String en = Crypto.enCrypt(userID);
			String de = Crypto.deCrypt(en);

//			String	enc		= crypto.getSecurityID();
//			System.out.println(userID);
			System.out.println(de);
//			Thread.sleep(1000);
//		}
	}
	
	
	/**
	 * 
	 * @param val
	 * @return
	 * @throws Exception
	 */
	public static String enCrypt (String val) throws Exception {
		Crypto crypt = Crypto.getInstace();
		return crypt.encrypt(val);
	}

	/**
	 * 
	 * @param val
	 * @return
	 * @throws Exception
	 */
	public static String deCrypt (String val) throws Exception {
		Crypto crypt = Crypto.getInstace();
		return crypt.decrypt(val);
	}
	
	/**
	 * 파일 대칭 암호화
	 *
	 * @param infile 암호화할 파일명
	 * @param outfile 암호화된 파일명
	 * @throws Exception
	 */
	public static void encryptFile(String infile, String outfile)
	throws Exception
	{
		Crypto crypt = Crypto.getInstace();
		crypt.encryptfile(infile, outfile);
	}
	
	/**
	 * 파일 대칭 복호화
	 *
	 * @param infile 복호화할 파일명
	 * @param outfile 복호화된 파일명
	 * @throws Exception
	 */
	public static void decryptFile(String infile, String outfile)
	throws Exception
	{
		Crypto crypt = Crypto.getInstace();
		crypt.decryptfile(infile, outfile);
	}
	
	/**
	 * 파일 대칭 암호화
	 *
	 * @param infile 암호화할 파일명
	 * @param outfile 암호화된 파일명
	 * @throws Exception
	 */
	private void encryptfile(String infile, String outfile)
	throws Exception
	{
		javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("DES/ECB/PKCS5Padding");
		cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, Crypto.pKey);

		FileInputStream in = new FileInputStream(infile);
		FileOutputStream fileOut = new FileOutputStream(outfile);
		javax.crypto.CipherOutputStream out = new javax.crypto.CipherOutputStream(fileOut, cipher);
			
		byte[] buffer = new byte[BUFFER_SIZE];
		int length;

		while((length = in.read(buffer)) != -1)
			out.write(buffer, 0, length);

		in.close();
		out.close();
	}

	/**
	 * 파일 대칭 복호화
	 *
	 * @param infile 복호화할 파일명
	 * @param outfile 복호화된 파일명
	 * @throws Exception
	 */
	private void decryptfile(String infile, String outfile)
	throws Exception
	{
		javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("DES/ECB/PKCS5Padding");
		cipher.init(javax.crypto.Cipher.DECRYPT_MODE, Crypto.pKey);		

		FileInputStream in = new FileInputStream(infile);
		FileOutputStream fileOut = new FileOutputStream(outfile);
		javax.crypto.CipherOutputStream out = new javax.crypto.CipherOutputStream(fileOut, cipher);
			
		byte[] buffer = new byte[BUFFER_SIZE];
		int length;

		while((length = in.read(buffer)) != -1)
			out.write(buffer, 0, length);

		in.close();
		out.close();
	}
	 
}



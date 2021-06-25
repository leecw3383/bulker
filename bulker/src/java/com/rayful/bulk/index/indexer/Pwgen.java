package com.rayful.bulk.index.indexer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.rayful.bulk.index.Crypto;

public class Pwgen {
  
  /**
  *  프로그램 사용법을 콘솔에 출력한다.
  */
  private static void printUsage() {
    // Parameter를 지정하지 않으면 프로그램을 종료합니다.
    System.out.println( " ***** pwgen.java *****");
    System.out.println( " 기능  : 문자열을 암호화한다. ");
    System.out.println( " 사용법: java com.rayful.netcomp.ftp.main.pwgen %1 %2");
    System.out.println( " ");
    
    System.out.println( " %1 - Encrypt(Decrypt) target String");
    System.out.println( " %2 - method(encrypt/decrypt) String");
    
    System.exit(1);
  }
  
    public static void main(String argv[]) {
      try {
        
        int iParameterSize = 0;
        iParameterSize = argv.length;
        
        Date time = new Date(); 
        System.out.println("time:"+time); 
          
        String dateFormat = "yyyyMMdd'T'HHmmss.SSSZ"; 
        SimpleDateFormat df = new SimpleDateFormat(dateFormat, Locale.KOREA); 
        System.out.println("KOREA:"+df.format(time)); 
          
        SimpleDateFormat sdf4 = new SimpleDateFormat ( dateFormat, Locale.US ); 
        System.out.println("US:"+sdf4.format(time)); 
      
        SimpleDateFormat sdf5 = new SimpleDateFormat ( dateFormat ); 
        sdf5.setTimeZone ( TimeZone.getTimeZone ( "America/Los_Angeles" ) ); 
        System.out.println("America/Los_Angeles:"+sdf5.format(time)); 
          
        SimpleDateFormat sdf6 = new SimpleDateFormat ( dateFormat ); 
        sdf6.setTimeZone ( TimeZone.getTimeZone ( "Asia/Seoul" ) ); 
        System.out.println("Asia/Seoul:"+sdf6.format(time)); 
        
        System.out.println(Arrays.toString(TimeZone.getAvailableIDs()));
        
        String sTest = argv[0];
        
        System.out.println(sTest + " : " + sTest.replaceAll("\\\\", "\\\\\\\\"));

        System.exit(1);
      
        if (iParameterSize != 2) {
          printUsage();
        }
        
        if ("encrypt".equalsIgnoreCase(argv[1])) {
        	System.out.println("Encrypt : " + Crypto.enCrypt(argv[0]));
        } else if ("decrypt".equalsIgnoreCase(argv[1])) {
        	System.out.println("Decrypt : " + Crypto.deCrypt(argv[0]));
        } 
//        else if ("file_encrypt".equalsIgnoreCase(argv[1])) {
//        	String infile = argv[0];
//        	String outfile = infile + ".encrypt";
//        	Crypto.encryptFile(infile, outfile);
//        } else if ("file_decrypt".equalsIgnoreCase(argv[1])) {
//        	String infile = argv[0];
//        	String outfile = infile + ".decrypt";
//        	Crypto.decryptFile(infile, outfile);
//        }
        
        System.exit(0);
      } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    }
    
}

package com.rayful.bulk.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/*import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;*/

public class TikaParser {
	
	private static final TikaParser INSTANCE = new TikaParser();

	public TikaParser() {}
	// 추추 사용 시 주석 해제 할것
	/*public void run1() throws IOException {
		String srcPath = "D:\\Data\\tika_test\\src";
		String destPath = "D:\\Data\\tika_test\\dest";
		
		File destFile = null;
		String destFileName = null;
		for (File file : new File(srcPath).listFiles()) {
			if (file.isFile()) {
				System.out.println("================================================= :" + file.getName());
				getMimeType(file);
				getCharset(file);
				
				destFileName = file.getName() + ".txt";
				destFile = new File (destPath, destFileName);
				getMetadata(file, destFile);
				System.out.println("=================================================");
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		INSTANCE.run1();
	}
	
	public static void getMetadata(File file, File textFile) {
		Tika tika = new Tika();
		Metadata metadata = new Metadata();

		BufferedWriter writer = null;
		try (TikaInputStream reader = TikaInputStream.get(file.toPath())){

			// 파일 본문
			String contents = tika.parseToString(reader, metadata);

			
			 * 파일 메타데이터 X-Parsed-By: org.apache.tika.parser.DefaultParser
			 * Content-Encoding: UTF-8
			 * csv:delimiter: comma
			 * Content-Type: text/csv; charset=UTF-8; delimiter=comma
			 
            
	        for(String name : metadata.names()) {
	            System.out.println(name + ": " + metadata.get(name));
	        }
	        
	        writer =  new BufferedWriter(new OutputStreamWriter(new FileOutputStream(textFile), StandardCharsets.UTF_8));
	        writer.write(contents);

		} catch (IOException | TikaException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) try {writer.close();} catch(IOException e) {}
		}
		
	}
	
	public static void getMimeType(File file) {
		String mimeType;
		Tika tika = new Tika();
		
		try {
			mimeType = tika.detect(file);
			System.out.println("mimeType=" + mimeType);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void getCharset(File file) {
		try {
			byte[] arr = Files.readAllBytes(file.toPath());
			
			CharsetDetector charsetDetector = new CharsetDetector();
			charsetDetector.setText(arr);
			charsetDetector.enableInputFilter(true);
			CharsetMatch cm = charsetDetector.detect();
			
			System.out.println(cm.getName());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/

}

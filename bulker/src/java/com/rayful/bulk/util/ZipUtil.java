package com.rayful.bulk.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtil {
	
	/** 마지막 버퍼 읽은 사이즈 */
	private int bufferReadSize;
	/** 버퍼 : 읽은 사이즈 만큼만 사용 */
	private static byte buffer[] = new byte[8192];
	
	/** 최상위 디렉토리를 압축에 포함할지 여부 */
	private boolean includeRoot;
	/** 자식 디렉토리를 포함할지 여부 */
	private boolean includeChild;
	/** 덮어쓰기 허용할지 여부 */
	private boolean overWrite;
	
	/** 최상위 디렉토리명 */
	private String rootDir;
	
	/** 유닉스 계열의 경로 구분 문자 */
	private static final char UNIX_PATH_SEPERATOR = '/';
	/** 윈도우 계열의 경로 구분 문자 */
	private static final char WINDOW_PATH_SEPERATOR = '\\';
	/** 기본 경로 구분 문자 */
	private static final char PATH_SEPERATOR = File.separatorChar;
	
	/** 압축 레벨 */
	private static final int COMPRESSION_LEVEL = 8;	// 압축 레벨 - 최대 압축률은 9, 디폴트 8
	
	/**
	 * 특정 디렉토리의 파일을 압축하여 파일로 압축 
	 * <p>
	 * @param srcDir 압축하고자 하는 대상 디렉토리(폴더)
	 * @param zipFile 새로 만들어지는 압축 파일명
	 * @throws Exception
	 */
	public void compressZip(String srcDir, String zipFile) throws Exception {
		// 존재여부 검사
		File fp = new File(zipFile);
		if( !overWrite && fp.exists() )
			throw new Exception("ZIP File already exists!");
		compress( new FileOutputStream(fp), srcDir);
	}
	
	/**
	 * 특정 디렉토리의 파일 압축
	 * <p>
	 * @param os File OuptputStream : 새로 만들어지는 압축 파일 OutputStream
	 * @param srcDir 압축하고자 하는 대상 디렉토리(폴더)
	 * @throws Exception
	 */
	private void compress(OutputStream os, String srcDir) throws Exception {
		// 디렉토리 검사
		File fp = new File(srcDir);
		if( !fp.exists() )
			throw new Exception("Target File or Directory does not exists!");
		// 압축시작
		ZipOutputStream zos = null;
		try {
			// 압축파일 생성
			zos = new ZipOutputStream( new BufferedOutputStream(os) );
			
			// 압축 레벨 - 최대 압축률은 9, 디폴트 8
			zos.setLevel(COMPRESSION_LEVEL);
			
			// 최상위 경로 정리
			srcDir = setNormalizePathSeperator(srcDir);
			rootDir = srcDir.substring(srcDir.lastIndexOf(PATH_SEPERATOR));
			// 압축
			zipDir(srcDir, srcDir, zos);
			// 압축 종료
			zos.finish();
			zos.close();
			zos = null;
		} catch(Exception e) {
			throw e;
		} finally {
			if( zos != null ) {
				zos.finish();
				zos.close();
				zos = null;
			}
		}
	}
	
	/**
	 * 압축 대상을 특정 디렉토리로부터 정보를 읽어들여 작업
	 * <p>
	 * @param rootPath 대상 디렉토리(폴더)
	 * @param srcDir 압축을 하고자 하는 디렉토리(폴더) : 디렉토리에 포함된 파일을 읽어들여 압축 수행
	 * @param zos ZIP OuptputStream : 새로 만들어지는 ZipOutputStream
	 * @throws Exception
	 */
	private void zipDir(String rootPath, String srcDir, ZipOutputStream zos) throws Exception {
		// 파일목록
		File files[] = readDir(srcDir);
		if( files == null || files.length < 1 ) {
			File fp = new File(srcDir);
			if( !fp.exists() || !fp.isDirectory() )
				return;
			// 디렉토리 추가
			addEmptyDir(zos, rootPath, fp);
			return;
		}
		// 파일 압축
		for(int i=0; i<files.length; i++) {
			// 추가
			if( files[i].isDirectory() && includeChild ) {
				zipDir(rootPath, setNormalizePathSeperator(files[i].getAbsolutePath()), zos);
			} else if( files[i].isFile() ) {
				addFile(zos, rootPath, files[i]);
			}
		}
	}
	
	/**
	 * 압축 OutputStream에 대상 파일을 추가
	 * <p>
	 * @param zos ZIP OuptputStream : 새로 만들어지는 ZipOutputStream
	 * @param rootPath 대상 경로
	 * @param fp 파일
	 * @throws Exception
	 */
	private void addFile(ZipOutputStream zos, String rootPath, File fp) throws Exception {
		// 파일의 상대경로 : 압축 파일의 디렉토리 경로를 위해
		String path = getRelativePath(rootPath, setNormalizePathSeperator(fp.getAbsolutePath()), fp.getName());
		// 압축 항목 추가
		ZipEntry ze = null;
		if( path.length() > 0 )
			ze = new ZipEntry( path + File.separator + fp.getName() );
		else
			ze = new ZipEntry( fp.getName() );
		ze.setTime( fp.lastModified() );
		zos.putNextEntry(ze);
		// 파일 쓰기
		write(zos, fp);
		zos.closeEntry();
	}
	
	/**
	 * 빈 디렉토리 추가
	 * <p>
	 * @param zos ZIP OuptputStream : 새로 만들어지는 ZipOutputStream
	 * @param rootPath 대상 경로
	 * @param fp 파일
	 * @throws Exception
	 */
	private void addEmptyDir(ZipOutputStream zos, String rootPath, File fp) throws Exception {
		String path = getRelativePath(rootPath, setNormalizePathSeperator(fp.getAbsolutePath()), fp.getName());
		if( path == null || path.equals("") )
			path = fp.getName() + PATH_SEPERATOR;
		else
			path = path + PATH_SEPERATOR + fp.getName() + PATH_SEPERATOR;
		ZipEntry ze = new ZipEntry(path);
		ze.setTime(fp.lastModified());
		zos.putNextEntry(ze);
		zos.closeEntry();
	}
	
	/**
	 * 파일 쓰기
	 * <p>
	 * @param os 
	 * @param fp
	 * @throws Exception
	 */
	private void write(OutputStream os, File fp) throws Exception {
		BufferedInputStream bis = null;
		try {
			bis = new BufferedInputStream( new FileInputStream(fp) );
			while( ( bufferReadSize = bis.read(buffer) ) > 0 ) {
				os.write(buffer, 0, bufferReadSize);
			}
			bis.close();
			bis = null;
		} catch(Exception e) {
			throw e;
		} finally {
			if( bis != null ) {
				bis.close();
				bis = null;
			}
		}
	}
	
	/**
	 * 최상위 경로에서의 특정 파일에 대한 상대경로 구하기
	 * <p>
	 * @param rootPath 최상위 경로
	 * @param filePath 특정 파일의 경로
	 * @param fileName 특정 파일명
	 * @return
	 */
	private String getRelativePath(String rootPath, String filePath, String fileName) {
		if( rootPath.equals(filePath) )
			return "";
		String path = null;
		if( filePath.length() > rootPath.length() + fileName.length() + 1)
			path = filePath.substring(rootPath.length() + 1, filePath.length() - fileName.length() - 1);
		if( path != null && includeRoot )
			return rootDir + PATH_SEPERATOR + path;
		else if( includeRoot )
			return rootDir + PATH_SEPERATOR;
		else if( path != null )
			return path;
		else
			return "";
	}
	
	/**
	 * 특정 디렉토리의 모든 파일 및 디렉토리 목록 반환
	 * <p>
	 * @param path 대상 경로
	 * @return File Array
	 */
	private File[] readDir(String path) {
		File fp = new File(path);
		if( !fp.exists() || !fp.isDirectory() )
			return null;
		return fp.listFiles();
	}
	
	/**
	 * 경로 문자열 정리하기 : 경로구분문자 등등
	 * <p>
	 * @param path 대상 경로
	 * @return 경로 구분 문자
	 */
	private String setNormalizePathSeperator(String path) {
		StringBuffer sbuf = new StringBuffer();
		// 첫 글자의 경로 문자 검사
		char ch = path.charAt(0);
		if( ch != WINDOW_PATH_SEPERATOR && ch != UNIX_PATH_SEPERATOR )
			sbuf.append(ch);
		// 검사
		for(int i=1; i<path.length(); i++) {
			ch = path.charAt(i);
			// 이중 경로문자 제거, 마지막 경로문자 제거
			if( ch == WINDOW_PATH_SEPERATOR || ch == UNIX_PATH_SEPERATOR ) {
				if( sbuf.charAt(sbuf.length() - 1) == PATH_SEPERATOR )
					continue;
				else if( i < path.length() - 1 )
					sbuf.append(PATH_SEPERATOR);
				continue;
			}
			sbuf.append(ch);
		}
		return sbuf.toString();
	}
	
	/**
	 * 덮어쓰기 여부 설정
	 * <p>
	 * @param overWrite true : 압축 파일 존재하면 덮어씀 / false : 압축 파일 존재하면 수행하지 않음
	 */
	public void setOverWrite(boolean overWrite) {
		this.overWrite = overWrite;
	}
	
	/**
	 * 하위 디렉토리(폴더) 포함 여부 설정
	 * <p>
	 * @param withChild true : 하위 디렉토리 포함 / false : 하위 디렉토리 포함하지 않음 
	 */
	public void setIncludeChild(boolean withChild) {
		this.includeChild = withChild;
	}
	
	/**
	 * 최상위 경로 포함 여부 설정
	 * <p>
	 * @param withRoot true : 최상위 경로 포함 / false : 최상위 경로 포함하지 않음
	 */
	public void setIncludeRoot(boolean withRoot) {
		this.includeRoot = withRoot;
	}
	
	public static void main(String argv[]) throws Exception {
		ZipUtil zipUtil = new ZipUtil();
		
		zipUtil.setIncludeChild(true);
		zipUtil.setIncludeRoot(false);
		// 압축하는 파일 존재 여부에 따라 존재하면 Overwrite
		// false인 경우 존재하면 Exception 발생
		zipUtil.setOverWrite(true);

		zipUtil.compressZip("D:/FAST_Application/bulker/log", "D:/FAST_Application/bulker/log.zip");
	}
	
}


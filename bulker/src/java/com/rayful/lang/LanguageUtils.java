package com.rayful.lang;

import java.util.Map;

import org.apache.log4j.Logger;

import com.rayful.bulk.index.Config;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.bulk.sql.RdbmsEtcReader;

public class LanguageUtils {

	/** logger 지정 */
	static Logger logger = RayfulLogger.getLogger( RdbmsEtcReader.class.getName(), Config.LOG_PATH );
	
	/**
	 * Language And Encoding Setting
	 * @param dataMap	본문 데이터
	 * @param resultMap	색인 데이터
	 * @throws Exception
	 */
	
	public void setLanguageAndEncoding( Map<String, String> dataMap, Map<String, String> resultMap )
	throws Exception
	{
		logger.info( "	>>>setLanguageAndEncoding");
		int iRowCnt = 0;

		try {
			String sSiteCD = (String)dataMap.get("DSITE_CD");
			String sLanguage = null;
			String sLanguages = null;
			String sCharset = null;

			if (sSiteCD.equalsIgnoreCase("ae")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ar")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
			    iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("at")) {
				sLanguage = "de";
				sLanguages = "de";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("au")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("baltic")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("be")) {
				sLanguage = "nl";
				sLanguages = "nl";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("be_fr")) {
				sLanguage = "fr";
				sLanguages = "fr";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("br")) {
				sLanguage = "pt";
				sLanguages = "pt";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ca")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ca_fr")) {
				sLanguage = "fr";
				sLanguages = "fr";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ch")) {
				sLanguage = "de";
				sLanguages = "de";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ch_fr")) {
				sLanguage = "fr";
				sLanguages = "fr";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("cl")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("cn")) {
				sLanguage = "zh-simplified";
				sLanguages = "zh-simplified";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("co")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("cz")) {
				sLanguage = "cs";
				sLanguages = "cs";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("de")) {
				sLanguage = "de";
				sLanguages = "de";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("dk")) {
				sLanguage = "da";
				sLanguages = "da";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("es")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("eu")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("fi")) {
				sLanguage = "fi";
				sLanguages = "fi";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("fr")) {
				sLanguage = "fr";
				sLanguages = "fr";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("gr")) {
				sLanguage = "el";
				sLanguages = "el";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("hk")) {
				sLanguage = "zh-traditional";
				sLanguages = "zh-traditional";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("hk_en")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("hu")) {
				sLanguage = "hu";
				sLanguages = "hu";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("id")) {
				sLanguage = "id";
				sLanguages = "id";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ie")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("il")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("in")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("iran")) {
				sLanguage = "fa";
				sLanguages = "fa";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("it")) {
				sLanguage = "it";
				sLanguages = "it";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("jp")) {
				sLanguage = "ja";
				sLanguages = "ja";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("kz_ru")) {
				sLanguage = "ru";
				sLanguages = "ru";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("latin")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("mx")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("my")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("nl")) {
				sLanguage = "nl";
				sLanguages = "nl";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("no")) {
				sLanguage = "nn";
				sLanguages = "nn";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("nz")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("pe")) {
				sLanguage = "es";
				sLanguages = "es";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ph")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("pl")) {
				sLanguage = "pl";
				sLanguages = "pl";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("pt")) {
				sLanguage = "pt";
				sLanguages = "pt";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ro")) {
				sLanguage = "ro";
				sLanguages = "ro";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ru")) {
				sLanguage = "ru";
				sLanguages = "ru";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("sa_en")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("se")) {
				sLanguage = "sv";
				sLanguages = "sv";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("sec")) {
				sLanguage = "ko";
				sLanguages = "ko";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("sg")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("sk")) {
				sLanguage = "sk";
				sLanguages = "sk";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("th")) {
				sLanguage = "th";
				sLanguages = "th";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("tr")) {
				sLanguage = "tr";
				sLanguages = "tr";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("tw")) {
				sLanguage = "zh-traditional";
				sLanguages = "zh-traditional";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ua")) {
				sLanguage = "uk";
				sLanguages = "uk";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("ua_ru")) {
				sLanguage = "ru";
				sLanguages = "ru";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("uk")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("us")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("vn")) {
				sLanguage = "vi";
				sLanguages = "vi";
				sCharset = "utf-8";
				iRowCnt ++;
			} else if (sSiteCD.equalsIgnoreCase("za")) {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
				iRowCnt ++;
			} else {
				sLanguage = "en";
				sLanguages = "en";
				sCharset = "utf-8";
			}
			
			logger.info( "	iRowCnt=" + iRowCnt );
			
			resultMap.put( "language", sLanguage);
			resultMap.put( "languages", sLanguages);
			resultMap.put( "charset", sCharset);
			
		} catch ( Exception e) {
			logger.error("에러:Did not match any Language and Encoding");
			throw e;
		} finally {
		}
	}
}

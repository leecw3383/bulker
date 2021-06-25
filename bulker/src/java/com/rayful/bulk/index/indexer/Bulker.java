package com.rayful.bulk.index.indexer;

/**
 *******************************************************************
 * 파일명 : Indexer.java
 * 파일설명 : 색인프로그램을 구동시키는 클래스
 *******************************************************************
 * 작성일자     작성자    내용
 * -----------------------------------------------------------------
 * 2005/05/30   정충열    최초작성             
 *******************************************************************
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.rayful.bulk.index.BatchJob;
import com.rayful.bulk.index.Config;
import com.rayful.bulk.index.DFLoader;
import com.rayful.bulk.index.DFLoaderException;
import com.rayful.bulk.logging.RayfulLogger;
import com.rayful.localize.Localization;

/**
 * 색인프로그램을 구동시키는 클래스
 * <p>
 * 데이터소스의 유형에 따라 NotesIndexr 혹은 RdbmsIndexer를 생성한 후 run() 메서드를 실행하여 색인로직을 수행한다.
 * <p>
 * 그외 Config정보, DF정보, 로그정보를 관리한다.
 * <p>
 * main()함수가 정의됨
 * <p>
 */
public class Bulker {

	static Logger logger = null;

	// 입력한 Parameter들의 정보를 기억
	static String ms_ssTableName = null;
	static String ms_datasourceid = null;
	static String ms_dfFileName = null;
	static String ms_keys = null;
	static int mi_indexMode = DFLoader.INDEX_NONE;
	static int mi_maxRows = -1;

	static boolean mb_statusCheck = true;
	static boolean mb_batchjob = true;

	static File mo_lockFile = null;

	// localize 정보 : 색인 시 지정하지 않으면 KR로 설정된다.
	static String localize = Localization.LOCLALIZE_KR;

	// 로그를 위한 통계치
	static int mi_totalCnt = 0;
	static int mi_tryCnt = 0;
	static int mi_successCnt = 0;
	static int mi_failCnt = 0;
	static int mi_warnCnt = 0;
	static int mi_totalDatasourceCnt = 0;
	static int mi_exeDatasourceCnt = 0;

	/**
	 * 프로그램 사용법을 콘솔에 출력한다.
	 */
	public static void printUsage() {
		String sClassName = Bulker.class.getName();

		// Parameter를 지정하지 않으면 프로그램을 종료합니다.
		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0001"));
		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0002"));
		System.out.println(" ");
		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0003"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0004"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0005"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0006"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0007"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0008"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0009"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0010"));
		System.out.println("                      "
				+ Localization.getMessage(sClassName + ".Console.0011"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0012"));
		System.out.println("                       "
				+ Localization.getMessage(sClassName + ".Console.0013"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0014"));
		System.out.println("                       "
				+ Localization.getMessage(sClassName + ".Console.0015"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0016"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0017"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0018"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0019"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0020"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0021"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0022"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0023"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0024"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0025"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0026"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0027"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0028"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0029"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0030"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0031"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0032"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0033"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0034"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0035"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0036"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0037"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0038"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0039"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0040"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0041"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0042"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0043"));

		System.out.println(" ");
		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0044"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0045"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0046"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0047"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0048"));
		System.out.println("                  "
				+ Localization.getMessage(sClassName + ".Console.0049"));
		System.out.println("                  "
				+ Localization.getMessage(sClassName + ".Console.0050"));

		System.out.println(" "
				+ Localization.getMessage(sClassName + ".Console.0051"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0052"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0053"));
		System.out.println("          "
				+ Localization.getMessage(sClassName + ".Console.0054"));

		System.exit(1);
	}

	/**
	 * 실행시 입력한 Parameter들을 Parsing한다.
	 * <p>
	 * 
	 * @param aParameter
	 *            실행시 입력한 값을 저장한 Parameter배열
	 */
	private static boolean parseParameter(String[] aParameter) {
		String sCommand = null;
		String sParameterValue = null;

		try {
			for (int i = 0; i < aParameter.length - 1; i += 2) {
				sCommand = aParameter[i].toLowerCase();
				sParameterValue = aParameter[i + 1].toLowerCase();

				if (sCommand.equals("-maxrows")) {
					// 데이터소스별 최대색인수
					mi_maxRows = Integer.parseInt(sParameterValue);
				} else if (sCommand.equals("-datasourceid")) {
					// 색인할 특정 데이터소스ID
					ms_datasourceid = sParameterValue;
				} else if (sCommand.equals("-indexmode")) {
					// 색인모드
					if (sParameterValue.equals("reindex")) {
						mi_indexMode = DFLoader.INDEX_REINDEX;
					} else if (sParameterValue.equals("all")) {
						mi_indexMode = DFLoader.INDEX_ALL;
					} else if (sParameterValue.equals("modified")) {
						mi_indexMode = DFLoader.INDEX_MODIFIED;
					} else {
						return false;
					}
				} else if (sCommand.equals("-statuscheck")) {
					// 상태체크여부
					if (sParameterValue.equals("on")) {
						mb_statusCheck = true;
					} else if (sParameterValue.equals("off")) {
						mb_statusCheck = false;
					} else {
						return false;
					}
				} else if (sCommand.equals("-batchjob")) {
					// batchJob 수행여부
					if (sParameterValue.equals("on")) {
						mb_batchjob = true;
					} else if (sParameterValue.equals("off")) {
						mb_batchjob = false;
					} else {
						return false;
					}
				} else if (sCommand.equals("-dfname")) {
					// df명 명시
					ms_dfFileName = sParameterValue;

				} else if (sCommand.equals("-keys")) {
					ms_keys = sParameterValue;
					if (ms_keys == null || ms_keys.length() == 0) {
						System.out.println(Localization.getMessage(Bulker.class
								.getName()
								+ ".Console.0055"));
						return false;
					}
				} else if (sCommand.equals("-loglevel")) {
					// 색인모드
					if (sParameterValue.equals("all")) {
						RayfulLogger.LEVEL = Level.ALL;
					} else if (sParameterValue.equals("debug")) {
						RayfulLogger.LEVEL = Level.DEBUG;
					} else if (sParameterValue.equals("info")) {
						RayfulLogger.LEVEL = Level.INFO;
					} else if (sParameterValue.equals("off")) {
						RayfulLogger.LEVEL = Level.OFF;
					} else {
						return false;
					}
				} else if (sCommand.equals("-keyquery")) {
					// 색인모드
					if (sParameterValue.equals("all")) {
						Config.KEYQUERY_METHOD = Config.KEY_ALL;
					} else if (sParameterValue.equals("dbquery")) {
						Config.KEYQUERY_METHOD = Config.KEY_DBQUERY;
					} else if (sParameterValue.equals("errorfile")) {
						Config.KEYQUERY_METHOD = Config.KEY_ERROR;
					} else {
						return false;
					}
				} else if (sCommand.equals("-localize")) {
					localize = sParameterValue;
				}
			} // for

			if (ms_keys != null && ms_datasourceid == null) {
				System.out.println(Localization.getMessage(Bulker.class
						.getName()
						+ ".Console.0056"));
				return false;
			}
			if (ms_keys != null && mi_indexMode != DFLoader.INDEX_ALL) {
				System.out.println(Localization.getMessage(Bulker.class
						.getName()
						+ ".Console.0057"));
				return false;
			}

			ms_ssTableName = aParameter[aParameter.length - 1];
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 색인객체로 부터 Summary정보를 알아낸다.
	 */
	private static void getSummary(BaseIndexer oIndexer) {
		if (oIndexer != null) {
			mi_totalCnt += oIndexer.getTotalCount();
			mi_tryCnt += oIndexer.getTryCount();
			mi_successCnt += oIndexer.getSuccessCount();
			mi_failCnt += oIndexer.getFailCount();
			mi_warnCnt += oIndexer.getWarnCount();
		}
	}

	/**
	 * Summary Log를 기록한다. ( 색인시작시간, 색인종료시간, ... )
	 */
	private static void writeSummaryLog(boolean bNormalTerminate) {
		try {
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0023"));

			String sSummaryFileName = "SUMMARY";
			File oSummaryFile = new File(Config.SUMMARYLOG_PATH,
					sSummaryFileName);
			FileOutputStream oFis = new FileOutputStream(oSummaryFile, true);
			PrintStream oPs = new PrintStream(oFis);

			String sStartTime = com.rayful.bulk.ESPTypes.dateFormatString(Config.INDEX_DATE, "yyyy-MM-dd HH:mm:ss");
			String sEndTime = com.rayful.bulk.ESPTypes.dateFormatString(new java.util.Date(), "yyyy-MM-dd HH:mm:ss");

			StringBuffer oSb = new StringBuffer();
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0024"));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0025", Config.ESP_COLLECTION_NAME));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0026", sStartTime));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0027", sEndTime));

			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0028", mi_totalDatasourceCnt));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0029", mi_exeDatasourceCnt));

			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0030", mi_totalCnt));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0031", mi_tryCnt));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0032", mi_successCnt));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0033", mi_failCnt));
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0034", mi_warnCnt));

			if (bNormalTerminate) {
				oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0035"));
			} else {
				oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0036"));
			}
			oSb.append(Localization.getMessage(Bulker.class.getName() + ".Logger.0024"));

			oPs.println(oSb.toString());
			oPs.close();
		} catch (FileNotFoundException fnfe) {
			logger.error(Localization.getMessage(Bulker.class.getName() + ".Logger.0037"), fnfe);
		}
	}

	/**
	 * Indexer.properites 로 부터 설정정보를 읽어 Config객체에 세팅.
	 */
	private static void loadProperties() 
	{
		try {
			//String sPropFileName = "Indexer.properties";
			String sPropFileName = "Indexer.xml";

			ClassLoader cl;
			cl = Thread.currentThread().getContextClassLoader();
			if (cl == null)
				cl = ClassLoader.getSystemClassLoader();

			java.net.URL oUrl = cl.getResource(sPropFileName);

			// java.net.URL oUrl = Bulker.class.getResource( sPropFileName );

			if (oUrl == null) {
				throw new FileNotFoundException(Localization
						.getMessage(Bulker.class.getName() + ".Exception.0004"));
			}

			sPropFileName = oUrl.getPath();
			Config.load(sPropFileName, ms_ssTableName);
		} catch (FileNotFoundException fnfe) {
			System.out.println(fnfe);
			printUsage();
		} catch (IOException ioe) {
			System.out.println(ioe);
			printUsage();
		} catch (Exception e) {
			System.out.println(e);
			printUsage();
		}
	}

	/**
	 * LogBudle_<Language>.properties 로 부터 로그 메세지를 읽어 Localization 객체에 할당.
	 */
	private static void loadLocalize(String[] aParameter) {
		String sCommand = null;
		String sParameterValue = null;

		try {

			try {
				for (int i = 0; i < aParameter.length - 1; i += 2) {
					sCommand = aParameter[i].toLowerCase();
					sParameterValue = aParameter[i + 1].toLowerCase();

					if (sCommand.equals("-localize")) {
						localize = sParameterValue;
						break;
					}
				}
			} catch (Exception ae) {
			}

			ClassLoader cl;
			cl = Thread.currentThread().getContextClassLoader();
			if (cl == null)
				cl = ClassLoader.getSystemClassLoader();

			String sPropFileName = "LogBundle_" + localize.toUpperCase()
					+ ".properties";
			java.net.URL oUrl = cl.getResource(sPropFileName);

			// java.net.URL oUrl = Bulker.class.getResource( sPropFileName );

			if (oUrl == null) {
				throw new FileNotFoundException(Localization.getMessage(
						Bulker.class.getName() + ".Exception.0005", localize));
			}

			sPropFileName = oUrl.getPath();
			Localization.load(sPropFileName);

		} catch (FileNotFoundException fnfe) {
			System.out.println(fnfe);
			// printUsage();
		} catch (IOException ioe) {
			System.out.println(ioe);
			// printUsage();
		} catch (Exception e) {
			System.out.println(e);
			// printUsage();
		}

	}
	
	/**
	 * BaseIndexer 객체를 생성
	 * <p>
	 * 
	 * @param sClassName
	 *            생성할 클래스명
	 * @return BaseIndexer 객체
	 */
	private static BaseIndexer getIndexerClass(String sClassName)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		BaseIndexer oIndexer = null;

		logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0014", sClassName));
		try {
			oIndexer = (BaseIndexer) Class.forName(sClassName).newInstance();
		} catch (ClassNotFoundException cnfe) {
			logger.error(Localization.getMessage(Bulker.class.getName() + ".Logger.0015"), cnfe);
		} catch (InstantiationException ie) {
			logger.error(Localization.getMessage(Bulker.class.getName() + ".Logger.0015"), ie);
		} catch (IllegalAccessException iae) {
			logger.error(Localization.getMessage(Bulker.class.getName() + ".Logger.0015"), iae);
		}
		logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0016", sClassName));
		return oIndexer;
	}

	/**
	 * DF 파일명을 리턴
	 * <p>
	 * 
	 * @return DF 파일명
	 */
	private static String getDFFileName() {
		String sDFFileName = null;
		String sLogPath = null;

		if (ms_dfFileName != null) {
			// ### Parameter( -dfname )값 프로그램에 적용

			// 사용자가 입력한 DF명을 받는다.
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0039", ms_dfFileName));
			sDFFileName = ms_dfFileName + ".xml";

			// 로그파일경로도 변경
			sLogPath = Config.LOG_PATH.replaceAll(ms_ssTableName, ms_dfFileName);
			Config.LOG_PATH = sLogPath;
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0040", Config.LOG_PATH));

			// 로그객체도 다시로드
			logger = RayfulLogger.getLogger(Bulker.class.getName(), Config.LOG_PATH);
		} else {
			sDFFileName = ms_ssTableName + ".xml";
		}

		return sDFFileName;
	}

	/**
	 * 색인 전처리 배치작업을 수행합니다.
	 * <p>
	 * 
	 * @param oDFLoader
	 *            DF로더객체
	 * @param oDFFile
	 *            DF파일
	 */
	private static void doPreIndexBatchJob(DFLoader oDFLoader, File oDFFile)
			throws Exception {
		if (mb_batchjob) {
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0004"));
			ArrayList<BatchJob> oBatchJobList = oDFLoader.getBatchJobList();
			BatchJob oBatchJob = null;
			int iBatchJobResult = 0;

			for (int i = 0; i < oBatchJobList.size(); i++) {
				oBatchJob = (BatchJob) oBatchJobList.get(i);

				logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0005", oBatchJob.getJobName()));
				iBatchJobResult = oBatchJob.execute();

				logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0006", iBatchJobResult));

				if (iBatchJobResult != 0 && oBatchJob.getErropProcess() == BatchJob.ERRORPROCESS_TYPE_STOP) {
					throw new Exception(Localization.getMessage(Bulker.class.getName() + ".Exception.0003", oBatchJob.getJobName()));
				}

				if (oBatchJob.isDfReload()) {
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0007"));
					oDFLoader = null;
					oDFLoader = new DFLoader("euc-kr");
					oDFLoader.setLogger(Config.LOG_PATH);
					oDFLoader.load(oDFFile.getPath());
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0008"));
				}
			} // for

			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0009"));
		} else {
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0010"));
		}
	}

	/**
	 * DF에 명시된 DtatSource의 상태를 체크한다. ( state="D" )된 경우 그 데이터소스만 삭제처리한다. (색인대상제외)
	 * false 리턴 ( state="C" )된 색인대상에서 제외 false 리턴 ( state="S" )된 색인대상에서 제외 false
	 * 리턴 ( state="N" )된 색인대상 ture 리턴
	 * <p>
	 * 
	 * @param oDFLoader
	 *            DF로더객체
	 * @param sDataSourceId
	 *            특정데이터소스ID
	 * @param oSSWriter
	 *            색인수행객체
	 * @return true: 색인대상/ false: 색인대상아님
	 */
	private static boolean checkDataSourceState(DFLoader oDFLoader,
			String sDataSourceId) throws DFLoaderException, SQLException,
			IOException {
		// 데이터소스의 상태를 알아낸다.
		String sDataSourceState = oDFLoader.getDataSourceState();

		if (sDataSourceState.equals("N")) {
			return true;
		} else if (sDataSourceState.equals("S")) {
			// 이 데이터소스는 색인하지 않는다.
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0017"));
			return false;
		} else {
			// 이 데이터소스는 색인하지 않는다.
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0018"));
			return false;
		}
	}

	public static void main(String argv[]) {
		String sClassName = Bulker.class.getName();

		Connection oGlobalRdbmsConnection = null;
		boolean bIsDefinedGlobalConnect = false;

		BaseIndexer oIndexer = null;
		DFLoader oDFLoader = null;

		File oDFFile = null;

		boolean bNormalTerminate = false;
		//int iIndexState = 0;
		int iParameterSize = 0;

		loadLocalize(argv);

		// =============================================================================
		// Application Parameter 처리
		// =============================================================================
		iParameterSize = argv.length;
		if (iParameterSize % 2 != 1) {
			printUsage();
		} else {
			if (parseParameter(argv) == false) {
				printUsage();
			}
		}

		// =============================================================================
		// Indexer.properites 파일로 부터 색인에 필요한 설정정보를 알아내어 Config에 세팅.
		// =============================================================================
		loadProperties();

		// =============================================================================
		// 로그 Properities file로 부터 설정정보 읽기
		// =============================================================================
		// PropertyConfigurator.configure("log4j.properties");
		// 인식하지 못함 - 정충
		logger = RayfulLogger.getLogger(Bulker.class.getName(), Config.LOG_PATH);

		logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0038"));
		logger.info(Localization.getMessage(sClassName + ".Logger.0001", Config.ESP_COLLECTION_NAME));
		logger.info(Config.toText());

		try {

			// =============================================================================
			// 색인프로그램 상태체크
			// - 색인중에 있으면 색인프로그램은 수행되지 않는다.
			// ### Parameter( -statuscheck )값 프로그램에 적용
			// =============================================================================
			FileOutputStream ofs = null;
			try {
				if (mb_statusCheck == true) {
					mo_lockFile = new File(Config.LOCKFILE_PATH, ms_ssTableName + ".lck");
					if (!mo_lockFile.exists()) {
						ofs = new FileOutputStream(mo_lockFile);
						ofs.write(1);
					} else {
						mo_lockFile = null; // lock 파일을 지우지 않도록 null로 초기화한다.
						throw new SQLException(Localization.getMessage(Bulker.class.getName() + ".Exception.0001"));
					}
				}
			} catch (Exception e) {
				// 에러로그기록
				logger.fatal("\t" + Localization.getMessage(Bulker.class.getName() + ".Exception.0002"), e);
				// 색인프로그램을 비정상 종료시킨다.
				throw e;
			} finally {
				if (ofs != null)
					try {
						ofs.close();
					} catch (Exception e) {
					}
			}

			try {
				// =============================================================================
				// DF파일을 로드합니다.
				// =============================================================================
				// DF파일명을 알아낸다.
				String sDFFileName = getDFFileName();

				// DF파일 Load...
				oDFFile = new File(Config.DEFINITION_PATH, sDFFileName);
				logger.info(">>>" + Localization.getMessage(sClassName + ".Logger.0002"));
				oDFLoader = new DFLoader("euc-kr");
				oDFLoader.setLogger(Config.LOG_PATH);
				oDFLoader.load(oDFFile.getPath());
				
				logger.info(">>>" + Localization.getMessage(sClassName + ".Logger.0003"));

				// =============================================================================
				// Batch Job 목록을 알아내어 실행한다.
				// =============================================================================
				// ### Parameter( -batchjob )값 프로그램에 적용
				doPreIndexBatchJob(oDFLoader, oDFFile);

				// =============================================================================
				// DF의 색인모드를 알아낸다.
				// =============================================================================
				Config.INDEX_MODE = oDFLoader.getTargetIndexMode();
				// ### Parameter( -indexmode )값 프로그램에 적용
				if (mi_indexMode != DFLoader.INDEX_NONE) {
					// Indexer실행시 색인모드를 지정했다면 그 값을 DF색인모드보다 우선으로 한다.
					Config.INDEX_MODE = mi_indexMode;
				}

				// =============================================================================
				// 전체데이터소스의 수를 알아낸다.
				// =============================================================================
				mi_totalDatasourceCnt = oDFLoader.getDataSourceCount();

				// =============================================================================
				// DF에 정의된 데이터소스수 만큼 루프를 돌며
				// 색인을 수행한다.
				// =============================================================================
				String sDataSourceId = null; // 데이터소스ID
				String sIndexerClassName = null; // Indexer클래스명
				int bDataSourceType; // 데이터소스Type
				int iCurDataSource = 0; // 현재 데이터소스 카운트

				while (oDFLoader.nextDataSource()) {
					// =============================================================================
					// 필요한 정보를 DF로 부터 알아낸다.
					// 데이터소스ID, 데이터소스Type
					// =============================================================================
					// 데이터소스ID를 알아낸다.
					sDataSourceId = oDFLoader.getDataSourceId();

					// ### Parameter( -datasourceid )값 프로그램에 적용
					// 특정 데이터소스를 찾을때까지 루프를 통과...
					if (ms_datasourceid != null) {
						if (ms_datasourceid.equalsIgnoreCase(sDataSourceId) == false) {
							continue;
						}
					}

					// 데이터소스타입을 알아낸다.
					bDataSourceType = oDFLoader.getDataSourceType();

					// DataSource의 수 카운트
					iCurDataSource++;

					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0011"));
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0012", sDataSourceId, iCurDataSource, mi_totalDatasourceCnt));
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0013"));

					// =============================================================================
					// 색인로직을 수행할 Indexer 클래스를 로드한다.
					// =============================================================================
					// DF에 명시한 Indexer Class명을 알아낸다.
					sIndexerClassName = oDFLoader.getDataSourceIndexerClassName();
					try {
						// DF에 명시한 Indexer를 로드
						oIndexer = getIndexerClass(sIndexerClassName);
					} catch (Exception e) {
						continue;
					}

					// =============================================================================
					// 전역 Connection이 선언된 경우 처리
					// =============================================================================
					if (bDataSourceType == DFLoader.DATASOURCE_TYPE_RDBMS) {
						bIsDefinedGlobalConnect = oDFLoader.isDefinedGlobalConnect();
						if (bIsDefinedGlobalConnect) {
							// 데이터소스 Connection정보가 전역적으로 선언되었다면 여기서 커넥션을 설정한다.
							((RdbmsIndexer) oIndexer).setConnection(oGlobalRdbmsConnection);
						}
					} else if (bDataSourceType == DFLoader.DATASOURCE_TYPE_NOTES) {
						// 아무런 작업도 하지 않는다.
						// 노츠를 데이터소스로 추가시 여기에 필요한 호출을 한다.
					}

					// =============================================================================
					// Application Option을 oIndxer에 설정
					// =============================================================================
					// ### Parameter( -maxrows )값 프로그램에 적용
					// maxrows값을 oIndexer에 설정
					if (mi_maxRows >= 0) {
						oIndexer.setMaxRows(mi_maxRows);
					}

					// ### Parameter( -pk )값 프로그램에 적용
					// pk값을 oIndexer에 설정
					// NotesIndexer에서만 유효, 현재 RdbmsIndexer의 경우는 pk설정값을 무시함.
					if (ms_keys != null) {
						oIndexer.setKey(ms_keys);
					}

					// =============================================================================
					// 특정데이터소스가 상태에 따른 처리
					// - ( state="S" )된 색인대상에서 제외 : false
					// - ( state="N" )된 색인대상 : true
					// =============================================================================
					if (checkDataSourceState(oDFLoader, sDataSourceId) == false) {
						// 해당 DataSource는 색인하지 않는다.
						continue;
					}

					// =============================================================================
					// ##### 실제 색인작업을 수행한다. #####
					// =============================================================================
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0018"));
					// ##### 색인메인로직을 수행 #####
					oIndexer.run(oDFLoader);

					// 색인수행결과를 수집한다.
					getSummary(oIndexer);
					logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0020"));
					// 색인 SUMMARY를 위해 실행된 데이터소스수를 카운트한다.
					mi_exeDatasourceCnt++;

					// =============================================================================
					// 전역 Connection이 선언된 경우 처리
					// =============================================================================
					if (bDataSourceType == DFLoader.DATASOURCE_TYPE_RDBMS) {
						if (bIsDefinedGlobalConnect
								&& ( oGlobalRdbmsConnection == null || oGlobalRdbmsConnection.isClosed() ) ) {
							// 데이터소스 Connection정보가 전역적으로 선언되었다면 ... 커넥션객체를 받아온다.
							oGlobalRdbmsConnection = ((RdbmsIndexer) oIndexer)
									.getConnection();
						}
					} else if (bDataSourceType == DFLoader.DATASOURCE_TYPE_NOTES) {
						// 아무런 작업도 하지 않는다.
						// 노츠를 데이터소스로 추가시 여기에 필요한 호출을 한다.
					}

					// ### Parameter( -datasourceid )값 프로그램에 적용
					// 특정 데이터소스의 색인을 수행후 더이상 색인을 수행하지 않음.
					if (ms_datasourceid != null) {
						if (ms_datasourceid.equalsIgnoreCase(sDataSourceId) == true) {
							break;
						}
					}
				} // while

			} catch (DFLoaderException de) {
				// 비정상 종료하도록 한다.
				throw de;
			}

			// 정상종료
			logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0021", Config.ESP_COLLECTION_NAME));
			bNormalTerminate = true;

		} catch (Throwable t) {
			logger.fatal(Localization.getMessage(Bulker.class.getName() + ".Logger.0022", Config.ESP_COLLECTION_NAME), t);
		} finally {

			// =============================================================================
			// 프로그램 종료를 위한 처리들
			// - lck 파일 삭제
			// - DF를 저장한다 ( 마지막 색인일등 수정사항 반영 )
			// - Summary Log를 기록
			// - 전역 데이터소스 Connection 종료 ( 전역적으로 Connection이 선언된 경우)
			// - SS Connection 종료
			// - MetaDB Connection 종료
			// =============================================================================
			// - DF가 변경된경우만 저장루틴실행 : 함수내부에서 변경여부 체크
			if (oDFLoader != null) {
				oDFLoader.save(oDFFile.getPath());
			}

			// - 색인테이블의 상태를 관리 DB에 기록한다.
			if (mo_lockFile != null) {
				mo_lockFile.deleteOnExit();
			}

			// - Summary Log를 기록한다.
			writeSummaryLog(bNormalTerminate);

			// - 데이터소스 Connection정보가 전역적으로 선언되었다면 ...데이터 소스 커넥션을 해제
			if (bIsDefinedGlobalConnect && oGlobalRdbmsConnection != null) {
				try {
					oGlobalRdbmsConnection.close();
				} catch (SQLException se) {
				}
			}
		}

		logger.info(Localization.getMessage(Bulker.class.getName() + ".Logger.0038"));
	}
}
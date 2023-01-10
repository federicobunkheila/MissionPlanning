/**
*
* MODULE FILE NAME: Configuration.java
*
* MODULE TYPE:      <Class definition>
*
* FUNCTION:         <Functional description of the DDC>
*
* PURPOSE:          <List of SR>
*
* CREATION DATE:    <01-Jan-2017>
*
* AUTHORS:          bunkheila Bunkheila
*
* DESIGN ISSUE:     1.0
*
* INTERFACES:       <prototype and list of input/output parameters>
*
* SUBORDINATES:     <list of functions called by this DDC>
*
* MODIFICATION HISTORY:
*
*             Date          |  Name      |   New ver.     | Description
* --------------------------+------------+----------------+-------------------------------
* <DD-MMM-YYYY>             | <name>     |<Ver>.<Rel>     | <reasons of changes>
* --------------------------+------------+----------------+-------------------------------
*
* PROCESSING
*/

package com.telespazio.csg.spla.csps.core.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.handler.HPCivilianRequestHandler;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RankPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.DeltaPlanProcessor;
import com.telespazio.csg.spla.csps.processor.ManualPlanProcessor;
import com.telespazio.csg.spla.csps.processor.NextARProcessor;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.processor.UnrankARListProcessor;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;

import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.exception.ConfigurationException;


/**
 * The CSPS configuration class.
 */
public class Configuration {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(Configuration.class);

	/**
	 * Configuration Properties
	 */
	public static CSPSProperties confProps;

	/**
	 * The DPL availability boolean (No DPL for debug = true)
	 */
	public static boolean debugDPLFlag;

	/**
	 * The SCM availability boolean (No SCM for flag = true)
	 */
	public static boolean debugSCMFlag;
	/**
	 * The SCM waiting boolean (No SCM is waited if flag = false)
	 */
	public static boolean waitSCMFlag;								   

	/**
	 * The EKM availability boolean (No EKM for flag = true)
	 */
	public static boolean debugEKMFlag;

	/**
	 * The loan boolean
	 */
	public static int loanRate;

	/**
	 * The premium rank-based Planning Session scheduling time (ms)
	 */
	public static double premSchedTime;

	/**
	 * The routine rank-based Planning Session scheduling time (ms)
	 */
	public static double routSchedTime;
	
	/**
	 * The optimization-based Planning Session scheduling time (ms)
	 */
	public static double optSchedTime;
	
	/**
	 * The maneuvering time (ms)
	 */
	public static double maneuverTime;

	/**
	 * The delta time (ms)
	 */
	public static double deltaTime;
		
	/**
	 * The AoI coverage fraction
	 */
	public static double aoICovFrac;

	/**
	 * The maximum DI2S compatibility time (ms)
	 */
	public static double di2sCompTime;

	/**
	 * The maximum number of HP per MH
	 */
	public static int maxHPNumber;

	/**
	 * The output file flag
	 */
	public static boolean outputFileFlag;

	/**
	 * The output file folder
	 */
	public static String outputFileFolder;

	/**
	 * The extra left cost
	 */
	public static double extraLeftCost;

	/**
	 * The sending timeout in (ms)
	 */
	public static double sendTimeout;

	/**
	 * The orbit revolution number
	 */
	public static double orbRevNumber;

	/**
	 * The Very Urgent visibility number
	 */
	public static double vuVisNumber;

	/**
	 * The default Memory Module size
	 */
	public static double defMMSize;

	/**
	 * The default split character
	 */
	public static String splitChar;

	/**
	 * The maximum length
	 */
	public static Integer maxLength;

	/**
	 * The check pdht flag
	 */
	public static boolean checkPDHTFlag;
	
	/**
	 * The dynamical DI2S boolean
	 */
	public static boolean dynDI2SBool;
	
	/**
	 * The ESS Factor
	 */
	public static double essFac;
	
	/**
	 * The Data Volume Factor
	 */
	public static double dataVolFac;
	
	/**
	 * The Data Volume BIC Map
	 */
	public static HashMap<DTOSensorMode, Double> dataVolBICMap;
	
	/**
	 * The Mega- to Mebi- byte factor
	 */
	public static double mega2MebiFac;	
	
	/**
	 * the Delta Packet Store Id number between MHs
	 */
	public static int deltaPacketNumber;
	
	/**
	 * the default GPS AddParam1
	 */
	public static String defGPSAddParam1;
	
	/**
	 * The  minimum download offset time
	 */
	public static double minDwlOffsetTime;
	
	/**
	 * The number of MHs available for the S-TUP
	 */
	public static double stupMHNumber;
	
	/**
	 * The cut-off delay
	 */
	public static double cutOffDelay;
	
	/**
	 * The TKI map for owner
	 */
	public static HashMap<String, Integer> tkiOwnerMap;
	
	/**
	 * The DI2S-ability flag only for requests relevant to the same UGS Id
	 */
	public static boolean sameUgsForDI2SFlag;
	
	/**
	 * The flag only for shared data to be downloaded on the same dwl station
	 */
	public static boolean sameDwlStationFlag;
	
	/**
	 * The premium Manual Replanning Session scheduling time (ms)
	 */
	public static double manSchedTime;
	
	/**
	 * The flag for the check of the DI2S requests visibility
	 */
	public static boolean checkDI2SVis;
	
	/**
	 * The not self-subscription compatible ugs list
	 */
	public static ArrayList<String> noSelfSubCompatibleUgsList;

	
	/**
	 *  The MSL variables
	 */
	private String activeMQURL;
	private String activeMQUser;
	private String activeMQPassword;
	private String moduleName;

	/**
	 * The Configuration handler
	 */
	public Configuration() {

	}

	/**
	 *
	 * @param filename
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws ConfigurationException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws DOMException
	 */
	public void init(String filename)
			throws ParserConfigurationException, SAXException, IOException, ConfigurationException,
			InstantiationException, IllegalAccessException, ClassNotFoundException, DOMException {

		/**
		 * The XML file
		 */
		File xmlFile = new File(filename);
		
		/**
		 * The DB Factory
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(xmlFile);

		// Normalize document
		doc.getDocumentElement().normalize();

		/**
		 * The node list
		 */
		NodeList nodeListActiveMQ = doc.getElementsByTagName("ActiveMQ");
		
		if (nodeListActiveMQ.getLength() == 0) {
			throw new ConfigurationException("Missing <ActiveMQ> elememt");
		}
		if (nodeListActiveMQ.getLength() > 1) {
			throw new ConfigurationException("Only one <ActiveMQ> element allowed");
		}
		for (int i = 0; i < nodeListActiveMQ.getLength(); ++i) {
			
			/**
			 * The Node
			 */
			Node nodeActiveMQ = nodeListActiveMQ.item(i);
			if (nodeActiveMQ.getNodeType() == Node.ELEMENT_NODE) {
				Element elementActiveMQ = (Element) nodeActiveMQ;

				Element elementURL = elementActiveMQ.getElementsByTagName("url").getLength() > 0
						? (Element) (NodeList) (elementActiveMQ.getElementsByTagName("url")).item(0)
						: null;
				Element elementUser = elementActiveMQ.getElementsByTagName("user").getLength() > 0
						? (Element) (NodeList) (elementActiveMQ.getElementsByTagName("user")).item(0)
						: null;
				Element elementPassword = elementActiveMQ.getElementsByTagName("password").getLength() > 0
						? (Element) (NodeList) (elementActiveMQ.getElementsByTagName("password")).item(0)
						: null;
				Element elementModuleName = elementActiveMQ.getElementsByTagName("moduleName").getLength() > 0
						? (Element) (NodeList) (elementActiveMQ.getElementsByTagName("moduleName")).item(0)
						: null;

				this.activeMQURL = Util.isNullReplace(elementURL.getTextContent().trim());
				this.activeMQUser = Util.isNullReplace(elementUser.getTextContent().trim());
				this.activeMQPassword = Util.isNullReplace(elementPassword.getTextContent().trim());
				this.moduleName = Util.isNullReplace(elementModuleName.getTextContent().trim());
			}
		}
	}

	/**
	 * Initialize CSPS
	 *
	 * @throws Exception
	 */
	public static void initCSPS() throws Exception {

		// Configure CSPS
		configCSPSProperties();
	
		// Configure log properties
		configLogProperties();
		logger.info("Setup CSPS configuration.");

		// Initialize session maps
		logger.info("Setup CSPS sessions.");
		setupSessions();

	}

	/**
	 * Setup maps for the CSPS sessions
	 */
	public static void setupSessions() {

		// Initialize maps

		/**
		 * Init Session Activator
		 */
		SessionActivator.planSessionMap = new HashMap<>();
		SessionActivator.schedDTOIdStatusMap = new HashMap<>();
		SessionActivator.mhPSIdListMap = new HashMap<>();
		SessionActivator.planPolicyMap = new HashMap<>();
		SessionActivator.planDateMap = new HashMap<>();
		SessionActivator.ownerARIdMap = new HashMap<>();
		SessionActivator.ownerListMap = new HashMap<>();
		SessionActivator.partnerListMap = new HashMap<>();
		SessionActivator.ugsOwnerIdMap = new HashMap<>();
		SessionActivator.ugsIdSubCompatibilityMap = new HashMap<>();	
		SessionActivator.ugsIsTUPMap = new HashMap<>();												 
		SessionActivator.initSchedDTOListMap = new HashMap<>();
		SessionActivator.initSchedARIdDTOMap = new HashMap<>();
		SessionActivator.initEquivDTOMap = new HashMap<>();
		SessionActivator.initARIdEquivDTOMap = new HashMap<>();
		SessionActivator.scmAvailMap = new HashMap<>();
		SessionActivator.scmResWaitMap = new HashMap<>();
		SessionActivator.firstSessionMap = new HashMap<>();
		SessionActivator.ownerAcqStationListMap = new HashMap<>();
		SessionActivator.ugsBackStationIdListMap = new HashMap<>();
		SessionActivator.workPSIdMap = new HashMap<>();
		SessionActivator.refPSIdMap = new HashMap<>();
		SessionActivator.activateMap = new HashMap<>();

		/**
		 * Init Next AR Processor
		 */
		NextARProcessor.nextARIterMap = new HashMap<>();
		NextARProcessor.nextSchedARMap = new HashMap<>();
		NextARProcessor.nextSchedDTOListMap = new HashMap<>();
		NextARProcessor.bestRankSolMap = new HashMap<>();
		NextARProcessor.nextARSubDateMap = new HashMap<>();

		/**
		 * Init Manual Plan Processor
		 */
		ManualPlanProcessor.manPlanIterMap = new HashMap<>();
		ManualPlanProcessor.manPlanARMap = new HashMap<>();
		ManualPlanProcessor.manPlanDTOListMap = new HashMap<>();
		ManualPlanProcessor.bestRankSolMap = new HashMap<>();

		/**
		 * Init PRList Processor
		 */
		PRListProcessor.schedARIdRankMap = new HashMap<>();
		PRListProcessor.newARSizeMap = new HashMap<>();
		PRListProcessor.pRListMap = new HashMap<>();
		PRListProcessor.refPRListMap = new HashMap<>();
		PRListProcessor.pRSchedIdMap = new HashMap<>();
		PRListProcessor.pSPRSchedIdMap = new HashMap<>();
		PRListProcessor.workPRSchedIdMap = new HashMap<>();	
		PRListProcessor.pRIntBoolMap = new HashMap<>();
		PRListProcessor.aRSchedIdMap = new HashMap<>();
		PRListProcessor.dtoSchedIdMap = new HashMap<>();
		PRListProcessor.schedDTOMap = new HashMap<>();
		PRListProcessor.equivTheatreMap = new HashMap<>();
		PRListProcessor.equivStartTimeIdMap = new HashMap<>();
		PRListProcessor.equivIdSchedARIdMap = new HashMap<>();
		PRListProcessor.equivStartTimeManMap = new HashMap<>();

		PRListProcessor.replPRListMap = new HashMap<>();
		PRListProcessor.crisisPRListMap = new HashMap<>();
		PRListProcessor.pRToPRListIdMap = new HashMap<>();
		PRListProcessor.discardPRIdListMap = new HashMap<>();

		/**
		 * Init Intersection Matrix
		 */
		IntMatrixCalculator.intDTOMatrixMap = new HashMap<>();
		IntMatrixCalculator.intTaskMatrixMap = new HashMap<>();

		/**
		 * Init Rules Performer
		 */
		RulesPerformer.brmInstanceListMap = new HashMap<>();
		RulesPerformer.brmWorkTaskListMap = new HashMap<>();
		RulesPerformer.brmRefAcqListMap = new HashMap<>();
		RulesPerformer.brmOperMap = new HashMap<>();
		RulesPerformer.brmParamsMap = new HashMap<>();
		RulesPerformer.brmInstanceMap = new HashMap<>();
		RulesPerformer.rejDTORuleListMap = new HashMap<>();

		/**
		 * Init Unranked ARList Processor
		 */
		UnrankARListProcessor.unrankSchedARListMap = new HashMap<>();

		/**
		 * Init HP/Civilian Request Handler
		 */
		HPCivilianRequestHandler.hpCivilDTOIdListMap = new HashMap<>();
		HPCivilianRequestHandler.hpCivilUniqueIdListMap = new HashMap<>();

		/**
		 * Init Rank Performer
		 */		
		RankPerformer.schedDTODomainMap = new HashMap<>();
		RankPerformer.iterMap = new HashMap<>();
		RankPerformer.jumpMap = new HashMap<>();
		RankPerformer.conflDTOIdListMap = new HashMap<>();
		RankPerformer.conflReasonDTOIdListMap = new HashMap<>();
		RankPerformer.conflElementListMap = new HashMap<>();
		RankPerformer.minARDwlTimeMap = new HashMap<>();

		/**
		 * Init Persistence Performer
		 */		
		PersistPerformer.workTaskListMap = new HashMap<>();
		PersistPerformer.refTaskListMap = new HashMap<>();
		PersistPerformer.refPSTaskListMap = new HashMap<>();
		PersistPerformer.refAcqIdMap = new HashMap<>();
//		PersistPerformer.partnerResPremBICMap = new HashMap<>();
		
		/**
		 * Init Session Scheduler
		 */
		SessionScheduler.satListMap = new HashMap<>();
		SessionScheduler.schedARListMap = new HashMap<>();
		SessionScheduler.schedDTOListMap = new HashMap<>();
		SessionScheduler.planDTOIdListMap = new HashMap<>();
		SessionScheduler.rejDTOIdListMap = new HashMap<>();
		SessionScheduler.rejARDTOIdSetMap = new HashMap<>();
		SessionScheduler.macroDLOListMap = new HashMap<>();
		SessionScheduler.persistenceMap = new HashMap<>();
		SessionScheduler.finalMap = new HashMap<>();
		SessionScheduler.ownerBICMap = new HashMap<>();
		SessionScheduler.ownerBICRepMap = new HashMap<>();
		SessionScheduler.dtoImageIdMap = new HashMap<>();
		SessionScheduler.ownerMinPSIdMap = new HashMap<>();
		SessionScheduler.intMinPSIdMap = new HashMap<>();
		SessionScheduler.planDLOListMap = new HashMap<>();

		/**
		 * Init Delta-Plan Processor
		 */
		DeltaPlanProcessor.initDTOListMap = new HashMap<>();
		DeltaPlanProcessor.initDTOIdListMap = new HashMap<>();
		DeltaPlanProcessor.currDTOListMap = new HashMap<>();
		DeltaPlanProcessor.deltaSchedDTOListMap = new HashMap<>();
		DeltaPlanProcessor.currPlanOffsetTimeMap = new HashMap<>();
		DeltaPlanProcessor.cancDTOIdListMap = new HashMap<>();
		DeltaPlanProcessor.cancTotDTOIdListMap = new HashMap<>();
		DeltaPlanProcessor.conflDTOIdListMap = new HashMap<>();
		DeltaPlanProcessor.conflReasonDTOIdListMap = new HashMap<>();
		DeltaPlanProcessor.conflElementListMap = new HashMap<>();

		/**
		 * Init Filter DTO Handler
		 */
		FilterDTOHandler.filtRejReqListMap = new HashMap<>();
		FilterDTOHandler.filtRejDTOIdListMap = new HashMap<>();
		FilterDTOHandler.isWaitFiltResultMap = new HashMap<>();

		/**
		 * Init Equivalent DTO Handler
		 */
		EquivDTOHandler.di2sMasterSchedDTOMap = new HashMap<>();		
		EquivDTOHandler.di2sSlaveSchedDTOMap = new HashMap<>();
		EquivDTOHandler.slaveDTOIdListMap = new HashMap<>();
		EquivDTOHandler.di2sLinkedIdsMap = new HashMap<>();

		
		/**
		 * Temporary maps
		 */
		PersistPerformer.pitchIntervalMap = new HashMap<>(); 
	}

	/**
	 * Configure logging properties
	 */
	public static void configLogProperties() {

		try {

			/**
			 * The log4j properties configuration
			 */
			PropertyConfigurator.configure(Environment.CONFIG_DIR + "log4j.properties");

			/**
			 * Log settings
			 */
			logger.debug("Custom Properties correctly loaded.");

		} catch (Exception ioe) {

			logger.error("Logging configuration Exception! ", ioe); //$NON-NLS-1$

		}
	}

	/**
	 * Configure CSPS properties
	 *
	 * @throws Exception
	 */
	public static void configCSPSProperties() throws Exception {

		try {
			/**
			 * The configuration properties
			 */		
			confProps = new CSPSProperties();
			
			// Init properties
			confProps.init(Environment.CSPS_PROPS_DIR);
			initProperties();

		} catch (Exception ex) {

			logger.error("Logging configuration Exception! ", ex); //$NON-NLS-1$

		}
	}

	/**
	 * 
	 * @return
	 */
	public String getModuleName() {
		return this.moduleName;
	}

	/**
	 * 
	 * @return
	 */
	public String getActiveMQURL() {
		return this.activeMQURL;
	}

	/**
	 * 
	 * @return
	 */
	public String getActiveMQUser() {
		return this.activeMQUser;
	}

	/**
	 * 
	 * @return
	 */
	public String getActiveMQPassword() {
		return this.activeMQPassword;
	}

	/**
	 * Initialize CSPS properties
	 *
	 * @throws Exception
	 */
	private static void initProperties() throws Exception {
	
		try {
	
			// Get CSPS properties
		
			// Debug Data
			debugDPLFlag = Boolean.parseBoolean(confProps.getProperty("debugDPLFlag"));
			debugSCMFlag = Boolean.parseBoolean(confProps.getProperty("debugSCMFlag"));
			debugEKMFlag = Boolean.parseBoolean(confProps.getProperty("debugEKMFlag"));
			waitSCMFlag = Boolean.parseBoolean(confProps.getProperty("waitSCMFlag"));			
	
			// BIC Data
			loanRate = Integer.parseInt(confProps.getProperty("loanRate"));
			premSchedTime = Double.parseDouble(confProps.getProperty("premSchedTime")) * 1000;
			routSchedTime = Double.parseDouble(confProps.getProperty("routSchedTime")) * 1000;
	
			// Execution times
			optSchedTime = Double.parseDouble(confProps.getProperty("optSchedTime")) * 1000;
			maneuverTime = Double.valueOf(confProps.getProperty("maneuverTime")) * 1000;
			deltaTime = Double.valueOf(confProps.getProperty("deltaTime")) * 1000;
			sendTimeout = Double.valueOf(confProps.getProperty("sendTimeout")) * 1000;
			
			// Di2s data
			aoICovFrac = Double.valueOf(confProps.getProperty("aoICovFrac"));
			di2sCompTime = Double.valueOf(confProps.getProperty("di2sCompTime")) * 1000;
	
			// Files data
			outputFileFlag = Boolean.valueOf(confProps.getProperty("outputFileFlag"));
			outputFileFolder = String.valueOf(confProps.getProperty("outputFileFolder"));
	
			// Process  data
			maxHPNumber = Integer.parseInt(confProps.getProperty("maxHPNumber"));
			dynDI2SBool = Boolean.valueOf(confProps.getProperty("dynDI2SBool")); 
			checkPDHTFlag = Boolean.valueOf(confProps.getProperty("checkPDHTFlag"));		
			
			// System Data
			splitChar = String.valueOf(confProps.getProperty("splitChar"));
			maxLength = Integer.valueOf(confProps.getProperty("maxLength"));
	
			// Constraints data
			extraLeftCost = Double.valueOf(confProps.getProperty("extraLeftCost"));
			orbRevNumber = Double.valueOf(confProps.getProperty("orbRevNumber"));
			vuVisNumber = Double.valueOf(confProps.getProperty("vuVisNumber"));
			defMMSize = Double.valueOf(confProps.getProperty("defMMSize"));
			minDwlOffsetTime = Double.valueOf(confProps.getProperty("minDwlOffsetTime"))  * 1000; 
			
			// Factors data
			essFac = Double.valueOf(confProps.getProperty("essFac")); 
			mega2MebiFac = 1.048576;
	
			// Data volume data
			dataVolFac = Double.valueOf(confProps.getProperty("dataVolFac")); 
			dataVolBICMap = new HashMap<>();
			dataVolBICMap.put(DTOSensorMode.SPOTLIGHT_1_MSOR, Double.valueOf(confProps.getProperty("SPOTLIGHT_1_MSOR_DataVolBIC")));
			dataVolBICMap.put(DTOSensorMode.SPOTLIGHT_2_MSOS, Double.valueOf(confProps.getProperty("SPOTLIGHT_2_MSOS_DataVolBIC")));
			dataVolBICMap.put(DTOSensorMode.SPOTLIGHT_2_MSJN, Double.valueOf(confProps.getProperty("SPOTLIGHT_2_MSJN_DataVolBIC")));
			
			// Download Params
			deltaPacketNumber = Integer.valueOf(confProps.getProperty("deltaPacketNumber"));
			defGPSAddParam1 = String.valueOf(confProps.getProperty("defGPSAddParam1"));
				
			// The STUP Data
			stupMHNumber = Double.valueOf(confProps.getProperty("stupMHNumber"))  * 1000; 
		
			// The cut-off delay		
			cutOffDelay = 15000;
			if (confProps.containsProperties("cutOffDelay")) {
			
				cutOffDelay = Double.valueOf(confProps.getProperty("cutOffDelay"))  * 1000; 
			}
			
			// The DI2S flag for same UGS Id
			sameUgsForDI2SFlag = false;			
			if (confProps.containsProperties("sameUgsForDI2SFlag")) {

				sameUgsForDI2SFlag = Boolean.valueOf(confProps.getProperty("sameUgsForDI2SFlag")); 
			}
			
			// The shared data flag for same station download
			sameDwlStationFlag = true;		
			if (confProps.containsProperties("sameDwlStationFlag")) {
				
				sameDwlStationFlag  = Boolean.valueOf(confProps.getProperty("sameDwlStationFlag"));  
			}
			
			if (confProps.containsProperties("manSchedTime")) {

				manSchedTime = Double.parseDouble(confProps.getProperty("manSchedTime")) * 1000;
			}
			
			if (confProps.containsProperties("checkDI2SVis")) {

				checkDI2SVis = Boolean.parseBoolean(confProps.getProperty("checkDI2SVis"));
			}
						
			// The default TKI for Owner map
			tkiOwnerMap = new HashMap<>();
			
			if (confProps.containsProperties("defOwnerTKI")) {
			
				/**
				 * The default TKI for owners
				 */
				String[] defTKIOwners = confProps.getProperty("defOwnerTKI").split(", ");
			
				for (int i = 0; i < defTKIOwners.length - 1; i ++) {
				
					tkiOwnerMap.put(defTKIOwners[i], Integer.parseInt(defTKIOwners[i + 1]));
					
					i++;
				}			
			}
			
			noSelfSubCompatibleUgsList = new ArrayList<>();
								 	
			noSelfSubCompatibleUgsList.addAll(Arrays.asList(
				confProps.getProperty("noSelfSubCompatibleUgs").split(" +")));	
			
		} catch (Exception ex) {
	
			logger.warn(ex.getStackTrace()[0].toString());
		}
	}

}

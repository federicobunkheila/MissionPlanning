/**
*
* MODULE FILE NAME: SessionActivator.java
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

package com.telespazio.csg.spla.csps.processor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.drools.DroolsOperations;
import com.nais.spla.brm.library.main.drools.DroolsParameters;
import com.nais.spla.brm.library.main.ontology.resources.ReasonOfRejectElement;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.handler.HPCivilianRequestHandler;
import com.telespazio.csg.spla.csps.model.impl.Partner;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RankPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;
import com.telespazio.csg.spla.csps.utils.MacroDLOCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.splaif.protobuf.ActivateMessage;
import com.telespazio.splaif.protobuf.ActivateMessage.Activate;
import com.telespazio.splaif.protobuf.Common.PlanningPolicyType;
import com.telespazio.splaif.protobuf.FilteringMessage.FilteringResult.RejectedRequest;

import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.PlanningSession;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.task.DLO;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;									 

/**
 * The session activator class
 *
 * @author bunkheila
 *
 */
public class SessionActivator {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(SessionActivator.class);

	/**
	 * The session Id list map
	 */
	public static HashMap<Long, List<Long>> mhPSIdListMap;

	/**
	 * The planning policy list map
	 */
	public static HashMap<Long, PlanningPolicyType> planPolicyMap;

	/**
	 * The first session map
	 */
	public static HashMap<Long, Boolean> firstSessionMap;

	/**
	 * The planning session date map
	 */
	public static HashMap<Long, Date> planDateMap;

	/**
	 * The owner list map
	 */
	public static HashMap<Long, List<Owner>> ownerListMap;

	/**
	 * The owner ARs Id map
	 */
	public static HashMap<Long, HashMap<String, ArrayList<String>>> ownerARIdMap;

	/**
	 * The SCM availability map
	 */
	public static HashMap<Long, Boolean> scmAvailMap;

	/**
	 * The SCM  response waiting map
	 */
	public static HashMap<Long, Boolean> scmResWaitMap;
	
	/**
	 * The initial scheduled DTO list map
	 */
	public static HashMap<Long, ArrayList<SchedDTO>> initSchedDTOListMap;
	
	/**
	 * The initial scheduling DTO map
	 */
	public static HashMap<Long, HashMap<String, SchedDTO>> initSchedARIdDTOMap;
	
	/**
	 * The initial equivalent DTO map
	 */
	public static HashMap<Long, HashMap<String, com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO>> initEquivDTOMap;

	/**
	 * The initial equivalent DTO map
	 */
	public static HashMap<Long, HashMap<String, com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO>> initARIdEquivDTOMap;
	
	/**
	 * The partner list map
	 */
	public static HashMap<Long, ArrayList<Partner>> partnerListMap;
	
	/**
	 * The UGS Owner Id map
	 */
	public static HashMap<Long, HashMap<String, String>> ugsOwnerIdMap;

	/**
	 * The UGS Id subscription compatibility map
	 */
	public static HashMap<Long, HashMap<String, List<String>>> ugsIdSubCompatibilityMap;

	/**
	 * The UGS isTUP map
	 */
	public static HashMap<Long, HashMap<String, Boolean>> ugsIsTUPMap;
	/**
	 * The list of owner acquisition station map
	 */
	public static HashMap<Long, HashMap<String, ArrayList<AcquisitionStation>>> ownerAcqStationListMap;

	/**
	 * The list of owner backup station map
	 */
	public static HashMap<Long, HashMap<String, ArrayList<String>>> ugsBackStationIdListMap;
	
	/**
	 * The planning session map
	 */
	public static HashMap<Long, PlanningSession> planSessionMap;
	
	/**
	 * The previously scheduled DTO Id map
	 */
	public static HashMap<Long, HashMap<String, DtoStatus>> schedDTOIdStatusMap;

	/**
	 * The working planning session Id map
	 */
	public static HashMap<Long, Long> workPSIdMap;

	/**
	 * The reference planning session Id map
	 */
	public static HashMap<Long, ArrayList<Long>> refPSIdMap;
	
	/**
	 * The planning session activate map
	 */
	public static HashMap<Long, Activate> activateMap;
	

	/**
	 * Activate the CSPS Scheduling Session. // TODO: identify end time from last
	 * task of previous Mission Horizon
	 *
	 * @param activate
	 * @return
	 * @throws Exception 
	 */
	public boolean activate(Activate activate) throws Exception {

		/**
		 * Instance handlers
		 */
		PersistPerformer persistPerformer = new PersistPerformer();

		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The output boolean
		 */
		boolean active = false;

		/**
		 * The Planning Session Id
		 */
		Long pSId = activate.getPlanningSessionId();

		try {

			// 1.0 Initialize new session
			initSession(activate);
			
			activateMap.put(pSId, activate);
			
			// 1.1 Get owners data
			active = persistPerformer.getOwnersData(planSessionMap.get(pSId),
					 getWorkPSId(pSId, activate), getRefPSId(pSId, activate));

			// 1.2 Get the MH reference data
			long refTime = new Date().getTime();

			if (active) {
			
				// 1.4 Get the initial statuses and tasks for the Planning Session 
				// (from working & reference data)			
				active = persistPerformer.setPlanSessionTasks(pSId, activate.getWorkingPlanningSession(),
						activate.getReferencePlanningSessionList(), refTime);
				
				// 1.5 Get the relevant state of the satellite resources
				active = persistPerformer.getSatState(planSessionMap.get(pSId), refTime, 
						getWorkPSId(pSId, activate), getRefPSId(pSId, activate));
							
				// 1.6 Get the relevant working PRs for the Planning Session
				if (activate.hasWorkingPlanningSession()) {
					
					persistPerformer.getWorkPSData(pSId, activate.getWorkingPlanningSession());
	
				} else {
	
					logger.info("No applicable working data exist for Planning Session: " + pSId);
				}
				
				if (activate.getReferencePlanningSessionList() != null 
						&& ! activate.getReferencePlanningSessionList().isEmpty()) {
					
					for (Activate.PlanningSession refPS : activate.getReferencePlanningSessionList()) {
					
						persistPerformer.getRefPSData(pSId, refPS);
					}
				}
				
				// 1.7 Setup Macro DLO for downloads
				MacroDLOCalculator.buildMHMacroDLOs(pSId);
				
				// 2.0 Setup BRM session
				if (RulesPerformer.setupBRMSession(planSessionMap.get(pSId), 
						SessionScheduler.satListMap.get(pSId))) {
	
					// 2.1. Initialize plan according to the relevant reference and working Planning Sessions 
					active = rulesPerformer.initBRMPlan(pSId);
					
					// Restore extra-cost for ranked routine session
					if (SessionChecker.isRankedRoutine(pSId)) {
	
						logger.info("Restore extra cost BICs for owners.");
						RulesPerformer.brmOperMap.get(pSId).restoreExtraCostLeftAcq(pSId.toString(), 
								RulesPerformer.brmInstanceMap.get(pSId), RulesPerformer.brmParamsMap.get(pSId));
						RulesPerformer.brmOperMap.get(pSId).restoreExtraCostTheatre(pSId.toString(), 
								RulesPerformer.brmInstanceMap.get(pSId), RulesPerformer.brmParamsMap.get(pSId));	
					}
				}

				if (! active) {
					
					logger.error("BRM is not properly initialized!");
				}

			} else {

				logger.error("BRM is not properly configured!");
			}

		} catch (Exception e) {

			logger.error("Error activating session {} - {}", pSId, e.getMessage(), e);

		} finally {

			logger.info("Activate processing ended.");
			System.out.println("");
		}
	
		return active;
	}
	
	
	/**
	 * Get the working PS Id
	 * @param pSId
	 * @param activate
	 * @return
	 */
	private Long getWorkPSId(Long pSId, Activate activate) {
	/**
	 * The working Planning Session Id
	 */
	long workPSId = 0;
	
	if (activate.hasWorkingPlanningSession()) {
			
		workPSId = activate.getWorkingPlanningSession().getPlanningSessionId();
	
		logger.debug("Working Planning Session data " + workPSId + " found for Planning Session: " + pSId);
		
		// 1.3 Update working and reference Planning Sessions maps
		workPSIdMap.put(pSId, workPSId);
		
	} else {
		
		logger.info("No applicable working data found for Planning Session: "  + pSId);
	}
	
	return workPSId;
}

	/**
	 * Get the reference PS Id
	 * @param pSId
	 * @param activate
	 * @return
	 */
	private Long getRefPSId(long pSId, Activate activate) {
	
	/**
	 * The reference Planning Session Id
	 */
	long refPSId = 0;
	
	if (! activate.getReferencePlanningSessionList().isEmpty()) {
		
		for (ActivateMessage.Activate.PlanningSession refPS : activate.getReferencePlanningSessionList()) {
			
			logger.debug("Reference data for reference Planning Session " + refPS.getPlanningSessionId() 
					+ " found for Planning Session: " + pSId);

			refPSIdMap.get(pSId).add(refPS.getPlanningSessionId());
				
			// Get the maximum reference PS Id
			if (refPS.getPlanningSessionId() > refPSId) {
			
				refPSId = refPS.getPlanningSessionId();
			}
		} 
		
	} else {
		
		logger.info("No applicable reference data found for Planning Session: " + pSId);
	}
	
	return refPSId;
	}

	/**
	 * Initialize the activation of Planning Session thread
	 *
	 * @param activate
	 * @throws InputException
	 */
	private static void initSession(Activate activate) throws InputException {
		
		
		/**
		 * The Planning Session Id
		 */
		Long pSId = activate.getPlanningSessionId();

		logger.info("Initialize new Planning Session " + pSId + " of type: " 
				+ activate.getSessionType());
		
		try {

			logger.info("Create the new Planning Session...");

			planDateMap.put(pSId, new Date());

			// 1.0. Set the new Planning Session
			PlanningSession pS = setNewPlanSession(activate);

			// 1.1. Initialize maps for the session
			initPlanSessionMaps(pS, activate);

		} catch (Exception e) {

			logger.error("Error activating session {} - {}", pSId, e.getMessage(), e);
		}
	}

	/**
	 * Set the new Planning Session
	 *
	 * @param activate
	 * @throws InputException
	 */
	private static PlanningSession setNewPlanSession(Activate activate) throws InputException {

		/**
		 * The new Planning Session
		 */
		PlanningSession pS = null;

		try {

			// 1.0 Initialize the new Planning Session data
			logger.debug("Initialize the new Planning Session data.");
			pS = new PlanningSession(activate.getPlanningSessionId(),
					ObjectMapper.parseCommonToPlanSessionType(activate.getSessionType()));

			// 1.1 Set planning session status
			pS.setStatus(ObjectMapper.parseCommonToPlanSessionStatus(activate.getSessionStatus()));

			// 1.2 Set planning session policy
			logger.debug("Planning Session policy: " + activate.getPlanningPolicy());
			planPolicyMap.put(activate.getPlanningSessionId(), activate.getPlanningPolicy());

			// 1.3 Set session mission horizon
			
			/**
			 * The date formatter
			 */
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

			/**
			 * The MH start date
			 */
			Date mhStart = formatter.parse(activate.getMissionHorizonStartTime());

			// Set MH start time
			pS.setMissionHorizonStartTime(mhStart);

			logger.info("Scheduling MH start time: " + pS.getMissionHorizonStartTime());

			if (activate.hasMissionHorizonStopTime()) {

				/**
				 * The MH stop date
				 */
				Date mhStop = formatter.parse(activate.getMissionHorizonStopTime());

				// Set Planning Session stop time
				pS.setMissionHorizonStopTime(mhStop);

				logger.info("Scheduling MH stop time: " + pS.getMissionHorizonStopTime());

			} else {

				// Set Planning Session stop time
				pS.setMissionHorizonStopTime(new Date((mhStart.getTime() + 43200 * 1000)));

				logger.info("Scheduling MH stop time: " + pS.getMissionHorizonStopTime());
			}

            // Set Planning Session Id parent for Delta-Plan
			if (activate.hasWorkingPlanningSession() 
					&& SessionChecker.isDelta(pS)) {

				logger.info("Planning Session Id parent: " + activate.getWorkingPlanningSession()
				.getPlanningSessionId());

				pS.setPlanningSessionIdParent(activate.getWorkingPlanningSession().getPlanningSessionId());
			}
			
		} catch (Exception e) {

			throw new InputException("Exception building new session.", e);
		}

		return pS;
	}

	/**
	 * Initialize CSPS maps for the Planning Session
	 *
	 * @param pS
	 * @param activate
	 * @throws Exception 
	 */
	public static void initPlanSessionMaps(PlanningSession pS, Activate activate) throws Exception {
		
		logger.debug("Initialize maps for Planning Session: " + pS.getPlanningSessionId());

		/**
		 * The PS Id
		 */
		Long pSId = pS.getPlanningSessionId();

		// Initialize maps according to the Planning Session Id

		// Persistence
		if (SessionScheduler.persistenceMap != null) {
			SessionScheduler.persistenceMap.put(pSId, "true");
		}
		
		// Finalization
		if (SessionScheduler.finalMap != null) {
			SessionScheduler.finalMap.put(pSId, 0);
		}
		// Planning Session
		if (planSessionMap != null) {
			planSessionMap.put(pSId, pS);
		}
		
		// scheduling DTO Id status
		if (schedDTOIdStatusMap != null) {
			schedDTOIdStatusMap.put(pSId, new HashMap<String, DtoStatus>());
		}

		// MH Planning Session Id list
		if (mhPSIdListMap != null) {

			mhPSIdListMap.put(pSId, new ArrayList<Long>());
			
			if (activate.getWorkingPlanningSession().getPlanningSessionId() > 0
					&& mhPSIdListMap.containsKey(
							activate.getWorkingPlanningSession().getPlanningSessionId())) {
			
				mhPSIdListMap.get(pSId).addAll(mhPSIdListMap.get(
						activate.getWorkingPlanningSession().getPlanningSessionId()));			
			}
						
			mhPSIdListMap.get(pSId).add(pSId);

		}

		// Satellite list
		if (SessionScheduler.satListMap != null) {
			SessionScheduler.satListMap.put(pSId, new ArrayList<Satellite>());
		}

		// Owner AR Id map
		if (ownerARIdMap != null) {
			ownerARIdMap.put(pSId, new HashMap<String, ArrayList<String>>());
		}

		// Planning Policy map
		if (planPolicyMap != null) {
			planPolicyMap.put(pSId, activate.getPlanningPolicy());
		}

		// First session map
		if (firstSessionMap != null) {

			firstSessionMap.put(pSId, false);
			
			if (SessionChecker.isInitial(pSId, activate)) {

				firstSessionMap.put(pSId, true);
			}
		}

		// Owner acquisition station list map
		if (ownerAcqStationListMap != null) {
			ownerAcqStationListMap.put(pSId, new HashMap<String, ArrayList<AcquisitionStation>>());
		}
		
		// UGS backup station Id list map
		if (ugsBackStationIdListMap != null) {
			ugsBackStationIdListMap.put(pSId, new HashMap<String, ArrayList<String>>());
		}

		// Init scheduled DTO list map
		if (initSchedDTOListMap != null) {
			initSchedDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}
		
		// Init sched AR Id map
		if (initSchedARIdDTOMap != null) {
			initSchedARIdDTOMap.put(pSId, new HashMap<String, SchedDTO>());
		}
	
		// Init equivalent DTO map
		if (initEquivDTOMap != null) {
			initEquivDTOMap.put(pSId, 
					new HashMap<String, com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO>());
		}
		
		// Init equivalent AR Id map
		if (initARIdEquivDTOMap != null) {
			initARIdEquivDTOMap.put(pSId, 
					new HashMap<String, com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO>());
		}
		
		// Owner List
		if (ownerListMap != null) {
			ownerListMap.put(pSId, new ArrayList<Owner>());
		}
		
		// Partner list map
		if (partnerListMap != null) {
			partnerListMap.put(pSId, new ArrayList<Partner>());
		}
		
		// UGS Owner Id map
		if (ugsOwnerIdMap != null) {
			ugsOwnerIdMap.put(pSId, new HashMap<String, String>());
		}
						
		// UGS Id SubCompatibility Id map
		if (ugsIdSubCompatibilityMap != null) {
			ugsIdSubCompatibilityMap.put(pSId, new HashMap<String, List<String>>());
		}

		// UGS isTUP map
		if (ugsIsTUPMap != null) {
			ugsIsTUPMap.put(pSId, new HashMap<String, Boolean>());
		}

		// SCM availability map
		if (scmAvailMap != null) {
			scmAvailMap.put(pSId, false);
		}
		
		// SCM wait response map
		if (scmResWaitMap != null) {
			scmResWaitMap.put(pSId, true);
		}

		// Working Planning Session Id map
		if (workPSIdMap != null) {
			workPSIdMap.put(pSId, null);
		}

		// Reference Planning Session Id map
		if (refPSIdMap != null) {
			refPSIdMap.put(pSId, new ArrayList<Long>());
		}
		
		// Activate map
		if (activateMap != null) {
			activateMap.put(pSId, Activate.getDefaultInstance());
		}

		// new AR Size map
		if (PRListProcessor.newARSizeMap != null) {
			PRListProcessor.newARSizeMap.put(pSId, 0);
		}

		// PR To PRList Ids map
		if (PRListProcessor.pRToPRListIdMap != null) {
			PRListProcessor.pRToPRListIdMap.put(pSId, new HashMap<String, ArrayList<String>>());
		}

		// Discard Ids map
		if (PRListProcessor.discardPRIdListMap != null) {
			PRListProcessor.discardPRIdListMap.put(pSId, new ArrayList<String>());
		}

		// ProgReqList
		if (PRListProcessor.pRListMap != null) {
			PRListProcessor.pRListMap.put(pSId, new ArrayList<ProgrammingRequest>());
		}
		
		// Reference ProgReqList
		if (PRListProcessor.refPRListMap != null) {
			PRListProcessor.refPRListMap.put(pSId, new ArrayList<ProgrammingRequest>());
		}

		// PR scheduling Ids
		if (PRListProcessor.pRSchedIdMap != null) {
			PRListProcessor.pRSchedIdMap.put(pSId, new HashMap<String, ProgrammingRequest>());
		}
		
		// Session PR scheduling Ids
		if (PRListProcessor.pSPRSchedIdMap != null) {
			PRListProcessor.pSPRSchedIdMap.put(pSId, new HashMap<String, ProgrammingRequest>());
		}
		
		// Working PR scheduling Ids
		if (PRListProcessor.workPRSchedIdMap != null) {
			PRListProcessor.workPRSchedIdMap.put(pSId, new HashMap<String, ProgrammingRequest>());
		}
		
		// PR scheduling Ids
		if (PRListProcessor.pRIntBoolMap != null) {
			PRListProcessor.pRIntBoolMap.put(pSId, new HashMap<String, Boolean>());
		}

		// AR Ids
		if (PRListProcessor.aRSchedIdMap != null) {
			PRListProcessor.aRSchedIdMap.put(pSId, new HashMap<String, AcquisitionRequest>());
		}

		// DTO Ids
		if (PRListProcessor.dtoSchedIdMap != null) {
			PRListProcessor.dtoSchedIdMap.put(pSId, new HashMap<String, DTO>());
		}

		// Sched DTOs
		if (PRListProcessor.schedDTOMap != null) {
			PRListProcessor.schedDTOMap.put(pSId, new HashMap<String, SchedDTO>());
		}

		// Equivalent Theatres
		if (PRListProcessor.equivTheatreMap != null) {
			PRListProcessor.equivTheatreMap.put(pSId, new ArrayList<EquivalentDTO>());
		}
		
		// Equivalent DTO Ids
		if (PRListProcessor.equivStartTimeIdMap != null) {
			PRListProcessor.equivStartTimeIdMap.put(pSId, new HashMap<String, String>());
		}
		
		// Equivalent Scheduling AR Ids
		if (PRListProcessor.equivIdSchedARIdMap != null) {
			PRListProcessor.equivIdSchedARIdMap.put(pSId, new HashMap<String, String>());
		}

		// Equivalent Maneuvers
		if (PRListProcessor.equivStartTimeManMap != null) {
			PRListProcessor.equivStartTimeManMap.put(pSId, new HashMap<String, Maneuver>());
		}
		
		// Replacing PRList
		if (PRListProcessor.replPRListMap != null) {
			PRListProcessor.replPRListMap.put(pSId, new ArrayList<ProgrammingRequest>());
		}
		
		// Crisis PRList
		if (PRListProcessor.crisisPRListMap != null) {
			PRListProcessor.crisisPRListMap.put(pSId, new ArrayList<ProgrammingRequest>());
		}

		// AR Id Rank
		if (PRListProcessor.schedARIdRankMap != null) {
			PRListProcessor.schedARIdRankMap.put(pSId, new HashMap<String, Integer>());
		}

		// Intersection DTO Matrix
		if (IntMatrixCalculator.intDTOMatrixMap != null) {
			IntMatrixCalculator.intDTOMatrixMap.put(pSId, new HashMap<String, Map<String, Integer>>());
		}

		// Intersection Task Matrix
		if (IntMatrixCalculator.intTaskMatrixMap != null) {
			IntMatrixCalculator.intTaskMatrixMap.put(pSId, new HashMap<String, Map<String, Integer>>());
		}

		// Next AR
		if (NextARProcessor.nextSchedARMap != null) {
			NextARProcessor.nextSchedARMap.put(pSId, new SchedAR());
		}

		// Next AR DTO list
		if (NextARProcessor.nextSchedDTOListMap != null) {
			NextARProcessor.nextSchedDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}

		// Next AR Iteration map
		if (NextARProcessor.bestRankSolMap != null) {
			NextARProcessor.bestRankSolMap.put(pSId, new ArrayList<SchedDTO>());
		}

		// Next AR Iteration map
		if (NextARProcessor.nextARIterMap != null) {
			NextARProcessor.nextARIterMap.put(pSId, 0);
		}
		
		
		// HP-Civilian DTO Id list map
		if (HPCivilianRequestHandler.hpCivilDTOIdListMap != null) {
			HPCivilianRequestHandler.hpCivilDTOIdListMap.put(pSId, new ArrayList<String>());
		}
		
		// HP-Civilian Unique Id list map
		if (HPCivilianRequestHandler.hpCivilUniqueIdListMap != null) {
			HPCivilianRequestHandler.hpCivilUniqueIdListMap.put(pSId, new ArrayList<String>());
		}

		// Unraked AR list
		if (UnrankARListProcessor.unrankSchedARListMap != null) {
			UnrankARListProcessor.unrankSchedARListMap.put(pSId, new HashMap<String, SchedAR>());
		}

		// Manual Plan iteration
		if (ManualPlanProcessor.manPlanIterMap != null) {
			ManualPlanProcessor.manPlanIterMap.put(pSId, 0);
		}

		// Manual Plan AR
		if (ManualPlanProcessor.manPlanARMap != null) {
			ManualPlanProcessor.manPlanARMap.put(pSId, new SchedAR());
		}

		// Manual Plan DTOList
		if (ManualPlanProcessor.manPlanDTOListMap != null) {
			ManualPlanProcessor.manPlanDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}

		// Best rank solution
		if (ManualPlanProcessor.bestRankSolMap != null) {
			ManualPlanProcessor.bestRankSolMap.put(pSId, new ArrayList<SchedDTO>());
		}

		// BRM Operations map
		if (RulesPerformer.brmOperMap != null) {
			RulesPerformer.brmOperMap.put(pSId, new DroolsOperations());
		}

		// BRM parameters map
		if (RulesPerformer.brmParamsMap != null) {
			RulesPerformer.brmParamsMap.put(pSId, new DroolsParameters());
		}
		
		// BRM parameters map
		if (RulesPerformer.brmInstanceMap != null) {
			RulesPerformer.brmInstanceMap.put(pSId, 0);
		}
		
		// BRM parameters map
		if (RulesPerformer.brmInstanceListMap != null) {
			if (!RulesPerformer.brmInstanceListMap.containsKey(pSId)) {

				RulesPerformer.brmInstanceListMap.put(pSId, new ArrayList<>());
			}
			
			RulesPerformer.brmInstanceListMap.get(pSId).add(RulesPerformer.brmInstanceListMap.size());
		}


		// // BRM Temporary parameters map
		// if (RulesPerformer.brmTmpParamsMap != null) {
		// RulesPerformer.brmTmpParamsMap.put(pSId, new DroolsParameters());
		// }

		// BRM Tasks
		if (RulesPerformer.brmWorkTaskListMap != null) {
			RulesPerformer.brmWorkTaskListMap.put(pSId, new ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Task>());
		}
		
		// BRM ref acquisitions
		if (RulesPerformer.brmRefAcqListMap != null) {
			RulesPerformer.brmRefAcqListMap.put(pSId, new ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Acquisition>());
		}

		// Rejected DTO Ids
		if (RulesPerformer.rejDTORuleListMap != null) {
			RulesPerformer.rejDTORuleListMap.put(pSId, new HashMap<String, List<ReasonOfRejectElement>>());
		}

		// Iteration
		if (RankPerformer.iterMap != null) {
			RankPerformer.iterMap.put(pSId, 0);
		}
		
		// Jump
		if (RankPerformer.jumpMap != null) {
			RankPerformer.jumpMap.put(pSId, 0);
		}

		// Session DTO List
		if (RankPerformer.schedDTODomainMap != null) {
			RankPerformer.schedDTODomainMap.put(pSId, new ArrayList<ArrayList<SchedDTO>>());
		}

		// Working Task List
		if (PersistPerformer.workTaskListMap != null) {
			PersistPerformer.workTaskListMap.put(pSId, new ArrayList<Task>());
		}
		
		// Reference Task List
		if (PersistPerformer.refTaskListMap != null) {
			PersistPerformer.refTaskListMap.put(pSId, new ArrayList<Task>());
		}
		
		// Reference PS Id Task List
		if (PersistPerformer.refPSTaskListMap != null) {
			PersistPerformer.refPSTaskListMap.put(pSId, new HashMap<Long, ArrayList<Task>>());
		}

		// Reference Acquisiton Ids
		if (PersistPerformer.refAcqIdMap != null) {
			PersistPerformer.refAcqIdMap.put(pSId, new HashMap<String, Task>());
		}
		
//		// Partner residual Premium BICs in the MH
//		if (PersistPerformer.partnerResPremBICMap != null) {		
//			PersistPerformer.partnerResPremBICMap.put(pSId, new HashMap<String, Double>());
//		}

		
		// Scheduled AR List
		if (SessionScheduler.schedARListMap != null) {
			SessionScheduler.schedARListMap.put(pSId, new ArrayList<SchedAR>());
		}
		// Scheduled DTO List
		if (SessionScheduler.schedDTOListMap != null) {
			SessionScheduler.schedDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}
		// Scheduled DTO List
		if (SessionScheduler.planDTOIdListMap != null) {
			SessionScheduler.planDTOIdListMap.put(pSId, new ArrayList<String>());
		}
		// Rejected DTO List
		if (SessionScheduler.rejDTOIdListMap != null) {
			SessionScheduler.rejDTOIdListMap.put(pSId, new ArrayList<String>());
		}

		// Rejected AR DTO Set
		if (SessionScheduler.rejARDTOIdSetMap != null) {
			SessionScheduler.rejARDTOIdSetMap.put(pSId, new ArrayList<String>());
		}

		// Owner BICs report
		if (SessionScheduler.ownerBICRepMap != null) {
			SessionScheduler.ownerBICRepMap.put(pSId, new HashMap<String, Double[]>());
		}
		
		// Owner BICs data
		if (SessionScheduler.ownerBICMap != null) {
			SessionScheduler.ownerBICMap.put(pSId, new HashMap<String, Double[]>());
		}
		
		// DTO Image Ids
		if (SessionScheduler.dtoImageIdMap != null) {
			SessionScheduler.dtoImageIdMap.put(pSId, new HashMap<String, Long>());
		}
		
		// Planned DLO List
		if (SessionScheduler.planDLOListMap != null) {
			SessionScheduler.planDLOListMap.put(pSId, new ArrayList<DLO>());
		}

		// Initial DTO List
		if ((DeltaPlanProcessor.initDTOListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.initDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}
		
		// Initial DTO Id List
		if ((DeltaPlanProcessor.initDTOIdListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.initDTOIdListMap.put(pSId, new ArrayList<String>());
		}

		
		// Current DTO List
		if ((DeltaPlanProcessor.currDTOListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.currDTOListMap.put(pSId, new ArrayList<ArrayList<SchedDTO>>());
		}
		
		// Delta-Plan DTO List
		if ((DeltaPlanProcessor.deltaSchedDTOListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.deltaSchedDTOListMap.put(pSId, new ArrayList<SchedDTO>());
		}
		
//		// Unavailable DTO List
//		if ((DeltaPlanProcessor.unavDTOIdListMap != null) && SessionChecker.isDeltaSession(pSId)) {
//			DeltaPlanProcessor.unavDTOIdListMap.put(pSId, new ArrayList<String>());
//		}
//		
//		// Delta Plan DTO List
//		if ((DeltaPlanProcessor.deltaSatVisTimeMap != null) && SessionChecker.isDeltaSession(pSId)) {
//			DeltaPlanProcessor.deltaSatVisTimeMap.put(pSId, new HashMap<String, Date>());
//		}

		// Current Plan Offset
		if ((DeltaPlanProcessor.currPlanOffsetTimeMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.currPlanOffsetTimeMap.put(pSId, new ArrayList<Long>());
		}

		// Delta plan cancelled DTOs
		if ((DeltaPlanProcessor.cancDTOIdListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.cancDTOIdListMap.put(pSId, new HashMap<String, ArrayList<String>>());
		}

		// Delta plan total cancelled DTOs
		if ((DeltaPlanProcessor.cancTotDTOIdListMap != null) && SessionChecker.isDelta(pSId)) {
			DeltaPlanProcessor.cancTotDTOIdListMap.put(pSId, new ArrayList<String>());
		}
		
		// DI2S Master SchedDTO
		if (EquivDTOHandler.di2sMasterSchedDTOMap != null) {
			EquivDTOHandler.di2sMasterSchedDTOMap.put(pSId, new HashMap<String, SchedDTO>());
		}
		
		// Di2s Slave SchedDTO
		if (EquivDTOHandler.di2sSlaveSchedDTOMap != null) {
			EquivDTOHandler.di2sSlaveSchedDTOMap.put(pSId, new HashMap<String, SchedDTO>());
		}

		 // Di2s slave DTO Id List
		 if (EquivDTOHandler.slaveDTOIdListMap != null) {
			 EquivDTOHandler.slaveDTOIdListMap.put(pSId, new ArrayList<String>());
		 }
		 
		 if (EquivDTOHandler.di2sLinkedIdsMap != null) {
			 EquivDTOHandler.di2sLinkedIdsMap.put(pSId, new HashMap<String, String>());
		 }
		 
		//
		// // Is DI2S Response
		// if (EquivDTOHandler.isDI2SResponseMap != null) {
		// EquivDTOHandler.isDI2SResponseMap.put(pSId, false);
		// }

		// Rejected Requests List
		if (FilterDTOHandler.filtRejReqListMap != null) {
			FilterDTOHandler.filtRejReqListMap.put(pSId, new ArrayList<RejectedRequest>());
		}

		// Rejected Requests DTO Id List
		if (FilterDTOHandler.filtRejDTOIdListMap != null) {
			FilterDTOHandler.filtRejDTOIdListMap.put(pSId, new ArrayList<String>());
		}

		// Waiting Filtering Result
		if (FilterDTOHandler.isWaitFiltResultMap != null) {
			FilterDTOHandler.isWaitFiltResultMap.put(pSId, false);
		}

		// Minimum owner Packet Store Id
		if (SessionScheduler.ownerMinPSIdMap != null) {			
			SessionScheduler.ownerMinPSIdMap.put(pSId, new HashMap<String, Long>());
		}
		
		// Minimum subscription Packet Store Id
		if (SessionScheduler.intMinPSIdMap != null) {			
			SessionScheduler.intMinPSIdMap.put(pSId, 0L);
		}
	}

	/**
	 * Get the owner Ids related to the given scheduled AR Id
	 *
	 * @param pSId
	 * @param schedARId
	 * @return the owner Ids related to the given scheduled AR Id
	 */
	public static ArrayList<String> getSchedAROwnerIdList(Long pSId, String schedARId) throws Exception {

		/**
		 * The output AR owner list
		 */
		ArrayList<String> aROwnerList = new ArrayList<>();

		if (!ownerListMap.get(pSId).isEmpty()) {

			for (int i = 0; i < ownerListMap.get(pSId).size(); i++) {

				if (!ownerARIdMap.get(pSId).isEmpty()) {

					if (ownerARIdMap.get(pSId).containsKey(
							ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId())) {

						// logger.debug("Associate AR Ids for owner: "
						// +
						// ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId());

						if (ownerARIdMap.get(pSId).get(ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId())
								.contains(schedARId)) {
							
							// Add AR Owner
							aROwnerList.add(ownerListMap.get(pSId).get(i).getCatalogOwner().getOwnerId());
						}
					}

				} else {

					logger.warn("The list of AR Ids is empty.");
				}
			}

		} else {

			logger.warn("The list of owners is empty.");
		}

		return aROwnerList;
	}	

}

/**
*
* MODULE FILE NAME: java
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.HPCivilianRequestHandler;
import com.telespazio.csg.spla.csps.handler.MessageHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.ARRankComparator;
import com.telespazio.csg.spla.csps.utils.BICCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.PRRankComparator;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.csg.spla.csps.utils.TUPCalculator;													
import com.telespazio.splaif.protobuf.NextARMessage.NextAR;
import com.telespazio.splaif.protobuf.PRListMessage.ManualPRList;
import com.telespazio.splaif.protobuf.PRListMessage.PRList;
import com.telespazio.splaif.protobuf.PRListMessage.PRList.ProgReqListInstance;
import com.telespazio.splaif.protobuf.PRListMessage.PRList.SubscribingProgReqListInstance;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRStatus;
import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.enums.SubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.enums.TaskStatus;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanSubscribingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.SubscribingProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.ekmlib.EKMLIB;

/**
 * The PRList processor class
 */
public class PRListProcessor {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(PRListProcessor.class);

	/**
	 * The new AR Size map
	 */
	public static HashMap<Long, Integer> newARSizeMap;

	/**
	 * The PRList map in the current MH
	 */
	public static HashMap<Long, ArrayList<ProgrammingRequest>> pRListMap;

	/**
	 * The PRList map in the reference MHs
	 */
	public static HashMap<Long, ArrayList<ProgrammingRequest>> refPRListMap;

	/**
	 * The PR to PRList Id map
	 */
	public static HashMap<Long, HashMap<String, ArrayList<String>>> pRToPRListIdMap;

	/**
	 * The subscribing PRList
	 */
	public static ArrayList<SubscribingProgrammingRequest> subscrPRList;

	/**
	 * The discarding PRList
	 */
	public static ArrayList<ProgrammingRequest> discardPRList;

	/**
	 * The discarding PR Id list map
	 */
	public static HashMap<Long, ArrayList<String>> discardPRIdListMap;

	/**
	 * The PR to scheduled Id map
	 */
	public static HashMap<Long, HashMap<String, ProgrammingRequest>> pRSchedIdMap;
	
	/**
	 * The Planning Session PR to scheduled Id map
	 */
	public static HashMap<Long, HashMap<String, ProgrammingRequest>> pSPRSchedIdMap;
	
	/**
	 * The working PR to scheduled Id map
	 */
	public static HashMap<Long, HashMap<String, ProgrammingRequest>> workPRSchedIdMap;

	/**
	 * The AR to scheduled Id map
	 */
	public static HashMap<Long, HashMap<String, AcquisitionRequest>> aRSchedIdMap;

	/**
	 * The DTO to scheduled Id map
	 */
	public static HashMap<Long, HashMap<String, DTO>> dtoSchedIdMap;

	/**
	 * The scheduled DTO map
	 */
	public static HashMap<Long, HashMap<String, SchedDTO>> schedDTOMap;

	/**
	 * The equivalent theatre requests map
	 */
	public static HashMap<Long, ArrayList<EquivalentDTO>> equivTheatreMap;

	/**
	 * The equivalent DTO Id maneuver map
	 */
	public static HashMap<Long, HashMap<String, Maneuver>> equivStartTimeManMap;
	
	/**
	 * The equivalent DTO start time Id map
	 */
	public static HashMap<Long, HashMap<String, String>> equivStartTimeIdMap;

	/**
	 * The equivalent DTO Id SchedDTO Id map
	 */
	public static HashMap<Long, HashMap<String, String>> equivIdSchedARIdMap;
	
	/**
	 * The replacing civilian PRList map
	 */
	public static HashMap<Long, ArrayList<ProgrammingRequest>> replPRListMap;

	/**
	 * The replacing civilian PRList map
	 */
	public static HashMap<Long, ArrayList<ProgrammingRequest>> crisisPRListMap;

	/**
	 * The map of scheduled AR Id ranks
	 */
	public static HashMap<Long, HashMap<String, Integer>> schedARIdRankMap;
	
	/**
	 * The PR to subscribed Id map
	 */
	public static HashMap<Long, HashMap<String, Boolean>> pRIntBoolMap;
	
	/**
	 * Import the incoming PRList
	 *
	 * @param pRList
	 * @param partnerId
	 * @return
	 */
	public boolean importPRList(PRList serPRList) {

		/**
		 * The output boolean
		 */
		boolean accepted = true;

		try {

			// 1.0 Process PRList

			/**
			 * The Planning Session Id
			 */
			Long pSId = serPRList.getPlanningSessionId();

			// 1.1. Get subscribing PRs from DHM
			setSubscribingPRs(pSId, serPRList);

			/**
			 * The partner Id
			 */
			String partnerId = "";
			
			if (serPRList.getProgReqListList() != null || serPRList.getProgReqListList().isEmpty()) {
				
				if (! SessionChecker.isFinal(pSId)) {
				
					partnerId = serPRList.getProgReqListList().get(0).getPartnerId();
				}
			}
				
			// 1.2. Check the PRList plannability
			accepted = checkPlanPRs(pSId, getInputPRList(pSId, serPRList), partnerId);
			
			// 1.3. Process PRs flagged as Crisis
			if (! SessionChecker.isCivilPP(pSId, partnerId)) {
			
				processCrisisPRs(pSId);
			}
			
			
		} catch (Exception e) {

			logger.error("Error processing PRList {} - {}", serPRList.getPlanningSessionId(), e.getMessage(), e);
			accepted = false;
		
		} finally {

			logger.info("PRList processing ended.");

		}

		return accepted;
	}

	/**
	 * Import the Very Urgent PRList
	 *
	 * @param pRList
	 * @param partnerId
	 * @return
	 */
	public boolean importVUPRList(PRList serPRList) {

		/**
		 * Instance handlers
		 */
		DeltaPlanProcessor deltaPlanProcessor = new DeltaPlanProcessor();

		/**
		 * The output boolean
		 */
		boolean accepted = true;

		/**
		 * The Planning Session Id
		 */
		Long pSId = serPRList.getPlanningSessionId();

		try {

			// 1.1. Set subscribing PRs from DHM
			setSubscribingPRs(pSId, serPRList);

			// 1.2. Check the PRList plannability
			if (checkPlanPRs(pSId, getInputPRList(pSId, serPRList), 
					serPRList.getProgReqListList().get(0).getPartnerId())) {

				// 1.3. Schedule VU Delta-Plan
				logger.info("Process VU Delta-Plan requests.");
				accepted = deltaPlanProcessor.processVUDeltaPlan(pSId, getInputPRList(pSId, serPRList));
			}
			
		} catch (Exception e) {

			logger.error("Error processing PRList {} - {}", pSId, e.getMessage(), e);
			accepted = false;
		
		} finally {

			logger.info("PRList processing ended.");

		}

		return accepted;
	}

	/**
	 * Import the Last Minute Planning PRList
	 *
	 * @param serPRList
	 * @param partnerId
	 * @return
	 */
	public boolean importLMPPRList(PRList serPRList) {

		/**
		 * Instance handlers
		 */
		DeltaPlanProcessor deltaPlanProcessor = new DeltaPlanProcessor();

		/**
		 * The output boolean
		 */
		boolean accepted = true;

		// 1.0. Process PRList

		/**
		 * The Planning Session Id
		 */
		Long pSId = serPRList.getPlanningSessionId();

		try {

			// 1.1. Get subscribing PRs from DHM
			setSubscribingPRs(pSId, serPRList);

			// 1.2. Check the PRList plannability
			if (checkPlanPRs(pSId, getInputPRList(pSId, serPRList), 
					serPRList.getProgReqListList().get(0).getPartnerId())) {

				// 1.3. Schedule LMP Delta-Plan
				logger.info("Process LMP Delta Plan requests.");
				accepted = deltaPlanProcessor.processLMPDeltaPlan(pSId, getInputPRList(pSId, serPRList));
			}

		} catch (Exception e) {

			logger.error("Error processing PRList {} - {}", pSId, e.getMessage(), e);
			accepted = false;
		
		} finally {

			logger.info("PRList processing ended.");
		}

		return accepted;
	}

	/**
	 * Import the manual planning PRList
	 *
	 * @param pSId
	 * @param serManPRList
	 * 
	 * @return
	 */
	public boolean importManualPRList(Long pSId, ManualPRList serManPRList) {

		/**
		 * Instance handlers
		 */
		ManualPlanProcessor manPlanProcessor = new ManualPlanProcessor();

		/**
		 * The output boolean
		 */
		boolean accepted = true;

		try {

			// No subscription detection

			// 1.1. Check the PRList plannability
			accepted = checkPlanPRs(pSId, getSelectedPRList(pSId, serManPRList), "0");

			// 1.2. Set the discarded PRs
			logger.info("Set discarded PRList...");
			setDiscardedPRs(pSId, serManPRList);

			// 1.3. Process Manual Replanning
			logger.info("Process Manual Replanning.");
			manPlanProcessor.processManualReplan(pSId, getSelectedPRList(pSId, serManPRList), 
					getInputDTOList(pSId));

		} catch (Exception e) {

			logger.error("Error processing PRList {} - {}", pSId, e.getMessage(), e);
			accepted = false;

		} finally {

			logger.info("PRList processing ended.");
		}

		return accepted;
	}

	/**
	 * Check the plannability of the PRs
	 * @param pSId
	 * @param newPRList
	 * @param partnerId
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private boolean checkPlanPRs(Long pSId, ArrayList<ProgrammingRequest> newPRList, String partnerId) 
			throws Exception {
		
		logger.info("Check PRList plannability for Planning Session:" + pSId);

		/**
		 * Instance handlers
		 */
		UnrankARListProcessor unrankARListProcessor = new UnrankARListProcessor();
		
		HPCivilianRequestHandler hpCivReqHandler = new HPCivilianRequestHandler();
		
		SessionScheduler sessionScheduler = new SessionScheduler();

		logger.info("Check new PRList plannability...");
		
		/**
		 * The output boolean
		 */
		boolean accepted = true;

		try {

			/**
			 * The list of unranked ARs
			 */
			ArrayList<SchedAR> unrankARList = new ArrayList<>();

			/**
			 * The plan PRList initialized as the previous PRList
			 */
			ArrayList<ProgrammingRequest> planPRList = new ArrayList<ProgrammingRequest>();

			if (SessionChecker.isManual(pSId)) {

				logger.debug("Manual PRList detected for Planning Session: " + pSId);

				planPRList = (ArrayList<ProgrammingRequest>) newPRList.clone();

			} else {
				
				logger.debug("New PRList detected for Planning Session: " + pSId);

				planPRList = (ArrayList<ProgrammingRequest>) pRListMap.get(pSId).clone();
			}

			// 1.1. Update theatre requests
			updateTheatreRequests(pSId, planPRList);

			for (int i = 0; i < newPRList.size(); i++) { 

				/**
				 * The new Programming Request
				 */
				ProgrammingRequest newPR = newPRList.get(i);
				
				logger.debug("Imported new PR: " + newPR.getProgrammingRequestId() 
					+ " of UGS: " + newPR.getUserList().get(0).getUgsId());			
				
//				// TODO: Erase!
//	            if (newPR.getProgrammingRequestId().equals("1163084")) {
//	            	
//	            	newPR.getUserList().get(0).setUgsId("220");
//	            	newPR.getUserList().get(0).setOwnerId("2200");
//	            }
				
				/**
				 * The scheduling PR Id
				 */
				String schedPRId = ObjectMapper.parseDMToSchedPRId(
						newPR.getUserList().get(0).getUgsId(), newPR.getProgrammingRequestId());
				
				// Add the new PR to the scheduling map
				pRSchedIdMap.get(pSId).put(schedPRId, newPR);
			
				// Add the new PR to the PS scheduling map				
				pSPRSchedIdMap.get(pSId).put(schedPRId, newPR);
				
				// Add the new PR to the working scheduling map				
				workPRSchedIdMap.get(pSId).put(schedPRId, newPR);
				
				// Add the PR international boolean to the scheduling map
				pRIntBoolMap.get(pSId).put(schedPRId, false);
				
				// Manage international PR
				if ((newPR.getUserList().size() > 1)
					&& RequestChecker.isDefence(newPR.getUgsId())) {
					
					logger.debug("Assigned international flag to PR: " + schedPRId);
					pRIntBoolMap.get(pSId).put(schedPRId, true);
				
				} else if (newPR.getDi2sAvailabilityFlag()
					&& RequestChecker.isDefence(newPR.getUgsId())) {
					
					logger.debug("Assigned international flag to PR: " + schedPRId);
					pRIntBoolMap.get(pSId).put(schedPRId, true);
				}
					
				if (newPR.getPitchExtraBIC() == null) {
					newPR.setPitchExtraBIC(Double.valueOf(0));
				}

				if (! RequestChecker.isReplacing(newPR)) {

					newARSizeMap.put(pSId, newARSizeMap.get(pSId) + newPR.getAcquisitionRequestList().size());

					// 1.2.1 Select PRs related to the session type
					if (SessionChecker.isUnranked(pSId)
							|| SessionChecker.isSelf(pSId)) {  // Unranked/Self session type

						if ((newPR.getRelativePriorityIndex() != null) && (newPR.getRelativePriorityIndex() >= 5)) {

							logger.debug("Add ARList of PR: " + ObjectMapper.parseDMToSchedPRId(
							newPR.getUserList().get(0).getUgsId(), newPR.getProgrammingRequestId()) 
							+ " to the unranked list.");

							pRSchedIdMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
									newPR.getUserList().get(0).getUgsId(), newPR.getProgrammingRequestId()), newPR);
							
							// Set the list of unranked ARs
							unrankARList.addAll(ObjectMapper.parseDMToSchedARList(pSId,
									newPR.getUserList().get(0).getUgsId(), newPR.getProgrammingRequestId(),
									newPR.getAcquisitionRequestList(),
									newPR.getUserList().get(0).getAcquisitionStationIdList(), 
									newPR.getPitchExtraBIC(), false));

						} else {

							logger.warn("For Unranked Routine PR: " + newPR.getProgrammingRequestId() +
									" the relative Priority Index results different from the expected value!");
							
							logger.warn("The PR: " + newPR.getProgrammingRequestId() + " of UGS Id : " 
							+ newPR.getUserList().get(0) + " is skipped.");
							
						}
						
						// 1.2.2 Add the Unranked PR to the session PRList
						planPRList.add(newPR);
						
					} else if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
							.equals(PlanningSessionType.InterCategoryRankedRoutine)
							|| (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
									.equals(PlanningSessionType.IntraCategoryRankedRoutine))) { // Ranked Routine Session type

							// 1.2.3. Add the Ranked Routine PR to the session PRList
							planPRList.add(newPR);
						
					} else { // HP/PP session type

						// 1.2.4. Add all the HP/PP PR to the session PRList
						planPRList.add(newPR);
					}

//					// 1.2.5. Build the intersection Matrix between the PRs DTOs
//					intMatrixCalculator.buildPRDTOIntMatrix(pSId, planPRList);

				} 
				else 
				{
					// 1.2.6. Handle Replacing PRs
					logger.info("PR " + ObjectMapper.parseDMToSchedPRId(newPR.getUserList().get(0).getUgsId(),
							newPR.getProgrammingRequestId()) + " has a positive replacing civilian flag.");
	
					// 1.2.7. Add the Ranked PR to the session PRList
					planPRList.add(newPR);

					// 1.2.8. Add the replacing ProgReq to the session Civil PRList
					replPRListMap.get(pSId).add(newPR);				
				}
					
				// 1.2.9 Handle Crisis PRs
				if (newPR.getType() != null && RequestChecker.isCrisis(newPR.getType())) {
					
					crisisPRListMap.get(pSId).add(newPR);
				}
			}

			// 1.3.Instance AR Ids to the partner list
			// Hp: 1 PRList for each Partner is identified

			if ((SessionActivator.ownerListMap.get(pSId) != null)
					|| SessionActivator.ownerListMap.get(pSId).isEmpty()) 
			{
				logger.debug("Instance the AR Ids relevant to the owner list.");

				for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {

					SessionActivator.ownerARIdMap.get(pSId).put(owner.getCatalogOwner().getOwnerId(),
							new ArrayList<String>());
				}
			} 
			else 
			{
				logger.warn("No owner list found!");
			}

			// 1.4. Process the complete PRList
			processCompletePRList(pSId, planPRList);

			// 1.5. Setup BRM partners
			RulesPerformer.setupBRMPartners(pSId);

			// 1.6. Initialize the PR statuses in the Planning Session with scheduled DTOs
			if (initPlanPRStatuses(pSId)) 
			{
				logger.info("Plannable Programming Requests are set.");
			} 
			else {
				logger.warn("No Programming Requests are plannable.");
				unrankARList.clear();
				
				if (SessionChecker.isFinal(pSId)) {
					unrankARList.clear();
				} else {
					logger.warn("Some problems raised during Programming Requests initialization.");			
				}
			}
			
			// 1.7. Initialize the Planning Session subscription PR status
			if (!SessionActivator.planSessionMap.get(pSId).getPlanningSessionType()
					.equals(PlanningSessionType.ManualPlanning)) 
			{
				logger.info("Initialize the list of the subscription PR statuses in the Planning Session.");

				if (initSubscribtionPRStatuses(pSId)) 
				{
					logger.info("Subscribing Programming Requests are set.");
				} 
				else 
				{
					logger.info("No Programming Requests are set as subscribable.");
				}
			}

			// 1.8. Manage the replacing of HP-Civilian/CRISIS requests
			if (!SessionChecker.isFinal(pSId) && SessionChecker.isCivilPP(pSId, partnerId) 
					&& ! replPRListMap.get(pSId).isEmpty()) {

				logger.debug("Manage the replacing of HP-Civilian/CRISIS requests.");
				hpCivReqHandler.handleHpCivilRequests(pSId);
			}
		
			logger.debug("Incoming PRList is parsed with a number of PRs: " + planPRList.size());

			// 2.0 Update PRList for the Planning Session
			pRListMap.put(pSId, (ArrayList<ProgrammingRequest>) planPRList.clone());

			try {
			
				if (!Configuration.debugEKMFlag) {
					
//					/**
//					 * Instance handlers
//					 */
//					EKMLIB ekmLib = new EKMLIB();
//		
//					// Check TKI
//					logger.info("Check PRList TKI by encryption...");
//					ekmLib.checkTKI((ArrayList<ProgrammingRequest>)pRListMap.get(pSId).clone(), (ArrayList<PlanProgrammingRequestStatus>)
//							((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
//									.getProgrammingRequestStatusList()).clone());
				}
				
			} catch (Exception ex) {
				
				logger.warn("Problems raised for TKI Encryption. " + ex.getMessage());
			}
			
			// 2.1 Evaluate input data
			evalInputData(pSId);

			// 2.2 Update owner BIC map
//			if (!SessionChecker.isDelta(pSId)) 
//			{

				updateOwnerBICs(pSId);
//			}

			if (SessionChecker.isUnranked(pSId)
					|| SessionChecker.isSelf(pSId)) {  // Unranked/self session type
			
				// 2.3 Process unranked ARs
				unrankARListProcessor.processUnrankARs(pSId, unrankARList);
				
				// 2.4 Update Progress Report
				sessionScheduler.setProgressReport(pSId);
			
			} else if (SessionChecker.isManual(pSId)) {
				
				// 2.5 Update Progress Report
				sessionScheduler.setProgressReport(pSId);
			}
		} 
		catch (Exception ex) 
		{
			accepted = false;
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		logger.info("The plannability of the incoming PRList is: " + accepted);
		
		return accepted;
	}
	
	/**
	 * Initialize the planning statuses for the PRList
	 *
	 * @param pSId
	 * @param planPRList
	 * @param schedDTOList
	 * @return
	 * @throws Exception 
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private boolean initPlanPRStatuses(Long pSId) throws Exception {
		
		/**
		 * Instance handler
		 */
		PersistPerformer persistPerformer = new PersistPerformer();
		
		TUPCalculator tupCalculator = new TUPCalculator();
		/**
		 * The output boolean
		 */
		boolean planAvail = false;
	
		try {
		
			logger.info("Initialize the PR statuses in the Planning Session.");
			
			/**
			 * The list of working PlanPRStatus Ids
			 */
			ArrayList<String> workPlanPRStatusIdList = new ArrayList<String>(); 	
			
			if (SessionActivator.workPSIdMap.get(pSId) != null && SessionActivator.workPSIdMap.get(pSId) > 0) {
				
				logger.info("Added Plan PR statuses of working PRs.");
				
				List<PlanProgrammingRequestStatus> workPlanPRStatusList = persistPerformer
						.getPlanPRStatusList(SessionActivator.workPSIdMap.get(pSId));
				
				// Get the full list of Programming Request Statuses
				SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().addAll(
						workPlanPRStatusList);
				
				for (PlanProgrammingRequestStatus workPlanPRStatus : workPlanPRStatusList) {
					
					logger.debug("Get Planning Status for PR: "  + workPlanPRStatus.getProgrammingRequestId());
					
					workPlanPRStatusIdList.add(ObjectMapper.parseDMToSchedPRId(
							workPlanPRStatus.getUgsId(), workPlanPRStatus.getProgrammingRequestId()));
				}
			}
			
			/**
			 * The input PRs of the Mission Horizon 
			 */
			ArrayList<ProgrammingRequest> pSPRList = (ArrayList<ProgrammingRequest>) getPlanSessionPRList(pSId).clone();
			
			/**
			 * The list of scheduled DTO Ids
			 */
			ArrayList<String> schedDTOIdList = RulesPerformer.getPlannedDTOIds(pSId);
			
			// Cycle for the scheduling PRs of the incoming PRList
			for (int i = 0; i < pSPRList.size(); i++) {
								
				/**
				 * The Plan Programming Request status
				 */
				PlanProgrammingRequestStatus pRStatus = new PlanProgrammingRequestStatus(
						pSPRList.get(i).getUserList().get(0).getUgsId(), 
						pSPRList.get(i).getProgrammingRequestListId(),
						pSPRList.get(i).getProgrammingRequestId(), PRStatus.Rejected,
						pSPRList.get(i).getReplacingCivilianRequestFlag(),
						pSPRList.get(i).getUserList().get(0).getOwnerId());
					
				if (! workPlanPRStatusIdList.contains(ObjectMapper.parseDMToSchedPRId(
						pRStatus.getUgsId(), pRStatus.getProgrammingRequestId()))) {
				
					/**
					 * The list of acquisition requests
					 */
					List<AcquisitionRequest> aRList = pSPRList.get(i).getAcquisitionRequestList();
		
					for (int j = 0; j < aRList.size(); j++) {
		
						/**
						 * The Plan Acquisition Request status
						 */
						PlanAcquisitionRequestStatus aRStatus = new PlanAcquisitionRequestStatus(
								aRList.get(j).getAcquisititionRequestId(), AcquisitionRequestStatus.New);
		
						/**
						 * The list of DTOs
						 */
						List<DTO> dtoList = aRList.get(j).getDtoList();
		
						/**
						 * The request availability
						 */
						boolean reqAvail = false;
		
						for (int k = 0; k < dtoList.size(); k++) {
		
							/**
							 * The Plan DTO status
							 */
							PlanDtoStatus dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Unused);
		
							// 1.1 Check Mission Horizon bounds integrity
							if (! RequestChecker.isInsideMH(pSId, dtoList.get(k))) {
		
								logger.warn("The DTO of " + dtoList.get(k).getDtoId() + " of AR "
										+ aRStatus.getAcquisitionRequestId() + " of PR " + pRStatus.getProgrammingRequestId()
										+ " of UGS " + pRStatus.getUgsId() + " from: " + dtoList.get(k).getStartTime() 
										+ " to: " + dtoList.get(k).getStopTime() + " is rejected because "
										+ "outside the relevant Mission Horizon!");
		
								// Set conflict status
								// TODO: check with Ground!
								dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Rejected);
								dtoStatus.setConflictDescription("DTO outside the relevant Mission Horizon.");
								dtoStatus.setConflictReasonId(0); // TODO: finalize conflict reason
								dtoStatus.setActualBic(0.0);
							
							// Added on 14/05/2021 for the S-TUP  management
							} else if (SessionActivator.ugsIsTUPMap.get(pSId).get(pSPRList.get(i).getUserList().get(0).getUgsId())							
								&& ! tupCalculator.isActiveTUP(pSId, pSPRList.get(i).getUserList().get(0).getUgsId())) {
								
								dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Rejected);
								dtoStatus.setConflictDescription("Related S-TUP is NOT active.");
								dtoStatus.setConflictReasonId(0); // TODO: finalize conflict reason
								dtoStatus.setActualBic(0.0);

							} else if (SessionActivator.ugsIsTUPMap.get(pSId).get(pSPRList.get(i).getUserList().get(0).getUgsId())													
										&& ! tupCalculator.isValidTUPStation(pSId, pSPRList.get(i).getUserList().get(0).getUgsId(),
												pSPRList.get(i).getUserList().get(0).getAcquisitionStationIdList())) {	
								
								dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Rejected);
								dtoStatus.setConflictDescription("Related S-TUP acquisition station is NOT valid.");
								dtoStatus.setConflictReasonId(0); // TODO: finalize conflict reason
								dtoStatus.setActualBic(0.0);

							} else if (SessionActivator.ugsIsTUPMap.get(pSId).get(pSPRList.get(i).getUserList().get(0).getUgsId())													
									&& ! tupCalculator.isAvailableTUP(pSId, pSPRList.get(i).getUserList().get(0).getUgsId())) {

								dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Rejected);
								dtoStatus.setConflictDescription("Related S-TUP is NOT available.");
								dtoStatus.setConflictReasonId(0); // TODO: finalize conflict reason
								dtoStatus.setActualBic(0.0);
							}
							
							if (schedDTOIdList.contains(ObjectMapper.parseDMToSchedDTOId(
									pRStatus.getUgsId(), pRStatus.getProgrammingRequestId(), 
									aRStatus.getAcquisitionRequestId(), dtoStatus.getDtoId()))) {
								
								pRStatus.setStatus(PRStatus.Scheduled);
								aRStatus.setStatus(AcquisitionRequestStatus.Scheduled);
								dtoStatus.setStatus(DtoStatus.Scheduled);
								dtoStatus.setActualBic(BICCalculator.getWorkDTOActualBIC(pSId, pRStatus.getUgsId(), 
										pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(), 
										dtoStatus.getDtoId()));
							}
		
							aRStatus.addDtoStatus(dtoStatus);
		
							if (!dtoStatus.getStatus().equals(DtoStatus.Rejected)) {
		
								reqAvail = true;
							}
						}
		
						if (!reqAvail) {
		
							aRStatus.setStatus(AcquisitionRequestStatus.Rejected);
		
						} else {
		
							planAvail = true;
						}
						
						aRStatus.setActualBic(BICCalculator.getWorkARActualBIC(pSId, pRStatus.getUgsId(), 
								pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId()));
		
						pRStatus.addAcquisitionRequestStatus(aRStatus);
					}
						
					pRStatus.setActualBic(BICCalculator.getWorkPRActualBIC(pSId, pRStatus.getUgsId(), 
							pRStatus.getProgrammingRequestId()));
		
					logger.info("Added Plan PR status of new PR: " + pRStatus.getProgrammingRequestId() 
						+ " for UGS: " + pRStatus.getUgsId());
					
					// Add the Plan PR status to the list
					SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().add(pRStatus);
				}
			}
			
			if (pSPRList.isEmpty()) {
				planAvail = true;
			}
								
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
	
			logger.warn("Some requests are NOT correctly initialized.");
			
			return false;
		}
	
		return planAvail;
	}

	/**
	 * Initialize the plan statuses for the discarded PRList
	 *
	 * @param pSId
	 * @param planPRList
	 * @return
	 * @throws IOException
	 */
	/**
	 * @param pSId
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void initDiscardedPRStatuses(Long pSId) throws IOException {
		
		// Cycle for the scheduling PRs of the incoming PRList
		// TODO: TBD if to check working PRList statuses from DPL
		for (int i = 0; i < discardPRList.size(); i++) {

			/**
			 * The Plan Programming Request status
			 */
			PlanProgrammingRequestStatus pRStatus = new PlanProgrammingRequestStatus(
					discardPRList.get(i).getUserList().get(0).getUgsId(),
					discardPRList.get(i).getProgrammingRequestListId(), discardPRList.get(i).getProgrammingRequestId(),
					PRStatus.Cancelled, discardPRList.get(i).getReplacingCivilianRequestFlag(),
					discardPRList.get(i).getUserList().get(0).getOwnerId());

			/**
			 * The list of acquisition requests
			 */
			List<AcquisitionRequest> aRList = discardPRList.get(i).getAcquisitionRequestList();

			for (int j = 0; j < aRList.size(); j++) {

				/**
				 * The Plan Acquisition Request status
				 */
				PlanAcquisitionRequestStatus aRStatus = new PlanAcquisitionRequestStatus(
						aRList.get(j).getAcquisititionRequestId(), AcquisitionRequestStatus.Cancelled);

				/**
				 * The list of DTOs
				 */
				List<DTO> dtoList = aRList.get(j).getDtoList();

				/**
				 * The request availability
				 */
				boolean reqAvail = false;

				for (int k = 0; k < dtoList.size(); k++) {

					/**
					 * The Plan DTO status
					 */
					// TODO: check with Ground!
					PlanDtoStatus dtoStatus = new PlanDtoStatus(dtoList.get(k).getDtoId(), DtoStatus.Cancelled);
					dtoStatus.setConflictDescription("DTO discarded by manual replanning.");
					dtoStatus.setConflictReasonId(99); // TODO: finalize conflict reason
					dtoStatus.setActualBic(0.0);

					aRStatus.addDtoStatus(dtoStatus);

					if (dtoStatus.getStatus().equals(DtoStatus.Scheduled)) {

						reqAvail = true;
					}
				}

				if (!reqAvail) {

					aRStatus.setStatus(AcquisitionRequestStatus.Cancelled);

				}

				pRStatus.addAcquisitionRequestStatus(aRStatus);
			}

			logger.info("Added Plan PR status of discarded PR: " + pRStatus.getProgrammingRequestId() 
					+ " for UGS: " + pRStatus.getUgsId());
			
			// Add the Plan PR status to the list
			SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().add(pRStatus);
		}

		if (!Configuration.debugEKMFlag) {
	
			try {
				/**
				 * Instance handlers
				 */
				EKMLIB ekmLib = new EKMLIB(true);
	
				// Check TKI
				logger.info("Check TKI by encryption...");
				ekmLib.checkTKI((ArrayList<ProgrammingRequest>) pRListMap.get(pSId).clone(), 
						(ArrayList<PlanProgrammingRequestStatus>)
						((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
								.getProgrammingRequestStatusList()).clone());
	
				// // TODO: add DTO to rejection Ids

			} catch (Exception ex) {

				logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
			}
		}
	}

	/**
	 * Get the planning PRList
	 *
	 * @param pSId
	 * @param pRList
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<ProgrammingRequest> getInputPRList(Long pSId, PRList pRList)
			throws ClassNotFoundException, IOException {

		/**
		 * The new PRList
		 */
		ArrayList<ProgrammingRequest> newPRList = new ArrayList<>();

		for (ProgReqListInstance pRListInst : pRList.getProgReqListList()) {

			/**
			 * The byteString PRList
			 */
			ByteString bsPRList = pRListInst.getProgrammingRequest();

			/**
			 * The new scheduling PRList
			 */
			newPRList.addAll(
					(ArrayList<ProgrammingRequest>) MessageHandler.deserializeByteArray(bsPRList.toByteArray()));

			logger.debug("Imported PRList: " + newPRList.toString());
		}

		return newPRList;
	}

	/**
	 * Get the selected PRList
	 *
	 * @param pSId
	 * @param serManPRList
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<ProgrammingRequest> getSelectedPRList(Long pSId, ManualPRList serManPRList)
			throws ClassNotFoundException, IOException {

		/**
		 * The new PRList
		 */
		ArrayList<ProgrammingRequest> selPRList = new ArrayList<>();

		/**
		 * The byteString PRList
		 */
		ByteString bsPRList = serManPRList.getSelectedPRs();

		/**
		 * The new scheduling PRList
		 */
		selPRList.addAll((ArrayList<ProgrammingRequest>) MessageHandler.deserializeByteArray(bsPRList.toByteArray()));

		logger.debug("Imported Selected PRList: " + selPRList.toString());

		return selPRList;

	}

	/**
	 * Get the discarded PRList
	 *
	 * @param pSId
	 * @param serManPRList
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private void setDiscardedPRs(Long pSId, ManualPRList serManPRList) throws ClassNotFoundException, IOException {

		// 1.2.1. Get discarded PRList from DHM
		discardPRList = new ArrayList<>();

		/**
		 * The byteString pRList
		 */
		ByteString bsPRList = serManPRList.getDiscardedPRs();

		/**
		 * The subscribing PRList
		 */
		ArrayList<ProgrammingRequest> pRList = (ArrayList<ProgrammingRequest>) MessageHandler
				.deserializeByteArray(bsPRList.toByteArray());

		discardPRList.addAll((ArrayList<ProgrammingRequest>) pRList.clone());

		for (ProgrammingRequest discPR : discardPRList) {

			/**
			 * The list of PR Ids
			 */
			ArrayList<String> pRIdList = new ArrayList<String>();
			
			pRIdList.add(discPR.getProgrammingRequestListId());
			
			// Update pRList to PR map
			pRToPRListIdMap.get(pSId).put(
					ObjectMapper.parseDMToSchedPRId(discPR.getUserList().get(0).getUgsId(), 
							discPR.getProgrammingRequestId()), pRIdList);

			discardPRIdListMap.get(pSId).add(
					ObjectMapper.parseDMToSchedPRId(discPR.getUserList().get(0).getUgsId(), 
							discPR.getProgrammingRequestId()));
		}

		// 1.2.2. Initialize statuses of the discarded PRs
		initDiscardedPRStatuses(pSId);

		// Add discarded PRList
		pRListMap.get(pSId).addAll((ArrayList<ProgrammingRequest>) discardPRList.clone());
	}

	/**
	 * Set the subscribing PRs
	 *
	 * @param pSId
	 * @param pRList
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private void setSubscribingPRs(Long pSId, PRList serPRList) throws ClassNotFoundException, IOException {

		logger.info("Set existing subscribing PRs...");
		
		// 1.2.1. Get subscribing PRList from DHM
		subscrPRList = new ArrayList<>();

		for (SubscribingProgReqListInstance PRListInst : serPRList.getSubscrProgReqListList()) {

			/**
			 * The byteString pRList
			 */
			ByteString bsPRList = PRListInst.getSubscribingProgrammingRequest();

			/**
			 * The subscribing PRList
			 */
			ArrayList<SubscribingProgrammingRequest> subPRList = (ArrayList<SubscribingProgrammingRequest>) MessageHandler
					.deserializeByteArray(bsPRList.toByteArray());

			subscrPRList.addAll((ArrayList<SubscribingProgrammingRequest>) subPRList.clone());
		}
	}
	
	/**
	 * Process the Crisis PRs
	 * 
	 * @param pSId
	 */
	@SuppressWarnings("unchecked")
	private void processCrisisPRs(Long pSId) {
		
		/**
		 * Instance handlers
		 */
		NextARProcessor nextARProcessor = new NextARProcessor();
		
		ArrayList<ProgrammingRequest> crisisPRList = (ArrayList<ProgrammingRequest>) crisisPRListMap.get(pSId).clone();
		
		/**
		 * Sort Crisis PRs based on the PR rank
		 */
		Collections.sort(crisisPRList, new PRRankComparator());
		
		
		for (ProgrammingRequest crisisPR : crisisPRList) {
						
			logger.info("Process Crisis PR: " + crisisPR.getProgrammingRequestId() 
			+ " for UGS: " + crisisPR.getUserList().get(0).getUgsId());
	
			/**
			 * The list of Crisis ARs to be sorted
			 */
			ArrayList<AcquisitionRequest> crisisARList = (ArrayList<AcquisitionRequest>) crisisPR.getAcquisitionRequestList();
			
			Collections.sort(crisisARList, new ARRankComparator());		
					
			// Plan the Crisis PR
			for (AcquisitionRequest aR : crisisARList) {
			
				/**
				 * The internal NextAR builder
				 */
				NextAR.Builder nextARBuilder = NextAR.newBuilder().setPlanningSessionId(pSId)
						.setUgsId(crisisPR.getUserList().get(0).getUgsId())
						.setProgRedId(crisisPR.getProgrammingRequestId())
						.setArId(aR.getAcquisititionRequestId())
						.setMaxProcessingTime(9999);						
					
				// Process Crisis AR
				nextARProcessor.processNextAR(nextARBuilder.build(), true);
			}
		}
	}
	
	/**
	 * Process the complete PRList in the Mission Horizon
	 * @param pSId
	 * @param planPRList
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	private void processCompletePRList(Long pSId, ArrayList<ProgrammingRequest> planPRList) throws Exception 
	{
		
		logger.debug("Process the complete set of " + planPRList.size() + " PRs for Planning Session: " + pSId);
		
		/**
		 * The list of AR Ids
		 */
		ArrayList<String> aRIdList = new ArrayList<>();

		logger.info("Incoming PR Ids to be scheduled: ");

		// 1.4. Process complete PRList
		for (ProgrammingRequest planPR : planPRList) 
		{
			
			logger.info("Input PR " + planPR.getProgrammingRequestId() 
			+ " of PRList " + planPR.getProgrammingRequestListId()
			+ " for UGS " + planPR.getUserList().get(0).getUgsId());
			
			/**
			 * The list of PR Ids
			 */
			ArrayList<String> pRIdList = new ArrayList<String>();
			
			pRIdList.add(planPR.getProgrammingRequestListId());
			
			// Update pRList ugsId map
			pRToPRListIdMap.get(pSId).put(
					ObjectMapper.parseDMToSchedPRId(planPR.getUserList().get(0).getUgsId(), 
							planPR.getProgrammingRequestId()), pRIdList);

			if (planPR.getPitchExtraBIC() == null) 
			{
				planPR.setPitchExtraBIC(Double.valueOf(0));
			}

			/**
			 * The PR Id
			 */
			String pRId = planPR.getProgrammingRequestId();

			// Update pRId map
			pRSchedIdMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
					planPR.getUserList().get(0).getUgsId(), pRId), planPR);

			aRIdList.clear();

			// 1.4.1. Get the relevant AR Ids related to the involved
			// Partners
			for (AcquisitionRequest aR : planPR.getAcquisitionRequestList()) 
			{

				/**
				 * The AR Id
				 */
				String aRId = aR.getAcquisititionRequestId();

				// Update AR Ids map
				aRSchedIdMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
						planPR.getUserList().get(0).getUgsId(), pRId, aRId), aR);

				// 1.4.2. Get the relevant AR Ids related to the involved
				// Partners
				for (DTO dto : aR.getDtoList()) 
				{

					String dtoId = dto.getDtoId();

					dtoSchedIdMap.get(pSId).put(ObjectMapper
							.parseDMToSchedDTOId(planPR.getUserList().get(0).getUgsId(), pRId, aRId, dtoId), dto);
				
					schedDTOMap.get(pSId).put(ObjectMapper
							.parseDMToSchedDTOId(planPR.getUserList().get(0).getUgsId(), pRId, aRId, dtoId), 
							ObjectMapper.parseDMToSchedDTO(pSId, planPR.getUserList().get(0).getUgsId(),  
									pRId, aRId, dto, planPR.getUserList().get(0).getAcquisitionStationIdList(), 
											false));
				}

				/**
				 * The scheduled AR
				 */
				SchedAR schedAR = ObjectMapper.parseDMToSchedAR(pSId, planPR.getUserList().get(0).getUgsId(),
						planPR.getProgrammingRequestId(), aR, 
						planPR.getUserList().get(0).getAcquisitionStationIdList(), planPR.getPitchExtraBIC(), false);

				aRIdList.add(schedAR.getARId());

				logger.trace("For proprietary owner " + planPR.getUserList().get(0).getOwnerId()
						+ " the following AR Id is internally associated: " + schedAR.getARId());
			}

			logger.trace("Set the list of a number of AR Ids " + aRIdList.size() + " for the owner: "
					+ planPR.getUserList().get(0).getOwnerId());

			// 1.4.3. Update the owner Id map
			if (SessionActivator.ownerARIdMap.get(pSId).get(planPR.getUserList().get(0).getOwnerId()) != null) 
			{

				SessionActivator.ownerARIdMap.get(pSId).get(planPR.getUserList().get(0).getOwnerId())
						.addAll((ArrayList<String>) aRIdList.clone());
			} 
			else 
			{
				logger.warn("Null AR Ids found for owner: " 
				+ planPR.getUserList().get(0).getOwnerId());
			}

		}
	}

	/**
	 * Initialize the list of subscribed PR status for the PRList
	 *
	 * @param pSId
	 * @param ugsId
	 */
	private boolean initSubscribtionPRStatuses(Long pSId) 
	{

		/**
		 * The output boolean
		 */
		boolean subscrAvail = false;

		// Cycle for the subscribing PRs of the incoming PRList
		for (int i = 0; i < subscrPRList.size(); i++) 
		{

			logger.info("Subscribing Requests found: " + subscrPRList.get(i).getProgrammingRequestId());

			/**
			 * The Plan Subscribing Request status
			 */
			PlanSubscribingRequestStatus subPRStatus = new PlanSubscribingRequestStatus(subscrPRList.get(i).getUgsId(),
					subscrPRList.get(i).getProgrammingRequestListId(), subscrPRList.get(i).getProgrammingRequestId(),
					subscrPRList.get(i).getAcquisitionRequestId(), subscrPRList.get(i).getDtoId(),
					SubscribingRequestStatus.NotScheduledBySubscription, "");

			// Add the status to the list
			SessionActivator.planSessionMap.get(pSId).getSubscribingRequestStatusList().add(subPRStatus);

			subscrAvail = true;
		}

		return subscrAvail;
	}
	
	/**
	 * Evaluate input DTO and Equivalent DTO data
	 *
	 * @param pSId
	 */
	@SuppressWarnings("unchecked")
	private void evalInputData(Long pSId) throws Exception 
	{

		logger.debug("Evaluate Equivalent DTO in the imported PRList.");

		/**
		 * The PR counter
		 */
		int pRCount = 0;
		
		/**
		 * The maneuver counter
		 */
		int manCount = 0;

		for (ProgrammingRequest pR : (ArrayList<ProgrammingRequest>) pRListMap.get(pSId).clone()) {

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

				if (aR.getEquivalentDTO() != null) { 
	
					// Add theatre/experimental EquivalentDTO
					if ((pR.getMode().equals(PRMode.Theatre) || pR.getMode().equals(PRMode.Experimental))) {

						/**
						 * The equivalent DTO Id
						 */
						String equivDTOId = aR.getEquivalentDTO().getEquivalentDtoId();
					
						if (aR.getEquivalentDTO().getTaskList() != null 
								&& aR.getEquivalentDTO().getTaskList().size() > 0) {
							
							for (Task man : aR.getEquivalentDTO().getTaskList()) {
	
								// Set default Task data
								try {
									
									if (man.getSatelliteId() == null) {
										man.setSatelliteId(aR.getDtoList().get(0).getSatelliteId());
									}
		
									if (man.getTaskId() == null) {
										man.setTaskId(BigDecimal.valueOf(Double.valueOf(pSId.toString() + manCount)));
									}
								
								} catch (Exception ex) {
									
									logger.warn("Invalid set for task!"); 
									man.setTaskId(BigDecimal.ZERO);
									man.setSatelliteId("SSAR1");
								}
								
								logger.debug("Associate maneuver for Equivalent DTO: " + equivDTOId);
								
								// Add Equivalent DTO Id
								equivStartTimeIdMap.get(pSId).put(Long.toString(man.getStartTime().getTime()), 
										aR.getEquivalentDTO().getEquivalentDtoId());
	
								// Add Equivalent Maneuver
								equivStartTimeManMap.get(pSId).put(Long.toString(man.getStartTime().getTime()),
										 (Maneuver) man);
								
								// Add Equivalent Scheduling AR Id
								equivIdSchedARIdMap.get(pSId).put(aR.getEquivalentDTO().getEquivalentDtoId(),
										ObjectMapper.parseDMToSchedARId(pR.getUgsId(), pR.getProgrammingRequestId(), 
												aR.getAcquisititionRequestId()));
																
								// Add pitch intervals
								if (((Maneuver) man).retrievePitchIntervals() != null
										&& !((Maneuver) man).retrievePitchIntervals()
										.getPitchIntervalDetailsList().isEmpty()) {
									
									PersistPerformer.pitchIntervalMap.put(Long.toString(
											man.getStartTime().getTime()), ((Maneuver) man).retrievePitchIntervals());
									
									logger.debug("Added Pitch Intervals in Map from Maneuver: " + man.getTaskId());
								
								} else  {
									
									logger.debug("No Pitch Intervals added in Map from Maneuver: " + man.getTaskId());
								}
					
								// Set task data
								man.setTaskStatus(TaskStatus.Unchecked);
																						
								// Update counter
								manCount ++;							 
							} 
						}
					
					// Add DI2S EquivalentDTO
					} else if (pR.getMode().equals(PRMode.DI2S)) {
						
						// Add Equivalent Scheduling AR Id
						equivIdSchedARIdMap.get(pSId).put(aR.getEquivalentDTO().getEquivalentDtoId(),
								ObjectMapper.parseDMToSchedARId(pR.getUgsId(), pR.getProgrammingRequestId(), 
										aR.getAcquisititionRequestId()));
					}
					
					// Add scheduling AR Id
					aRSchedIdMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(pR.getUserList().get(0).getUgsId(), 
							pR.getProgrammingRequestId(), aR.getAcquisititionRequestId()), aR);
				}
			}
			
			// Add scheduling PR Id			
			pRSchedIdMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(), 
					pR.getProgrammingRequestId()), pR);
			
			pRListMap.get(pSId).set(pRCount, pR);
			
			pRCount ++;
		}
		
		logger.info("A number of Equivalent maneuvers " + manCount + " has been associated for Planning Session " + pSId);

	}

	/**
	 * Get the number of scheduled ARs
	 *
	 * @param pSId
	 */
	public static int getSchedARNumber(Long pSId) throws Exception {

		/**
		 * The number of scheduled
		 */
		int schedARNum = 0;

		/**
		 * The list of PR statuses
		 */
		List<PlanProgrammingRequestStatus> pRStatusList = SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList();

		for (int i = 0; i < pRStatusList.size(); i++) {

			/**
			 * The list of AR statuses
			 */
			List<PlanAcquisitionRequestStatus> aRStatusList = pRStatusList.get(i).getAcquisitionRequestStatusList();

			for (int j = 0; j < aRStatusList.size(); j++) {

				if (aRStatusList.get(j).getStatus().equals(AcquisitionRequestStatus.Scheduled)) {

					schedARNum ++;
				}
			}
		}

		return schedARNum;
	}

	/**
	 * Update the owner BIC map at the Planning Session start
	 *
	 * @param pSId
	 */
	private void updateOwnerBICs(Long pSId) throws Exception {

		logger.info("Update owner BICs for Planning Session: " + pSId);
		
		/**
		 * The owner BIC map
		 */	
		HashMap<String, Double[]> ownerBICSessMap = new HashMap<>();

		/**
		 * The owner BIC array
		 */	
		Double[][] ownerBICs = new Double[SessionActivator.ownerListMap.get(pSId).size()][2];

		for (int i = 0; i < SessionActivator.partnerListMap.get(pSId).size(); i++) {

			ownerBICs[i][0] = SessionActivator.partnerListMap.get(pSId).get(i).getPremBIC();

			ownerBICs[i][1] = SessionActivator.partnerListMap.get(pSId).get(i).getRoutBIC();

			ownerBICSessMap.put(SessionActivator.ownerListMap.get(pSId).
					get(i).getCatalogOwner().getOwnerId(), ownerBICs[i]);

		}

		// Update owner BIC map
		SessionScheduler.ownerBICMap.put(pSId, ownerBICSessMap);
	}

	/**
	 * Update the Equivalent Theatre requests
	 *
	 * @param pSId
	 * @param planPRList
	 */
	private void updateTheatreRequests(Long pSId, ArrayList<ProgrammingRequest> planPRList) throws Exception {

		// Check theatre request
	
		for (ProgrammingRequest pR : planPRList) {

			// Check PRMode
			if (pR.getMode().equals(PRMode.Theatre) || pR.getMode().equals(PRMode.Theatre)) {

				for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

					if (aR.getEquivalentDTO() != null) {

						// Add equivalent theatre
						equivTheatreMap.get(pSId).add(aR.getEquivalentDTO());
					}
				}
			}
		}
	}

	/**
	 * Get the input DTOList
	 * 
	 * @param pSId
	 * @return
	 */
	private ArrayList<SchedDTO> getInputDTOList(Long pSId) {

		/**
		 * The outcoming list of scheduling DTOs
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<SchedDTO>();

		/**
		 * The entry set iterator
		 */
		Iterator<Entry<String, SchedDTO>> it = schedDTOMap.get(pSId).entrySet().iterator();

		while (it.hasNext()) {

			schedDTOList.add(it.next().getValue());
		}

		return schedDTOList;
	}
	
	/**
	 * Get the PR statuses for the given Planning Session 
	 * @param pSId
	 * @return
	 */
	public ArrayList<ProgrammingRequest> getPlanSessionPRList(Long pSId) {
		
		/**
		 * The Programming Request list
		 */
		ArrayList<ProgrammingRequest> pRList = new ArrayList<ProgrammingRequest>();
		
		/**
		 * The entry set iterator
		 */
		Iterator<Entry<String, ProgrammingRequest>> it = pRSchedIdMap.get(pSId).entrySet().iterator();

		while (it.hasNext()) {
		
			ProgrammingRequest pR = it.next().getValue();
			
			if (pSPRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
					pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId()))) {
				
				pRList.add(pR);
			}
		}
		
		return pRList;
	}
	
	/**
	 * Get the PR statuses for the given Planning Session 
	 * @param pSId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<PlanProgrammingRequestStatus> getPlanSessionPRStatuses(Long pSId) {
		
		/**
		 * The Plan Programming Request Status list
		 */
		ArrayList<PlanProgrammingRequestStatus> pRStatusList = new ArrayList<PlanProgrammingRequestStatus>();
		
		for (PlanProgrammingRequestStatus planPRStatus : (ArrayList<PlanProgrammingRequestStatus>)
				((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()).clone()) {
		
			if (pSPRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
					planPRStatus.getUgsId(), planPRStatus.getProgrammingRequestId()))) {
				
				// Add Plan PR Status
				pRStatusList.add(planPRStatus);
			}
		}
		
		return pRStatusList;
	}
	
	/**
	 * Get the working PR statuses for the given MH 
	 * @param pSId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<PlanProgrammingRequestStatus> getWorkPRStatuses(Long pSId) {
		
		/**
		 * The Plan Programming Request Status list
		 */
		ArrayList<PlanProgrammingRequestStatus> pRStatusList = new ArrayList<PlanProgrammingRequestStatus>();
		
		for (PlanProgrammingRequestStatus planPRStatus : (ArrayList<PlanProgrammingRequestStatus>)
				((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()).clone()) {
		
			if (workPRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
					planPRStatus.getUgsId(), planPRStatus.getProgrammingRequestId()))) {
				
				// Add Plan PR Status
				pRStatusList.add(planPRStatus);
			}
		}
		
		return pRStatusList;
	}
	
	/**
	 * Get subscribing PR statuses of the given Planning Session 
	 * @param pSId
	 * @return
	 */
	public List<PlanSubscribingRequestStatus> getPlanSessionSubPRStatuses(Long pSId) {
		
		/**
		 * The output Plan Subscribing Programming Request Status list
		 */
		ArrayList<PlanSubscribingRequestStatus> subPRStatusList = new ArrayList<PlanSubscribingRequestStatus>();
		
		for (PlanSubscribingRequestStatus subPRStatus : (ArrayList<PlanSubscribingRequestStatus>)
				((ArrayList<PlanSubscribingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
				.getSubscribingRequestStatusList())) {
		
			if (pSPRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
					subPRStatus.getUgsId(), subPRStatus.getProgrammingRequestId()))) {
				
				// Add Plan Subscribed PR Status
				subPRStatusList.add(subPRStatus);
			}
		}
		
		return subPRStatusList;
	}

}

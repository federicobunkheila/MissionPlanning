/**
 *
 * MODULE FILE NAME: DeltaPlanProcessor.java
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

//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.nais.spla.brm.library.main.ontology.resources.ReasonOfRejectElement;
import com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO;
import com.nais.spla.brm.library.main.ontology.utils.ElementsInvolvedOnOrbit;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SchedDTODeltaTimeComparator;
import com.telespazio.csg.spla.csps.utils.SchedDTOTimeComparator;
import com.telespazio.csg.spla.csps.utils.VisTimeComparator;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.ReplacedProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

/**
 * The the Delta-Plan processing class for the incoming Very Urgent and Last
 * Minute Planning requests.
 *
 */
public class DeltaPlanProcessor {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(DeltaPlanProcessor.class);

	/**
	 * The map of the initial DTO list
	 */
	public static HashMap<Long, ArrayList<SchedDTO>> initDTOListMap;

	/**
	 * The map of the initial DTO list
	 */
	public static HashMap<Long, ArrayList<String>> initDTOIdListMap;

	/**
	 * The map of the current list of DTO list
	 */
	public static HashMap<Long, ArrayList<ArrayList<SchedDTO>>> currDTOListMap;

	/**
	 * The map of the current list of plan offset times
	 */
	public static HashMap<Long, ArrayList<Long>> currPlanOffsetTimeMap;
	
	/**
	 * The map of the current list of DTO list
	 */
	public static HashMap<Long, ArrayList<SchedDTO>> deltaSchedDTOListMap;
	
	/**
	 * The map of conflicting DTO Id list
	 */
	public static HashMap<String, ArrayList<String>> conflDTOIdListMap;
	
	/**
	 * The map of the conflicting element list
	 */
	public static HashMap<String, List<ElementsInvolvedOnOrbit>> conflElementListMap;
	
	/**
	 * The map of the reason conflicting DTO Ids list
	 */
	public static HashMap<ReasonOfReject, ArrayList<String>> conflReasonDTOIdListMap;

	/**
	 * The map of the replaced scheduled DTO Ids
	 */
	public static HashMap<Long, HashMap<String, ArrayList<String>>> cancDTOIdListMap;

	/**
	 * The map of the total replaced scheduled DTO Ids
	 */
	public static HashMap<Long, ArrayList<String>> cancTotDTOIdListMap;
	
	/**
	 * The map of the total replaced scheduled DTO Ids
	 */
	public static HashMap<Long, ArrayList<SchedDTO>> cancTotDTOListMap;
	
	/**
	 * Process the the Delta-Plan requests relevant to 1 PR
	 *
	 * @param pSId
	 * @param newPRList
	 * @return
	 * @throws SPLAException 
	 */
	@SuppressWarnings("unchecked")
	public boolean processVUDeltaPlan(Long pSId, ArrayList<ProgrammingRequest> vuPRList) 
			throws SPLAException {

		/**
		 * The output boolean
		 */
		boolean result = false;

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();

		try {

			logger.info("Compute the Very Urgent schedule for PR: " + vuPRList.get(0).getProgrammingRequestId());

			/**
			 * The Plan offset date
			 */
			Date deltaPlanDate = SessionActivator.planDateMap.get(pSId);

//			// 1.0. Instance accepted DTOs
			initDTOListMap.put(pSId, (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone());
			initDTOIdListMap.get(pSId).addAll(RulesPerformer.getPlannedDTOIds(pSId));

			/**
			 * The list of plan date times
			 */
			ArrayList<Long> vuRefTimeList = new ArrayList<Long>();

			for (ProgrammingRequest vuPR : vuPRList) {

				/**
				 * The planned AR counter
				 */
				int planARCounter = 0;

				for (AcquisitionRequest aR : vuPR.getAcquisitionRequestList()) {

					// 2.0. check VU planning
					if (!isVUDTOPlanned(pSId, vuPR, aR, vuRefTimeList)) {

						logger.warn("No solution is found for Very Urgent AR: "
								+ ObjectMapper.parseDMToSchedARId(vuPR.getUserList().get(0).getUgsId(), 
										vuPR.getProgrammingRequestId(), aR.getAcquisititionRequestId())); 

						logger.warn("The Very Urgent PR: " + ObjectMapper.parseDMToSchedPRId(
								vuPR.getUserList().get(0).getUgsId(), vuPR.getProgrammingRequestId()) 
						+ " is NOT scheduled for the Planning Session: " + pSId);						

						break;

					} else {

						planARCounter ++;
					}
				}

				if (!vuRefTimeList.isEmpty()) {
					
					deltaPlanDate = new Date(Collections.min(vuRefTimeList));
	
//					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
//	
//					logger.info("The Delta-Plan Offset time for the VU PR " + vuPR.getProgrammingRequestId() + " is: "
//							+ formatter.format(deltaPlanDate));
	
					currPlanOffsetTimeMap.get(pSId).add(deltaPlanDate.getTime());
				}

				// 3.0. Check PR schedulability
				if (planARCounter == vuPR.getAcquisitionRequestList().size()) {

					// At least 1 PR is scheduled
					result = true;
					
					logger.debug("Solution found for VU PR: " + ObjectMapper.parseDMToSchedPRId(
							vuPR.getUserList().get(0).getUgsId(), vuPR.getProgrammingRequestId()));
				
				} else {
										
					// Clear current Plan offset
					currPlanOffsetTimeMap.get(pSId).clear();
	
					// 3.1. Purge scheduled tasks in the Delta-Plan
					rulesPerformer.purgeSchedTasks(pSId);
					// 3.2. Restore previous solution
					rulesPerformer.initBRMPlan(pSId);
	
					// 3.3. Set LMP Delta-Plan rejection
					setDeltaPlanRejection(pSId, vuPR);
				}
			}
			
			logger.debug("VU output result is: " + result);

			// 4.0. Get the VU Delta-Plan solution according to the first Plan Offset date
			ArrayList<SchedDTO> vuSol = getDeltaPlanSol(pSId);
			
			// 4.1. Update scheduled tasks from BRM in the Planning Session
			rulesPerformer.updateSchedTasks(pSId, false);

			if (vuSol.isEmpty()) {
				
				// Reset the delta plan
				resetDeltaPlan(pSId);
				
			} else {
			
				for (SchedDTO deltaDTO : vuSol) {
	
					if (!initDTOIdListMap.get(pSId).contains(deltaDTO.getDTOId())
							&& (RequestChecker.isVU(deltaDTO.getPRType()))) {
	
						logger.debug("Following VU DTO " + deltaDTO.getDTOId() 
						+ " is newly scheduled for Planning Session: " + pSId);				
	
						deltaSchedDTOListMap.get(pSId).add(deltaDTO);
					}
				} 
				
				if (deltaSchedDTOListMap.get(pSId).isEmpty()) {
					
					logger.warn("No VU DTOs found!");
				}
			}

//			// 4.0. Update the Delta-Plan statuses
//			sessionScheduler.setDeltaPlanStatuses(pSId, initDTOListMap.get(pSId), vuSol);
//
//			if (!vuSol.isEmpty()) {
//
//				// 4.1. Finalize schedule
//				logger.info("Finalize the Delta-Plan schedule.");
//				sessionScheduler.finalizeSchedule(pSId);
//			}

			// 5.0. Update Progress Report
			sessionScheduler.setProgressReport(pSId);

		} catch (Exception e) {

			logger.error("Error processing Very Urgent PRList for Planning Session: {} - {}", 
					pSId, e.getMessage(), e);
		}

		return result;
	}

	/**
	 * Plan Very Urgent DTO
	 * // TODO: validate VU pre-check
	 * 
	 * @param pSId
	 * @param vuPR
	 * @param vuAR
	 * @param vuRefTimeList
	 * @return
	 * @throws Exception
	 */
	private boolean isVUDTOPlanned(Long pSId, ProgrammingRequest vuPR, AcquisitionRequest vuAR,
			ArrayList<Long> vuRefTimeList) throws Exception {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		ConflictDTOCalculator conflDTOCalculator = new ConflictDTOCalculator();

		/**
		 * The planed boolean
		 */
		boolean isPlanned = false;

		// Count the scheduled VU DTOs in the MH
		int vuCounter = countVURequestCount(pSId);

		/**
		 * The satellite Id list (1 for each DTO)
		 */
		ArrayList<String> dtoSatIdList = new ArrayList<>();

		/**
		 * The scheduled DTO list
		 */
		ArrayList<SchedDTO> schedDTOList = ObjectMapper.parseDMToSchedDTOList(pSId, vuPR.getUserList().get(0).getUgsId(),
				vuPR.getProgrammingRequestId(), vuAR.getAcquisititionRequestId(), vuAR.getDtoList(),
				vuPR.getUserList().get(0).getAcquisitionStationIdList(), false);
		
		// 2.1. Filter DTOs by veto
		schedDTOList = filterDTOs(pSId, schedDTOList);
		
		// Sort worth DTOs
		conflDTOCalculator.orderWorthDTOs(pSId, schedDTOList);

		// 2.2. Handle Equivalent DTO
		if (RequestChecker.hasEquivDTO(pSId, schedDTOList.get(0).getARId())) {

			Collections.sort(schedDTOList, new SchedDTOTimeComparator());

			dtoSatIdList.add(schedDTOList.get(0).getSatelliteId());

			// 2.3. Check if the DTO is inside the Delta-Plan				
			if (isDeltaPlanFeasible(pSId, schedDTOList.get(0)) 
					&& isUplinkAvailable(pSId, schedDTOList.get(0).getStartTime(), dtoSatIdList)) {

				/**
				 * The Equivalent DTO
				 */
				EquivalentDTO equivDTO = ObjectMapper.parseSchedToBRMEquivDTO(pSId, schedDTOList, vuAR.getEquivalentDTO(), 
						vuPR.getMode(), vuPR.getPitchExtraBIC());

				equivDTO.setAllDtoInEquivalentDto(ObjectMapper.parseSchedToBRMDTOList(pSId, schedDTOList));

				// 2.4. Plan VU Equivalent DTO
				if (rulesPerformer.planEquivDTO(pSId, equivDTO, false)) {

					logger.info("Selected AR Equivalent DTO: " + equivDTO.getEquivalentDtoId() + " is consistent.");

					isPlanned = true;

				} else {

					// 2.5. Check the VU Equivalent DTO solution
					if (planVUEquivSol(pSId, equivDTO, vuRefTimeList)) {

						isPlanned = true;

						vuCounter ++;
					}
				}
				
			} else {

				logger.warn("The Very Urgent Equivalent DTO of AR: " + schedDTOList.get(0).getARId()
						+ " is unfeasible within the Delta-Plan.");
				
				return false;
			}
		}
		
		// 2.1. Handle single DTO
		else 		
		{
			for (int i = 0; i < schedDTOList.size(); i ++) {

				SchedDTO schedDTO = schedDTOList.get(i);
				
				/**
				 * The Delta-Plan date
				 */
				Date deltaPlanDate = new Date((long) (schedDTO.getStartTime().getTime() 
						- Configuration.deltaTime));

				dtoSatIdList.add(schedDTO.getSatelliteId());

				// 2.2. Check if VU DTO is inside the Delta-Plan
				if (isDeltaPlanFeasible(pSId, schedDTO)
						&& isUplinkAvailable(pSId, schedDTO.getStartTime(), dtoSatIdList)) {

					// Set VU AR rank
					PRListProcessor.schedARIdRankMap.get(pSId).put(schedDTO.getARId(), vuCounter);

					// 2.3. Check if VU DTO is valid for overlap
					Map<Boolean, List<String>> vuConflMap = RulesPerformer.brmOperMap.get(pSId)
							.checkIfLMP_VU_IsValidForOverlap(RulesPerformer.brmParamsMap.get(pSId), 
									pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), 
									ObjectMapper.parseSchedToBRMDTO(pSId, schedDTO));

					if (vuConflMap.containsKey(true)) {

						// 2.4. Check the consistency of the DTO List
						if (rulesPerformer.planSchedDTO(pSId, schedDTO)) {

							logger.info("The Very Urgent AR: " + schedDTO.getARId() + " has been scheduled.");

							vuRefTimeList.add(deltaPlanDate.getTime());

							// 2.5. Handle Stereopair && Interferometric DTOs
							if (vuPR.getIsStereopair() && vuPR.getIsInterferometric() &&
									RulesPerformer.getPlannedDTOIds(pSId).contains(schedDTO.getLinkDtoIdList().get(0))) {

								if (rulesPerformer.planSchedDTO(pSId, PRListProcessor.schedDTOMap.get(pSId)
										.get(schedDTO.getLinkDtoIdList().get(0)))) {

									isPlanned = true;

									vuCounter ++;

									break;

								} else {

									isPlanned = false;

									vuCounter --;
								}

							} else {

								isPlanned = true;

								vuCounter ++;

								break;
							}

//							// Update solution for the current Delta-Plan
//							updateDeltaPlanSol(pSId, vuRefTimeList);
						
						} else {

							logger.debug("The Very Urgent DTO:" + schedDTO.getDTOId() + " is NOT freely placed.");	
						}

					} else {

						logger.debug("The Very Urgent DTO " + schedDTO.getDTOId() 
						+ " is rejected because at least in conflict with: " 
						+ vuConflMap.get(false).get(0));		

						// 2.4. Plan VU DTO (for rejection)
						rulesPerformer.planSchedDTO(pSId, schedDTO);

//						// Update Delta-Plan statuses
//						sessionScheduler.setDeltaPlanStatuses(pSId, initDTOListMap.get(pSId), 
//								initDTOListMap.get(pSId));
					}

				} else {

					logger.warn("The Very Urgent DTO: " + schedDTO.getDTOId() 
					+ " is unfeasible within the Delta-Plan.");
					
					schedDTOList.remove(schedDTO);
					
					i --;

					/**
					 * The list of rejection reasons
					 */
					ArrayList<ReasonOfRejectElement> rejReasonList = new ArrayList<ReasonOfRejectElement>();
					rejReasonList.add(new ReasonOfRejectElement(
							ReasonOfReject.outsideMHDTO, 1, "Unfeasible within the MH Delta-Plan", null));

					// Add rejected Id into the map
					RulesPerformer.rejDTORuleListMap.get(pSId).put(schedDTO.getDTOId(), 
							rejReasonList);
				}

				dtoSatIdList.clear();
			}

			if (!isPlanned && !schedDTOList.isEmpty()) {

				// Plan the Very Urgent DTO solution
				if (planVUSol(pSId, schedDTOList, vuRefTimeList)) {

					vuCounter ++;

					isPlanned = true;
				}
			}
		}

		return isPlanned;
	}

	/**
	 * Plan the Very Urgent solution for the scheduled DTOs of an AR 
	 * 
	 * @param pSId
	 * @param vuDTOList
	 * @param vuRefTimeList
	 * @return
	 * @throws Exception
	 */
	private boolean planVUSol(Long pSId, ArrayList<SchedDTO> vuDTOList, ArrayList<Long> vuRefTimeList) 
			throws Exception {

		/**
		 * The found boolean
		 */
		boolean isVUFound = false;

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		logger.debug("Plan the Very Urgent DTO solution for AR: " + vuDTOList.get(0).getARId());

		// Initialize VU data
		initUrgentData(pSId, vuDTOList);
		
		for (SchedDTO vuDTO : vuDTOList) {

			logger.info("Remove conflicting DTOs for overlap.");

			// Check if VU DTO is valid for overlap
			Map<Boolean, List<String>> vuConflMap = RulesPerformer.brmOperMap.get(pSId)
					.checkIfLMP_VU_IsValidForOverlap(RulesPerformer.brmParamsMap.get(pSId), 
							pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), 
							ObjectMapper.parseSchedToBRMDTO(pSId, vuDTO));

			// Retract conflicting DTOs (redundant)
			if (vuConflMap.get(true) == null)
				vuConflMap.put(true, new ArrayList<String>());

			// Add cancelling DTO Id list
			cancDTOIdListMap.get(pSId).put(vuDTO.getDTOId(), (ArrayList<String>) vuConflMap.get(true));
			cancTotDTOIdListMap.get(pSId).addAll(vuConflMap.get(true));
			
			for (String conflDTOId : vuConflMap.get(true)) {

				// Retract conflicting DTO
				rulesPerformer.retractDTOById(pSId, conflDTOId, ReasonOfReject.deletedForVU);

			}

			// Check DTO consistency
			if (rulesPerformer.planSchedDTO(pSId, vuDTO)) {

				/**
				 * The Delta-Plan date
				 */
				Date deltaPlanDate = new Date((long) (vuDTO.getStartTime().getTime() 
						- Configuration.deltaTime));

				if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
						.getMissionHorizonStartTime())) {
					
					deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
							.getMissionHorizonStartTime();
				}
				
				vuRefTimeList.add(deltaPlanDate.getTime());

				isVUFound = true;

				logger.info("The Very Urgent DTO: " + vuDTO.getDTOId() + " has been scheduled.");

				break;
			
			} else {
				
				logger.info("Remove conflicting DTOs for overlap.");

				if (vuConflMap.get(true) == null)
					vuConflMap.put(true, new ArrayList<String>());

				for (String conflDTOId : vuConflMap.get(true)) {

					logger.debug("Replan conflicting DTOs for overlap.");	
					
					/**
					 * The scheduled DTO
					 */
					SchedDTO conflDTO = ObjectMapper.parseDMToSchedDTO(pSId, conflDTOId,
							PRListProcessor.dtoSchedIdMap.get(pSId).get(conflDTOId), false);
					
					// Replan conflicting DTO
					if (rulesPerformer.planSchedDTO(pSId, conflDTO)) {

						// Remove cancelled DTO Id
						cancDTOIdListMap.get(pSId).remove(conflDTOId);
						
						/**
						 * The Delta-Plan date
						 */
						Date deltaPlanDate = new Date((long) (conflDTO.getStartTime().getTime() 
								- Configuration.deltaTime));
						
						if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
								.getMissionHorizonStartTime())) {
							
							deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
									.getMissionHorizonStartTime();
						}
						
						vuRefTimeList.add(deltaPlanDate.getTime());

					} else {

						logger.info("Overlapping DTO " + conflDTOId + " is NOT replannable.");	
					}
				}

				// Set conflict elements if existing
				setUrgentConflElements(pSId, vuDTO.getDTOId());
				
				logger.info("Remove conflicting DTOs for generic violation.");
				
				cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).clear();
				cancTotDTOIdListMap.get(pSId).clear();
				
				int conflSize = conflDTOIdListMap.get(vuDTO.getDTOId()).size();
				
				for (int i = 0; i < conflSize; i ++) {

					// The conflicting DTO Id
					String conflDTOId = conflDTOIdListMap.get(vuDTO.getDTOId()).get(i);
					
					// Retract conflicting DTO
					rulesPerformer.retractDTOById(pSId, conflDTOId, ReasonOfReject.deletedForVU);

					cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).add(conflDTOId);
					cancTotDTOIdListMap.get(pSId).add(conflDTOId);
				
					// Check DTO consistency			
					if (rulesPerformer.planSchedDTO(pSId, vuDTO)) {
					
						/**
						 * The Delta-Plan date
						 */
						Date deltaPlanDate = new Date((long) (vuDTO.getStartTime().getTime() 
								- Configuration.deltaTime));
	
						if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
								.getMissionHorizonStartTime())) {
							
							deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
									.getMissionHorizonStartTime();
						}
						
						vuRefTimeList.add(deltaPlanDate.getTime());
	
						isVUFound = true;
	
						logger.info("The Very Urgent DTO: " + vuDTO.getDTOId() + " has been scheduled.");
	
						break;
					
					} else {

						logger.info("The Very Urgent DTO: " + vuDTO.getDTOId() + " is NOT scheduled yet. " +
									"Additional conflicting DTOs shall be removed.");		
						
						if (conflDTOId.equals(conflDTOIdListMap.get(vuDTO.getDTOId()).get(conflSize - 1))) {

							conflDTOIdListMap.get(vuDTO.getDTOId()).clear();
							
							// Set conflict elements if existing
							setUrgentConflElements(pSId, vuDTO.getDTOId());	
							
							conflSize = conflDTOIdListMap.get(vuDTO.getDTOId()).size();	
							
							i = -1;
								
						}
					}
				}
			}
							
			// TODO: check n-ary conflicts			
			if (cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).size() > 2) { //n-ary Conflict
				
				logger.debug("Try to replan conflicting DTOs for n-ary conflict violation.");
				
				for (int i = 0; i < cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).size(); i ++) {
					
					String conflDTOId = cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).get(i);
					
					/**
					 * The conflicting DTO
					 */
					SchedDTO conflDTO = ObjectMapper.parseDMToSchedDTO(pSId, conflDTOId,
							PRListProcessor.dtoSchedIdMap.get(pSId).get(conflDTOId), false); 
					
					// Replan conflicting DTOs
					if (rulesPerformer.planSchedDTO(pSId, conflDTO)) {
						
						// Remove cancelled DTO Id
						cancDTOIdListMap.get(pSId).get(vuDTO.getDTOId()).remove(conflDTOId);
						cancTotDTOIdListMap.get(pSId).remove(conflDTOId);
	
						i --;
	
						/**
						 * The Delta-Plan date
						 */
						Date deltaPlanDate = new Date((long) (conflDTO.getStartTime().getTime() 
								- Configuration.deltaTime));
	
						if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
								.getMissionHorizonStartTime())) {
							
							deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
									.getMissionHorizonStartTime();
						}
						
						vuRefTimeList.add(deltaPlanDate.getTime());
				
					} else {
	
						logger.warn("Conflicting DTO " + conflDTOId + " is NOT replannable.");	
					}
				}
			}
				
			break;


//			// Restore conflicting DTOs
//			if (!isVUFound) {
//	
//				logger.debug("Acquisition relevant to the VU AR: "" + schedDTO.getARId() + " is NOT scheduled.");	
//
//				// Remove cancelled DTO Id
//				cancDTOIdListMap.get(pSId).add(schedDTO.getDTOId());
//
//				logger.debug("Replan conflicting DTOs...");	
//
//				if (vuConflMap.get(true) == null)
//					vuConflMap.put(true, new ArrayList<String>());
//
//				for (String conflDTOId : vuConflMap.get(true)) {
//
//					// Replan conflicting DTO
//					if (rulesPerformer.planDTO(pSId, ObjectMapper.parseDMToSchedDTO(
//							pSId, conflDTOId, PRListProcessor.dtoSchedIdMap.get(pSId).get(conflDTOId)), true)) {
//
//						// Remove cancelled DTO Id
//						cancDTOIdListMap.get(pSId).remove(conflDTOId);
//
//					} else {
//
//						logger.info("Conflicting DTO " + conflDTOId + " is NOT replannable.");	
//					}
//				}
//			}
		}
		
		// Update solution for the current Delta-Plan
		updateDeltaPlanSol(pSId, vuRefTimeList);

		return isVUFound;
	}

	/**
	 * Plan the Very Urgent Equivalent solution
	 * 
	 * @param pSId
	 * @param vuEquivDTO
	 * @param vuRefTimeList
	 * @return
	 * @throws Exception
	 */
	private boolean planVUEquivSol(Long pSId, EquivalentDTO vuEquivDTO, ArrayList<Long> vuRefTimeList) 
			throws Exception {

		/**
		 * The found boolean
		 */
		boolean isVUFound = false;

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		logger.debug("Plan the Very Urgent Equivalent DTO solution.");

		// Find the conflict elements of the DTO
		for (com.nais.spla.brm.library.main.ontology.resourceData.DTO brmDTO : vuEquivDTO.getAllDtoInEquivalentDto()) {

			logger.debug("Remove conflicting DTOs...");

			// Check if VU DTO is valid for overlap
			Map<Boolean, List<String>> vuConflMap = RulesPerformer.brmOperMap.get(pSId)
					.checkIfLMP_VU_IsValidForOverlap(RulesPerformer.brmParamsMap.get(pSId), 
							pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), brmDTO);

			if (vuConflMap.get(true) == null)
				vuConflMap.put(true, new ArrayList<String>());

			cancDTOIdListMap.get(pSId).put(brmDTO.getDtoId(), (ArrayList<String>) vuConflMap.get(true));
			cancTotDTOIdListMap.get(pSId).addAll(vuConflMap.get(true));

			
			for (String conflDTOId : vuConflMap.get(true)) {

				// Retract conflicting DTO
				rulesPerformer.retractDTOById(pSId, conflDTOId, ReasonOfReject.deletedForVU);
			}
		}

		// Check DTO List consistency
		if (rulesPerformer.planEquivDTO(pSId, vuEquivDTO, false)) {

			isVUFound = true;

			/**
			 * The Delta-Plan date
			 */
			Date deltaPlanDate = new Date((long) (vuEquivDTO.getStartTime().getTime() 
					- Configuration.deltaTime));

			if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
					.getMissionHorizonStartTime())) {
				
				deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
						.getMissionHorizonStartTime();
			}
			
			vuRefTimeList.add(deltaPlanDate.getTime());

			logger.info("The Very Urgent Equivalent DTO: " + vuEquivDTO.getEquivalentDtoId() 
			+ " has been scheduled.");

//			// Update solution for the current Delta-Plan
//			updateDeltaPlanSol(pSId, vuRefTimeList);

		} else {

			logger.warn("No solution is found for the Very Urgent Equivalent DTO: " + vuEquivDTO.getEquivalentDtoId() 
			+ " of Planning Session:" + pSId);

//			// Cancel Equivalent DTOs
//			for (com.nais.spla.brm.library.main.ontology.resourceData.DTO dto : vuEquivDTO.getAllDtoInEquivalentDto()) {
//
//				cancDTOIdListMap.get(pSId).add(dto.getDtoId());
//			}

			isVUFound = false;
		}
		return isVUFound;
	}

	/**
	 * Schedule the Last Minute Planning Delta-Plan
	 *
	 * // TODO: validate LMP pre-check
	 *
	 * @param pSId
	 * @param newPRList
	 * @return
	 * @throws SPLAException 
	 */
	@SuppressWarnings("unchecked")
	public boolean processLMPDeltaPlan(Long pSId, ArrayList<ProgrammingRequest> newPRList) 
			throws SPLAException {

		/**
		 * Instance handlers
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();

		RulesPerformer rulesPerformer = new RulesPerformer();

		ConflictDTOCalculator conflDTOCalculator = new ConflictDTOCalculator();

		/**
		 * The output boolean
		 */
		boolean result = false;

		try {

			logger.info("Compute the Last Minute Planning schedule.");

			/**
			 * Plan offset date
			 */
			Date planDate = SessionActivator.planDateMap.get(pSId);

			// 1.0. Set initial DTO list
			initDTOListMap.put(pSId, (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone());
			initDTOIdListMap.get(pSId).addAll(RulesPerformer.getPlannedDTOIds(pSId));

			// 1.1. Get the counter of the already scheduled LMP in the Mission Horizon
			int lmpCounter = getLMPRequestCount(pSId);

			/**
			 * The Last Minute Planning reference time list
			 */
			ArrayList<Long> lmpRefTimeList = new ArrayList<>();

			for (ProgrammingRequest lmpPR : newPRList) {

				/**
				 * The LMP PR Id
				 */
				String lmpPRId = ObjectMapper.parseDMToSchedPRId(lmpPR.getUgsId(), 
						lmpPR.getProgrammingRequestId());
				
				/**
				 * The cancelled DTO list
				 */
				ArrayList<String> cancDTOIdList = new ArrayList<String>();

				cancDTOIdListMap.get(pSId).put(lmpPRId, new ArrayList<>());
				cancTotDTOIdListMap.get(pSId).clear();
				
				// 1.2. Delete replacing DTO for each PR
				if (lmpPR.getReplacedRequestList() != null) {

					for (ReplacedProgrammingRequest replPR : lmpPR.getReplacedRequestList()) {

						// Get replacing DTOs
						SchedDTO cancDTO = getReplacingDTO(pSId, replPR);

						if (cancDTO != null) {		

							cancDTOIdList.add(cancDTO.getDTOId());
													
						} else {

							logger.warn("No replacing DTOs found for PR: " + ObjectMapper.parseDMToSchedPRId(
									lmpPR.getUserList().get(0).getUgsId(), lmpPR.getProgrammingRequestId()));
						}
					}
					
					cancDTOIdListMap.get(pSId).put(lmpPRId, cancDTOIdList);
					cancTotDTOIdListMap.get(pSId).addAll(cancDTOIdList);

				}

				for (String cancDTOId : cancDTOIdListMap.get(pSId).get(lmpPRId)) {

					// Retract replacing DTO
					rulesPerformer.retractDTOById(pSId, cancDTOId, ReasonOfReject.deletedForLMP);
				}

				/**
				 * The planned AR counter
				 */
				int planARCounter = 0;

				// 2.0. Schedule AR DTOs
				for (AcquisitionRequest lmpAR : lmpPR.getAcquisitionRequestList()) {

					/**
					 * The AR planned boolean
					 */
					boolean isPlanned = false;

					/**
					 * The list of satellite Ids
					 */
					ArrayList<String> dtoSatIdList = new ArrayList<String>();

					/**
					 * The scheduled DTO list
					 */
					ArrayList<SchedDTO> schedDTOList = ObjectMapper.parseDMToSchedDTOList(pSId, 
							lmpPR.getUserList().get(0).getUgsId(), lmpPR.getProgrammingRequestId(), 
							lmpAR.getAcquisititionRequestId(), lmpAR.getDtoList(),
							lmpPR.getUserList().get(0).getAcquisitionStationIdList(), false);

					// Filter DTOs by veto
					schedDTOList = filterDTOs(pSId, schedDTOList);

					// Sort worth DTOs
					conflDTOCalculator.orderWorthDTOs(pSId, schedDTOList);

					// 2.1. Handle Equivalent DTO
					if (RequestChecker.hasEquivDTO(pSId, schedDTOList.get(0).getARId())) {

						Collections.sort(schedDTOList, new SchedDTOTimeComparator());

						dtoSatIdList.add(schedDTOList.get(0).getDTOId());

						// 2.2. Check if the DTO is inside the Delta-Plan				
						if (isDeltaPlanFeasible(pSId, schedDTOList.get(0)) 
								&& isUplinkAvailable(pSId, schedDTOList.get(0).getStartTime(), dtoSatIdList)) {

							/**
							 * The Equivalent DTO
							 */
							EquivalentDTO equivDTO = ObjectMapper.parseSchedToBRMEquivDTO(pSId, schedDTOList, 
									lmpAR.getEquivalentDTO(), lmpPR.getMode(), lmpPR.getPitchExtraBIC());

							equivDTO.setAllDtoInEquivalentDto(ObjectMapper.parseSchedToBRMDTOList(pSId, schedDTOList));

							// 2.3. Plan Equivalent DTO
							if (rulesPerformer.planEquivDTO(pSId, equivDTO, false)) {

								logger.info("Selected AR Equivalent DTO: " + equivDTO.getEquivalentDtoId() 
								+ " is consistent.");

								// 2.5. Update the Plan Offset date
								if (!lmpRefTimeList.isEmpty()) {
									
									planDate = new Date(Collections.min(lmpRefTimeList));
	
//									SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
//	
//									logger.debug("The LMP Delta-Plan Offset time for the PR " + lmpPR.getProgrammingRequestId() + " is: "
//											+ formatter.format(deltaPlanDate));
	
									currPlanOffsetTimeMap.get(pSId).add(planDate.getTime());
	
//									// Update solution for the current Delta-Plan
//									updateDeltaPlanSol(pSId, lmpRefTimeList);
								}

								lmpCounter ++;

								isPlanned = true;

							} else {

								logger.info("The Equivalent DTO " + equivDTO.getEquivalentDtoId() + " is NOT plannable");
//								// 2.4. Plan the LMP Equivalent DTO solution
//								if (planLMPEquivSol(pSId, equivDTO, lmpRefTimeList, 
//										lmpPR.getReplacedRequestList())) {
//									
//									lmpCounter ++;
//									
//									isPlanned = true;
//								}
							}
						} else {
							
							logger.warn("The Last Minute Planning Equivalent DTO of AR: " + schedDTOList.get(0).getARId()
									+ " is unfeasible within the Delta-Plan.");
							
							return false;

						}
					}	

					// 2.1B. Handle single DTO
					else 		
					{	
						for (int i = 0; i < schedDTOList.size(); i ++) {

							/**
							 * The scheduling DTO 
							 */
							SchedDTO schedDTO = schedDTOList.get(i);
							
							/**
							 * The DTO satellite Id list
							 */
							dtoSatIdList = new ArrayList<String>();

							dtoSatIdList.add(schedDTO.getSatelliteId());

							schedDTO.setReplDTOIdList(cancDTOIdList);

							// 2.2B. Check if the DTO is inside the Delta-Plan
							if (isDeltaPlanFeasible(pSId, schedDTO) 
									&& isUplinkAvailable(pSId, schedDTOList.get(0).getStartTime(), dtoSatIdList)) {

								PRListProcessor.schedARIdRankMap.get(pSId).put(schedDTO.getARId(), lmpCounter);

								// 2.3B. check LMP planning
								Map<Boolean, List<String>> lmpConflMap = RulesPerformer.brmOperMap.get(pSId)
										.checkIfLMP_VU_IsValidForOverlap(RulesPerformer.brmParamsMap.get(pSId),
												pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), 
												ObjectMapper.parseSchedToBRMDTO(pSId, schedDTO));

								if (lmpConflMap.containsKey(true)) {

									logger.info("Schedule Last Minute Planning DTO: " + schedDTO.getDTOId());

									Date deltaPlanDate = new Date((long)(schedDTO.getStartTime().getTime() 
											- Configuration.deltaTime));

									if (deltaPlanDate.before(SessionActivator.planSessionMap.get(pSId)
											.getMissionHorizonStartTime())) {
										
										deltaPlanDate = SessionActivator.planSessionMap.get(pSId)
												.getMissionHorizonStartTime();
									}
									
									lmpRefTimeList.add(deltaPlanDate.getTime());
									
									// 2.4B. Plan the LMP DTO
									if (rulesPerformer.planSchedDTO(pSId, schedDTO)) {

										logger.info("The Last Minute Planning AR: "
												+ ObjectMapper.parseDMToSchedARId(lmpPR.getUserList().get(0).getUgsId(),
														lmpPR.getProgrammingRequestId(), lmpAR.getAcquisititionRequestId())
												+ " has been scheduled.");

										// 2.5B. Update the Plan Offset date
										if (!lmpRefTimeList.isEmpty()) {
											
											deltaPlanDate = new Date(Collections.min(lmpRefTimeList));
	
//											SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
//	
//											logger.debug("The LMP Delta-Plan Offset time for the PR " + lmpPR.getProgrammingRequestId() + " is: "
//													+ formatter.format(deltaPlanDate));
	
											currPlanOffsetTimeMap.get(pSId).add(deltaPlanDate.getTime());
	
											// Update solution for the current Delta-Plan
											updateDeltaPlanSol(pSId, lmpRefTimeList);
										}
										
										// 2.6B. Handle Stereopair && Interferometric DTOs
										if (lmpPR.getIsStereopair() && lmpPR.getIsInterferometric() &&
												RulesPerformer.getPlannedDTOIds(pSId).contains(schedDTO.getLinkDtoIdList().get(0))) {

											if (rulesPerformer.planSchedDTO(pSId, PRListProcessor.schedDTOMap.get(pSId)
													.get(schedDTO.getLinkDtoIdList().get(0)))) {

												isPlanned = true;

												lmpCounter ++;

												break;

											} else {

												isPlanned = false;

												lmpCounter --;
											}

										} else {

											isPlanned = true;

											lmpCounter ++;

											break;
										}										

									} else {

										logger.debug("The Last Minute Planning DTO:" + schedDTO.getDTOId() 
										+ " is NOT scheduled.");

										// Remove cancelled DTO Id
										cancDTOIdListMap.get(pSId).get(lmpPRId).add(schedDTO.getDTOId());
									}

								} else {

									logger.debug("The Last Minute Planning DTO " + schedDTO.getDTOId() 
									+ " is rejected because at least in conflict with the not replaceable DTO: " 
									+ lmpConflMap.get(false).get(0));

									// Remove cancelled DTO Id
									cancDTOIdListMap.get(pSId).get(lmpPRId).add(schedDTO.getDTOId());
								}
								

							} else {

								logger.warn("The Last Minute Planning DTO: " + schedDTO.getDTOId() 
								+ " is unfeasible within the Delta-Plan.");
								
								schedDTOList.remove(schedDTO);
								
								i --;
							}
						} 					
					}

					if (isPlanned) {

						planARCounter ++;
					}
				}

				// 3.0. Check PR schedulability
				if (planARCounter == lmpPR.getAcquisitionRequestList().size()) {

					// At least 1 PR is scheduled
					result = true;
					
					logger.debug("Solution found for LMP PR: " + ObjectMapper.parseDMToSchedPRId(
							lmpPR.getUserList().get(0).getUgsId(), lmpPR.getProgrammingRequestId()));
				
				} else {
	
					// Clear current Plan offset
					currPlanOffsetTimeMap.get(pSId).clear();
	
					// Replan replaced DTO
					for (ReplacedProgrammingRequest replPR : lmpPR.getReplacedRequestList()) {

						// Get replacing DTOs
						SchedDTO cancDTO = getReplacingDTO(pSId, replPR);

						if (cancDTO != null) {							
							logger.info("Plan replaced DTO " + cancDTO.getDTOId());							

							rulesPerformer.planSchedDTO(pSId, cancDTO);
						}
					}
													
					
//					// 3.1. Purge scheduled tasks in the Delta-Plan
//					rulesPerformer.purgeSchedTasks(pSId);
//	
//					// 3.2. Restore previous solution
////					rulesPerformer.planDTOList(pSId, initDTOListMap.get(pSId), true);				
//					rulesPerformer.initBRMPlan(pSId);
	
					// 3.3. Set LMP Delta-Plan rejection
					setDeltaPlanRejection(pSId, lmpPR);
				}
			}
			
			logger.debug("LMP output result is: " + result);

			// 4.0. Get the LMP Delta-Plan solution according to the first Plan Offset date
			ArrayList<SchedDTO> lmpSol = getDeltaPlanSol(pSId);
			
			// 4.1. Update scheduled tasks from BRM in the Planning Session
			rulesPerformer.updateSchedTasks(pSId, false);

			if (lmpSol.isEmpty()) {
				
				// Reset the delta plan
				resetDeltaPlan(pSId);
			
			} else {
			
				for (SchedDTO deltaDTO : lmpSol) {
	
					if (!initDTOIdListMap.get(pSId).contains(deltaDTO.getDTOId())
							&& (RequestChecker.isLMP(deltaDTO.getPRType()))) {
	
						logger.info("Following LMP DTO " + deltaDTO.getDTOId() 
						+ " is newly scheduled in Planning Session: " + pSId);				
	
						deltaSchedDTOListMap.get(pSId).add(deltaDTO);
					}
				} 
				
				if (deltaSchedDTOListMap.get(pSId).isEmpty()) {
					
					logger.warn("No LMP DTOs found!");
				}
			}

//			// 4.2. Update the Delta-Plan statuses
//			logger.info("Update the Delta-Plan statuses.");
//			sessionScheduler.setDeltaPlanStatuses(pSId, initDTOListMap.get(pSId), lmpSol);
//
//			if (!lmpSol.isEmpty()) {
//			logger.info("Finalize the Delta-Plan schedule.");
//				sessionScheduler.finalizeSchedule(pSId);
//			}

			// 5.0. Update Progress Report
			sessionScheduler.setProgressReport(pSId);


		} catch (Exception e) {

			logger.error("Error processing Last Minute Planning PRList for Planning Session: {} - {}", 
					pSId, e.getMessage(), e);

		}

		return result;
	}

	/**
	 * Update the Delta-Plan solution relevant to the DTOs by time
	 *
	 * @param pSId
	 * @param vuRefTime
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void updateDeltaPlanSol(Long pSId, ArrayList<Long> refTimeList) throws Exception {

		logger.debug("Update the Delta-Plan solution for Planning Session: " + pSId);

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		SessionScheduler sessionScheduler = new SessionScheduler();
		
		/**
		 * The current DTO list
		 */
		ArrayList<SchedDTO> currDTOList = rulesPerformer.getAcceptedDTOs(pSId);

		currDTOListMap.get(pSId).clear();

		for (Long refTime : refTimeList) {

			for (int i = 0; i < currDTOList.size(); i++) {

				currDTOList.get(i).setDeltaTime(Math.abs(
						currDTOList.get(i).getStartTime().getTime() - refTime));
			}

			// Sort DTOs by delta time
			Collections.sort(currDTOList, new SchedDTODeltaTimeComparator());

			// Fill the current DTOList map
			currDTOListMap.get(pSId).add((ArrayList<SchedDTO>) currDTOList.clone());
		}

		//		logger.info("Change the Delta-Plan Mission Horizon start time.");
		//		
		//		/**
		//		 * The Delta-Plan start time - deleted: 26-09-2018
		//		 */
		//		Long deltaPlanStartTime = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime();
		//		
		//		if (!currPlanOffsetTimeMap.get(pSId).isEmpty()) {
		//		
		//			deltaPlanStartTime = Collections.min(currPlanOffsetTimeMap.get(pSId));
		//		}
		//		
		//		// Update the Mission Horizon start time according to the first current plan offset
		//		SessionActivator.planSessionMap.get(pSId).setMissionHorizonStartTime(
		//				new Date(deltaPlanStartTime));

		// Update Delta-Plan statuses
		sessionScheduler.setDeltaPlanStatuses(pSId, initDTOListMap.get(pSId), 
				initDTOListMap.get(pSId));

	}

	/**
	 * Check if the DTO is inside the Delta-Plan time and in a feasible status
	 *
	 * @param pSId
	 * @param schedDTO
	 * @return
	 */
	private boolean isDeltaPlanFeasible(Long pSId, SchedDTO schedDTO) {

		/**
		 * The inside delta-plan boolean
		 */	
		boolean isFeasible = false;

		if (! schedDTO.getStatus().equals(DtoStatus.Rejected)
				&& schedDTO.getStartTime().getTime() > (SessionActivator.planSessionMap.get(pSId)
						.getMissionHorizonStartTime().getTime())) {

			isFeasible = true;
		
		} else {
			
			logger.warn("The DTO: " + schedDTO.getDTOId() + " is outside the Delta-Plan starting at: "
					+ new Date((long)(SessionActivator.planSessionMap.get(pSId)
							.getMissionHorizonStartTime().getTime())).toString());
		}

		return isFeasible;
	}
	
	/**
	 * Filter DTOs due to SCM rejections
	 * @param pSId
	 * @param orderDTOList
	 * @return
	 */
	private ArrayList<SchedDTO> filterDTOs(Long pSId, ArrayList<SchedDTO> orderDTOList) {
	
		
		//Erase subscribers DTOs filtered by SCM
		for (int i = 0; i < orderDTOList.size(); i ++) {
		
		// Erase DTOs filtered by SCM
			if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId)
					.contains(orderDTOList.get(i).getDTOId())) {
	
				logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " is excluded "
						+ "due to filtering.");
	
				orderDTOList.remove(i);
	
				i--;
			}
	
		}
	
		//Erase subscribers DTOs filtered by SCM
		for (int i = 0; i < orderDTOList.size(); i ++) {
			
			/**
			 * The ordered DTO
			 */
			SchedDTO orderDTO = orderDTOList.get(i);
			
			for (int j = 1; j < orderDTO.getUserInfoList().size(); j ++) {
			
				/**
				 * The subscriber DTO Id
				 */
				String subSchedDTOId = ObjectMapper.parseDMToSchedDTOId(orderDTO.getUserInfoList().get(j).getUgsId(),
						ObjectMapper.getPRId(orderDTO.getDTOId()), 
						ObjectMapper.getARId(orderDTO.getDTOId()), 
						ObjectMapper.getDTOId(orderDTO.getDTOId()));
				
				if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId)
						.contains(subSchedDTOId)) {
					
					logger.debug("User with UGS Id: " + orderDTO.getUserInfoList().get(j).getUgsId() 
							+ "has been filtered from subscribers of DTO: " + orderDTO.getDTOId());
					orderDTO.getUserInfoList().remove(j);
					
					j --;
				}
			}
			
			orderDTOList.set(i, orderDTO);
		}
		
		return orderDTOList;
	}

	/**
	 * Get the Delta-Plan solution for the Planning Session
	 *
	 * @param pSId
	 * @return
	 * @throws Exception 
	 */
	private ArrayList<SchedDTO> getDeltaPlanSol(Long pSId) throws Exception {

		logger.info("Get the Delta-Plan solution for Planning Session: " + pSId);

		/**
		 * The final Delta-Plan solution
		 */
		ArrayList<SchedDTO> deltaPlanSol = new ArrayList<>();

		/**
		 * The offset plan delta time
		 */
		Long deltaTime = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime().getTime();

		// Update the Delta-Plan solution
		updateDeltaPlanSol(pSId, currPlanOffsetTimeMap.get(pSId));
		
		/**
		 * The solution counter
		 */
		int solCounter = 0;

		for (Long time : currPlanOffsetTimeMap.get(pSId)) {

			if (time < deltaTime) {

				deltaTime = time;

				deltaPlanSol = currDTOListMap.get(pSId).get(solCounter);
			}

			solCounter ++;
		}

		return deltaPlanSol;
	}

	/**
	 * Set Delta-Plan rejection
	 *
	 * @param pSId
	 * @param planPR
	 */
	private void setDeltaPlanRejection(Long pSId, ProgrammingRequest planPR) {

		try {

			/**
			 * Instance handler
			 */
			RulesPerformer rulesPerformer = new RulesPerformer();
			
			int pRCounter = 0;
			
			for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()) {

				if (ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), pRStatus.getProgrammingRequestId())
						.equals(ObjectMapper.parseDMToSchedPRId(planPR.getUserList().get(0).getUgsId(), 
								planPR.getProgrammingRequestId()))) {
					/**
					 * The list of acquisition requests
					 */
					int aRCounter = 0;
	
					for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {
						
						int dtoCounter = 0;
						/**
						 * The list of DTOs
						 */
						for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {
	
							logger.debug("The DTO of " + dtoStatus.getDtoId() + " of AR "
									+ aRStatus.getAcquisitionRequestId() + " of PR "
									+ pRStatus.getProgrammingRequestId() + " of UGS " + pRStatus.getUgsId()
									+ " is rejected.");
	
							// Retract replacing DTO
							rulesPerformer.retractDTOById(pSId, ObjectMapper.parseDMToSchedDTOId(
									pRStatus.getUgsId(), pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(), 
									dtoStatus.getDtoId()), ReasonOfReject.systemConflict);
							
							/**
							 * The Plan DTO status
							 */
							dtoStatus.setStatus(DtoStatus.Rejected);
	
							// Set conflict status
							// TODO: check with Ground!
							dtoStatus.setConflictDescription("System Conflict.");
							dtoStatus.setConflictReasonId(1);
							// Conflict reason
							dtoStatus.setActualBic(0.0);
								
							aRStatus.setStatus(AcquisitionRequestStatus.Rejected);						
							aRStatus.getDtoStatusList().set(dtoCounter, dtoStatus);
							
							dtoCounter ++;
						}
	
						pRStatus.getAcquisitionRequestStatusList().set(aRCounter, aRStatus);
						
						aRCounter ++;
					}
	
					// Update the Planning Session map
					SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()
					.set(pRCounter, pRStatus);			
				}
				
				pRCounter ++;
			}

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getMessage(), e);
		}
	}
	
	/**
	 * Initialize CBJ data
	 * 
	 * @param pSId
	 * @param prevSchedDomain
	 * @param prevSchedSol
	 * @param aRDTOList
	 * @throws Exception 
	 */
	private void initUrgentData(Long pSId, ArrayList<SchedDTO> vuDTOList) throws Exception {
		
		logger.debug("Initialize Urgent data.");

		// Initialize actual AR conflict set
		for (SchedDTO schedDTO : vuDTOList) {

			conflDTOIdListMap.put(schedDTO.getDTOId(), new ArrayList<String>());

			conflElementListMap.put(schedDTO.getDTOId(), new ArrayList<ElementsInvolvedOnOrbit>());
		}

		for (ReasonOfReject reason : ReasonOfReject.values()) {

			conflReasonDTOIdListMap.put(reason, new ArrayList<String>());
		}	
	}
	
	/**
	 * Set the conflict elements for the given urgent DTO Id
	 *
	 * // TODO: test if conflict element correspond to an Equivalent DTO it should
	 * be not an applicable CBJ node 
	 * // TODO: check that first conflicting index
	 * list corresponds to the most valuable
	 *
	 * @param pSId
	 * @param schedDTOId
	 */
	public void setUrgentConflElements(Long pSId, String schedDTOId) {

		try {
						
			if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)) {
	
				for (ReasonOfRejectElement reason : RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId)) {
	
					/**
					 * The entrySet iterator
					 */
					Iterator<Entry<Double, List<ElementsInvolvedOnOrbit>>> it = reason.getElementsInvolved().entrySet()
							.iterator();
	
					while (it.hasNext()) {
	
						for (ElementsInvolvedOnOrbit element : it.next().getValue()) {
	
							if (((element.getElementsInvolved() != null) 
									&& (conflReasonDTOIdListMap.get(reason.getReason()) != null 
									&& !conflReasonDTOIdListMap.get(reason.getReason()).containsAll(element.getElementsInvolved())))
									|| (ConflictDTOCalculator.isConflictReason(reason.getReason()))) {
	
								// Add conflict element
								if (conflElementListMap.containsKey(schedDTOId)) {
								
									conflElementListMap.get(schedDTOId).add(element);
								}
	
								if (!conflReasonDTOIdListMap.get(reason.getReason()).isEmpty()
									&& !conflReasonDTOIdListMap.get(reason.getReason())
												.containsAll(element.getElementsInvolved())) {
	
									// Add involved elements
									conflReasonDTOIdListMap.get(reason.getReason()).addAll(
											element.getElementsInvolved());
								}
							}
						}
					}
				}
	
				// Filter equivalent DTOs from the list of conflict elements
				for (int i = 0; i < conflElementListMap.get(schedDTOId).size(); i++) {
	
					/** 
					 * The involved elements
					 */
					ElementsInvolvedOnOrbit element = conflElementListMap.get(schedDTOId).get(i);
	
					if (element.getElementsInvolved() != null) {
						
						for (int j = 0; j < element.getElementsInvolved().size(); j++) {
		
//							/** 
//							 * The conflicting DTO Id
//							 */
//							String conflDTOId = element.getElementsInvolved().get(j);
//		
//							if (RequestChecker.hasEquivDTO(pSId, ObjectMapper.getSchedARId(conflDTOId))
//									|| ! RequestChecker.isStandardAR(pSId, ObjectMapper.getSchedARId(conflDTOId))
//									|| RequestChecker.isLinkDTOScheduled(pSId, PRListProcessor.schedDTOMap.get(pSId).get(conflDTOId))) {
//		
//								logger.trace("The DTO: " + conflDTOId + " is removed from the conflict list.");
//								element.getElementsInvolved().remove(conflDTOId);
//							}	
						}
		
						// Set element
						conflElementListMap.get(schedDTOId).set(i, element);			
					} 
				}
	
				// Sort the conflicting DTO Ids based on their cost
				if (!conflElementListMap.get(schedDTOId).isEmpty()) {
	
//					// Sort conflicts by cost: TBD
//					sortConflictsByCost(pSId, schedDTOId);
	
					// Filter equivalent DTOs from the list of conflict elements
					for (int i = 0; i < conflElementListMap.get(schedDTOId).size(); i++) {
	
						/** 
						 * The involved elements
					     */
						ElementsInvolvedOnOrbit element = conflElementListMap.get(schedDTOId).get(i);
	
						if (element.getElementsInvolved() != null) {
	
							for (String conflDTOId : element.getElementsInvolved()) {
		
								if (!conflDTOId.equals(schedDTOId) 
										&& !conflDTOIdListMap.get(schedDTOId).contains(conflDTOId)) {
									
									if (! RequestChecker.isVU(PRListProcessor.pRSchedIdMap.get(pSId)
											.get(ObjectMapper.getSchedPRId(conflDTOId)).getType())) {
		
										conflDTOIdListMap.get(schedDTOId).add(conflDTOId);
									}
								}
							}				 
						}
					}
	
					if (!conflDTOIdListMap.get(schedDTOId).isEmpty()) {
	
						logger.info("A number of candidate conflicting DTOs found: " 
						+ conflDTOIdListMap.get(schedDTOId).size());
						
//						logger.info("Candidate conflicting DTO found: " 
//						+ conflDTOIdListMap.get(selSchedDTOId).get(0));	
					}
	
					// Reset conflict element list for the selected DTO
					conflElementListMap.get(schedDTOId).clear();
				}
			}
			
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Check if the number of uplinks before the DTO is sufficient from the final
	 * Asynch Plan Offset time
	 *
	 * @param pSId
	 * @param dtoStartDate
	 * @param vuSatIdList
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean isUplinkAvailable(Long pSId, Date dtoStartDate, ArrayList<String> satIdList) {

		/**
		 * The output boolean
		 */
		boolean isAvail = true;

		try {

			/**
			 * The visibility counter
			 */
			ArrayList<Integer> visCountList = new ArrayList<Integer>();

			/**
			 * The satellite counter
			 */
			int satCount = 0;

			// Get S-Band visibilities in the available uplink time interval ONLY
			for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {

				for (String satId : satIdList) {

					logger.info("Satellite involved in the visibilities count: " + satId);

					if (sat.getCatalogSatellite().getSatelliteId().equals(satId)) {

						visCountList.add(0);

						// Sort DTOs by delta time
						ArrayList<Visibility> visList = (ArrayList<Visibility>) 
								((ArrayList<Visibility>) sat.getVisibilityList()).clone();
						
						Collections.sort(visList, new VisTimeComparator());
					
						logger.debug("Compute delta plan visibilities from: " + dtoStartDate.toString());
						for (Visibility vis : visList) {

							logger.debug("Check visibility for satellite " 
									+ sat.getCatalogSatellite().getSatelliteId() + " between: "
									+ vis.getVisibilityStartTime() + " and " + vis.getVisibilityStopTime() 
									+ ", for the X-Band status " + vis.isXbandFlag() + ", for the station " 
									+ vis.getAcquisitionStationId() + " with look side: " + vis.getLookSide());
							
							if (! vis.isXbandFlag() && vis.isAllocated()
									&& (dtoStartDate.getTime() - vis.getVisibilityStopTime().getTime()) > 0
											&& (vis.getVisibilityStartTime().getTime() 
													- SessionActivator.planDateMap.get(pSId).getTime()) > 0) {

								logger.debug("The S-Band visibility relevant to the acquisition station: " 
								+ vis.getAcquisitionStationId() + " of contact counter " + vis.getContactCounter() 
								+ " is feasible.");
								visCountList.set(satCount, visCountList.get(satCount) + 1);
							}
						}

						if (visCountList.size() > satCount) {

							logger.info("A number of S-Band visibilities: " + visCountList.get(satCount) 
							+ " is found for Planning Session: " + pSId);

							if (visCountList.get(satCount) >= Configuration.vuVisNumber) {

								logger.debug("The number of S-Band visibilities is consistent with "
										+ "the configured number: " + Configuration.vuVisNumber);

							} else {

								logger.debug("The number of S-Band visibilities is inconsistent with "
										+ "the configured number: " + Configuration.vuVisNumber);

								// Unset availability
								isAvail = false;
							}
						}

						break;
					}
				}

				// Update satellite counter
				satCount ++;
			}

		} catch (Exception ex) {

			logger.warn("S-Band visbilities count incompleted for Planning Session: " + pSId);
		}

		return isAvail;
	}

	/**
	 * Get the scheduled DTO to be replaced
	 *
	 * @param pSId
	 * @param replPR
	 * @return
	 */
	private SchedDTO getReplacingDTO(Long pSId, ReplacedProgrammingRequest replPR) {

		/**
		 * The scheduled DTO to be replaced
		 */
		SchedDTO cancDTO = null;

		/**
		 * The scheduling DTO Id
		 */
		String schedDTOId = ObjectMapper.parseDMToSchedDTOId(replPR.getUgsId(),
				replPR.getProgrammingRequestId(), replPR.getAcquisitionRequestId(),
				replPR.getDtoId());
		/**
		 * The scheduling PR Id
		 */
		String schedPRId = ObjectMapper.parseDMToSchedPRId(replPR.getUgsId(),
				replPR.getProgrammingRequestId());

		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId) 
				&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTOId)) {

			cancDTO = ObjectMapper.parseDMToSchedDTO(pSId, replPR.getUgsId(),
					replPR.getProgrammingRequestId(), replPR.getAcquisitionRequestId(), 
					PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTOId),
					PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getUserList()
					.get(0).getAcquisitionStationIdList(), false);

			logger.debug("Found DTO be replaced: " + cancDTO.getDTOId());
		}

		return cancDTO;
	}

	/**
	 * Count VU request counter of the given PRType
	 * 
	 * @param pSId
	 * @return
	 * @throws Exception 
	 */
	public int countVURequestCount(Long pSId) {

		/**
		 * The request type counter
		 */
		int typeCounter = 0;

		for (SchedDTO initSchedDTO : initDTOListMap.get(pSId)) {

			if (RequestChecker.isVU(initSchedDTO.getPRType())) {

				typeCounter ++;						
			}
		}
		return typeCounter;
	}

	/**
	 * Get the LMP request counter of the given PRType
	 * 
	 * @param pSId
	 * @return
	 * @throws Exception 
	 */
	public int getLMPRequestCount(Long pSId) {

		/**
		 * The request type counter
		 */
		int typeCounter = 0;

		for (SchedDTO initSchedDTO : initDTOListMap.get(pSId)) {

			if (RequestChecker.isLMP(initSchedDTO.getPRType())) {

				typeCounter ++;						
			}
		}
		return typeCounter;
	}
	
	/**
	 * Reset the delta plan times according when is empty solution 
	 * @param pSId
	 */
	@SuppressWarnings("unchecked")
	private void resetDeltaPlan(Long pSId) {
		
		logger.info("Reset the Delta-Plan for Planning Session: " + pSId);
		
		// Reset the current plan offset time
		DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId).add((long)
				(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime().getTime()
				+ Configuration.deltaTime));
		
		/**
		 * The delta- plan date
		 */
		Date deltaPlanDate = SessionActivator.planDateMap.get(pSId);

		/**
		 * The sat counter
		 */
		int satCount = 0;
		
		/**
		 * The visibility counter list
		 */
		ArrayList<Integer> visCountList = new ArrayList<Integer>();

		// Get S-Band visibilities in the available uplink time interval ONLY
		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			logger.info("Satellite involved in the visibilities count: " 
			+ sat.getCatalogSatellite().getSatelliteId());

			visCountList.add(0);

			// Sort DTOs by delta time
			ArrayList<Visibility> visList = (ArrayList<Visibility>) 
					((ArrayList<Visibility>) sat.getVisibilityList()).clone();
			
			Collections.sort(visList, new VisTimeComparator());

			for (Visibility vis : visList) {
	
				// S-Band flag && visibility time
				if (! vis.isXbandFlag()
						&& (vis.getVisibilityStopTime().compareTo(deltaPlanDate) < 0)) {
	
					visCountList.set(satCount, visCountList.get(satCount) + 1);
					
					satCount ++;
					
					if (vis.getVisibilityStopTime().after(deltaPlanDate)) {
						
						deltaPlanDate = vis.getVisibilityStopTime();
					}
					
					if (satCount == Configuration.vuVisNumber) {
						
						break;
					}
				}
			}
		}			
	}

}

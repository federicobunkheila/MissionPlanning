/**
 *
 * MODULE FILE NAME: SessionScheduler.java
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

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
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

import com.nais.spla.brm.library.main.drools.utils.DroolsUtils;
import com.nais.spla.brm.library.main.ontology.resourceData.DebitCard;
import com.nais.spla.brm.library.main.ontology.tasks.Bite;
import com.nais.spla.brm.library.main.ontology.utils.ElementsInvolvedOnOrbit;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.handler.HPCivilianRequestHandler;
import com.telespazio.csg.spla.csps.handler.MessageHandler;
import com.telespazio.csg.spla.csps.model.impl.MacroDLO;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RankPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.BICCalculator;
import com.telespazio.csg.spla.csps.utils.BRMTaskTimeComparator;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.PredecessorCounter;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.csg.spla.csps.utils.TaskPlanner;
import com.telespazio.csg.spla.csps.utils.TaskStartTimeComparator;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRKind;
import it.sistematica.spla.datamodel.core.enums.PRStatus;
import it.sistematica.spla.datamodel.core.enums.ReportType;
import it.sistematica.spla.datamodel.core.enums.TaskMarkType;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.BICLoan;
import it.sistematica.spla.datamodel.core.model.BICReport;
import it.sistematica.spla.datamodel.core.model.ConflictingRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.ProgressReport;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogUgs;																		
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;
import it.sistematica.spla.datamodel.core.model.task.BITE;
import it.sistematica.spla.datamodel.core.model.task.CMGAxis;
import it.sistematica.spla.datamodel.core.model.task.DLO;
import it.sistematica.spla.datamodel.core.model.task.Download;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.datamodel.core.model.task.PassThrough;
import it.sistematica.spla.datamodel.core.model.task.Ramp;
import it.sistematica.spla.datamodel.core.model.task.Silent;
import it.sistematica.spla.datamodel.core.model.task.Store;
import it.sistematica.spla.datamodel.core.model.task.StoreAux;
import it.sistematica.spla.ekmlib.EKMLIB;

/**
 * The session scheduler class
 */
public class SessionScheduler {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(SessionScheduler.class);

	/**
	 * The satellite list map
	 */
	public static HashMap<Long, List<Satellite>> satListMap;

	/**
	 * The scheduled ARList map
	 */
	public static HashMap<Long, ArrayList<SchedAR>> schedARListMap;

	/**
	 * The scheduled DTOList map
	 */
	public static HashMap<Long, ArrayList<SchedDTO>> schedDTOListMap;

	/**
	 * The rejected AR DTO Id List map
	 */
	public static HashMap<Long, ArrayList<String>> rejARDTOIdSetMap;

	/**
	 * The rejected DTO Id List map
	 */
	public static HashMap<Long, ArrayList<String>> rejDTOIdListMap;

	/**
	 * The planned DTO Id List map
	 */
	public static HashMap<Long, ArrayList<String>> planDTOIdListMap;

	/**
	 * The map of the list of macro DLOs
	 */
	public static HashMap<Long, ArrayList<MacroDLO>> macroDLOListMap;
	
	/**
	 * The map of the list of planned DLOs
	 */
	public static HashMap<Long, ArrayList<DLO>> planDLOListMap;

	/**
	 * The map of the list of GPS slots for each visibility counter
	 */
	public static HashMap<Long, Long> visCounterGPSSlotMap = new HashMap<>();

	/**
	 * The persistence boolean map
	 */
	public static Map<Long, String> persistenceMap;
	
	/**
	 * The persistence boolean map
	 */
	public static Map<Long, Integer> finalMap;
	
	/**
	 * The owner BIC map (complete for the progress reports)
	 */
	public static HashMap<Long, HashMap<String, Double[]>> ownerBICRepMap;
	
	/**
	 * The owner BIC map (available premium and routine only)
	 */
	public static HashMap<Long, HashMap<String, Double[]>> ownerBICMap;
	
	/**
	 * The map of the DTO image Id
	 */
	public static HashMap<Long, HashMap<String, Long>> dtoImageIdMap;
	
	/**
	 * The minimum Owner Packet Store Id map
	 */
	public static HashMap<Long, HashMap<String, Long>> ownerMinPSIdMap;
	
	/**
	 * The minimum International Packet Store Id map
	 */
	public static HashMap<Long, Long> intMinPSIdMap;
	
	/**
	 * The maximum International Packet Store Id map
	 */
	public static Long catIntMaxPSId;
	
	/**
	 * The owner minimum International Packet Store Id map
	 */
	private static Long catIntMinPSId;
	
	/**
	 * The owner minimum Owner Packet Store Id map
	 */
	private static HashMap<String, Long> catOwnerMinPSIdMap;
	
	/**
	 * The owner maximum Owner Packet Store Id map
	 */
	private static HashMap<String, Long> catOwnerMaxPSIdMap;
	
	/**	
	 * The temporary list of storage Ids
	 */
	public static ArrayList<Integer> storeIdList;
	
	/**	
	 * The temporary overlap visibility map
	 */
	public static HashMap<String, ArrayList<Visibility>> overMHVisListMap  = new HashMap<>();

	/**
	 * Update the Plan statuses of the DTOs
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param schedSol
	 *            - the scheduled solution
	 * @param newSchedSol
	 *            - the new scheduled DTO list
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public void setPlanStatuses(Long pSId, ArrayList<SchedDTO> schedSol, 
			ArrayList<SchedDTO> newSchedDTOList) throws Exception {

		logger.info("Update the PR statuses of Planning Session: " + pSId);
		
		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer  = new RulesPerformer();
		
		/**
		 * The list of scheduled DTOs by BRM
		 */
		ArrayList<SchedDTO> brmDTOList = rulesPerformer.getAcceptedDTOs(pSId);
		
		if (schedSol.size() != brmDTOList.size()) {
		
			logger.warn("Solution sizes do not match for Planning Session: " + pSId);
							
			logger.info("Solution size in CSPS is: " +  schedSol.size()); 
			
			logger.info("Number of scheduled DTOs from BRM is: " + brmDTOList.size());	

			ArrayList<String> brmDTOIdList = new ArrayList<String>();
			
			for (SchedDTO brmDTO : brmDTOList) {
				
				brmDTOIdList.add(brmDTO.getDTOId());
			}
				
			ArrayList<String> schedDTOIdList = new ArrayList<String>();
			
			for (SchedDTO schedDTO : schedSol) {
				
				schedDTOIdList.add(schedDTO.getDTOId());
				
				if (! brmDTOIdList.contains(schedDTO.getDTOId())) {
					
					logger.info(schedDTO.getDTOId() + " NOT present in BRM.");
				}
			}	
			
			for (String dtoId: brmDTOIdList) {
				
				if (! schedDTOIdList.contains(dtoId)) {
					
					logger.info(dtoId + " NOT present in CSPS.");
				}
			}
				
//			for (SchedDTO schedDTO : schedSol) {
//			
//				logger.debug("Scheduled DTO from BRM: " + schedDTO.getDTOId());
//			}
	
		} else {
			
			logger.debug("Solution sizes coherent for Planning Session: " + pSId);
		}
	
		try {

			/**
			 * The DTO statuses
			 */
			PlanDtoStatus[] schedDTOStatuses = new PlanDtoStatus[newSchedDTOList.size()];

			/**
			 * The AR Ids
			 */
			String[] aRIds = new String[newSchedDTOList.size()];

			/**
			 * The PR Ids
			 */
			String[] pRIds = new String[newSchedDTOList.size()];

			/**
			 * The best AR DTO List
			 */
			ArrayList<String> schedDTOIdList = new ArrayList<>();

			for (SchedDTO schedDTO : schedSol) {

				schedDTOIdList.add(schedDTO.getDTOId());
			}

			/**
			 * The new AR DTO Id List
			 */
			ArrayList<String> newDTOIdList = new ArrayList<>();

			// update new AR
			for (int i = 0; i < newSchedDTOList.size(); i++) {

				newDTOIdList.add(newSchedDTOList.get(i).getDTOId());

				// Update DTO status
				if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(newDTOIdList.get(i))) {

					schedDTOStatuses[i] = new PlanDtoStatus(newDTOIdList.get(i), DtoStatus.Rejected);

				} else {

					schedDTOStatuses[i] = new PlanDtoStatus(newDTOIdList.get(i), DtoStatus.Unused); 
				}
			}

			for (int i = 0; i < newDTOIdList.size(); i++) {

				if (!Collections.disjoint(schedDTOIdList, newDTOIdList)) {

					for (int j = 0; j < schedDTOIdList.size(); j++) {

						if (schedDTOIdList.get(j).equals(newDTOIdList.get(i))) {

							schedDTOStatuses[i] = new PlanDtoStatus(schedDTOIdList.get(j), 
									DtoStatus.Scheduled);

							if (!RequestChecker.hasEquivDTO(pSId,
									ObjectMapper.getSchedARId(schedDTOIdList.get(j)))
									|| RequestChecker.hasLinkedDTO(pSId, schedDTOIdList.get(j))) {

								break;
							}

						} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(
								newDTOIdList.get(i))) {

							schedDTOStatuses[i] = new PlanDtoStatus(newSchedDTOList.get(i).getDTOId(),
									DtoStatus.Rejected);

//						} else {
//
//							schedDTOStatuses[i] = new PlanDtoStatus(newSchedDTOList.get(i).getDTOId(),
//									DtoStatus.Unused);
						}
					}

				} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(newDTOIdList.get(i))) {

					schedDTOStatuses[i] = new PlanDtoStatus(newSchedDTOList.get(i).getDTOId(), DtoStatus.Rejected);
				}

				aRIds[i] = newSchedDTOList.get(i).getARId();

				pRIds[i] = newSchedDTOList.get(i).getPRId();
			}

			// Set new statuses
			setNewStatuses(pSId, schedDTOStatuses);
			
			// Add previous statuses
			// TODO: compute statuses from reference PR, AR, DTO relevant to downloaded request in this MH 
			// Steps: 1. get Downloaded Tasks, 2. cycle over reference PRs, 3. set new statuses.

			/**
			 * The already scheduled DTOList
			 */
			ArrayList<SchedDTO> schedDTOList = (ArrayList<SchedDTO>) schedDTOListMap.get(pSId).clone();

			for (int j = 0; j < schedDTOIdList.size(); j++) {

				for (int k = 0; k < schedDTOList.size(); k++) {

					if (schedDTOList.get(k).getARId().equals(schedSol.get(j).getARId())) {

						schedDTOList.remove(k);
					}
				}

				schedDTOList.add(schedSol.get(j));
			}

			// Update the map of the scheduled list of DTOs
			schedDTOListMap.put(pSId, (ArrayList<SchedDTO>) schedDTOList.clone());
			
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Update the Delta-Plan statuses of the DTOs
	 *
	 * @param pSId
	 * @param initDTOList
	 * @param newSchedSol
	 */
	@SuppressWarnings("unchecked")
	public void setDeltaPlanStatuses(Long pSId, 
			ArrayList<SchedDTO> initDTOList, ArrayList<SchedDTO> newSchedSol) {
		
		logger.info("Update the Delta-Plan statuses for Planning Session: " + pSId);
		
		try {

			/**
			 * The DTO statuses
			 */
			PlanDtoStatus[] schedDTOStatuses = new PlanDtoStatus[newSchedSol.size()];

			/**
			 * The AR Ids
			 */
			String[] aRIds = new String[newSchedSol.size()];

			/**
			 * The PR Ids
			 */
			String[] pRIds = new String[newSchedSol.size()];

			/**
			 * The best AR DTO List
			 */
			ArrayList<String> initDTOIdList = new ArrayList<>();

			for (SchedDTO initDTO : initDTOList) {

				initDTOIdList.add(initDTO.getDTOId());
			}

			/**
			 * The new AR DTO Id List
			 */
			ArrayList<String> newDTOIdList = new ArrayList<>();

			// Update Current AR
			for (int i = 0; i < newSchedSol.size(); i++) {

				newDTOIdList.add(newSchedSol.get(i).getDTOId());

				// Update DTO status
				if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(newDTOIdList.get(i))) {

					schedDTOStatuses[i] = new PlanDtoStatus(newDTOIdList.get(i), DtoStatus.Rejected);

				} else {

					schedDTOStatuses[i] = new PlanDtoStatus(newDTOIdList.get(i), DtoStatus.Unused);
				}
			}

			for (int i = 0; i < newDTOIdList.size(); i++) {

				if (!Collections.disjoint(initDTOIdList, newDTOIdList)) {

					for (int j = 0; j < initDTOIdList.size(); j++) {

						if (initDTOIdList.get(j).equals(newDTOIdList.get(i))) {

							schedDTOStatuses[i] = new PlanDtoStatus(initDTOIdList.get(j), DtoStatus.Scheduled);

							if (!RequestChecker.hasEquivDTO(pSId,
									ObjectMapper.getSchedARId(initDTOIdList.get(j)))
									|| RequestChecker.hasLinkedDTO(pSId, initDTOIdList.get(j))) {

								break;
							}

						} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(newDTOIdList.get(i))) {

							schedDTOStatuses[i] = new PlanDtoStatus(newSchedSol.get(i).getDTOId(), DtoStatus.Rejected);

						} else {

							schedDTOStatuses[i] = new PlanDtoStatus(newSchedSol.get(i).getDTOId(), DtoStatus.Unused);
						}
					}

				} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(newDTOIdList.get(i))) {

					schedDTOStatuses[i] = new PlanDtoStatus(newSchedSol.get(i).getDTOId(), DtoStatus.Rejected);
				}

				aRIds[i] = newSchedSol.get(i).getARId();

				pRIds[i] = newSchedSol.get(i).getPRId();
			}

			// Set new statuses
			// setNewStatuses(pSId, bestSol, newAR, pRIds, aRIds, schedDTOStatuses);
			setNewStatuses(pSId, schedDTOStatuses);

			/**
			 * The already scheduled DTOList
			 */
			ArrayList<SchedDTO> schedDTOList = (ArrayList<SchedDTO>) schedDTOListMap.get(pSId).clone();

			// Update the map of the scheduled DTOs
			schedDTOListMap.put(pSId, (ArrayList<SchedDTO>) schedDTOList.clone());

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Set the new statuses of the requests elements Hp: rejection reasons are
	 * filled with the first reason of the list
	 *
	 * @param pSId
	 * @param schedDTOStatuses
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void setNewStatuses(Long pSId, PlanDtoStatus[] schedDTOStatuses) throws Exception {
	
		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
	
		// Initialize lists
		planDTOIdListMap.get(pSId).clear();
	
		rejDTOIdListMap.get(pSId).clear();
	
		rejARDTOIdSetMap.get(pSId).clear();
	
		/**
		 * The scheduled solution
		 */
		ArrayList<SchedDTO> schedSol = rulesPerformer.getAcceptedDTOs(pSId);
	
		/**
		 * The scheduled boolean
		 */
		int scheduled = 0;
		
		/**
		 * The PR counter
		 */
		int pRCounter = 0;
	
		for (PlanProgrammingRequestStatus pRStatus : (ArrayList<PlanProgrammingRequestStatus>)(
				(ArrayList<PlanProgrammingRequestStatus>)SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()).clone()) {
			
			/**
			 * The ARs counter
			 */
			int aRCounter = 0;
			
			/**
			 * The scheduled ARs counter
			 */
			int schedARCounter = 0;
	
			/**
			 * The replaced ARs counter
			 */
			int replARCounter = 0;
	
			/**
			 * The cancelled ARs counter
			 */
			int cancARCounter = 0;
			
			/**
			 * The failed ARs counter
			 */
			int failARCounter = 0;
						
			for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {
	
				/**
				 * The DTOs counter
				 */
				int dtoCounter = 0;
				
				/**
				 * The scheduled DTOs counter
				 */
				int schedCounter = 0;
	
				/**
				 * The failed DTOs counter
				 */
				int dtoFail = 0;
				
				/**
				 * The cancelled DTOs counter
				 */
				int dtoCanc = 0;
	
				/**
				 * The replaced DTOs counter
				 */
				int dtoRepl = 0;
	
				for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {
	
					/**
					 * The scheduling DTO Id
					 */
					String schedDTOId = ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
							pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
							dtoStatus.getDtoId());
	
					/**
					 * The scheduling boolean
					 */
					boolean schedBool = false;
	
					for (SchedDTO schedDTO : schedSol) {
	
						if (schedDTO.getDTOId().equals(schedDTOId)) {
	
							schedCounter ++;
							schedBool = true;
	
							logger.trace("Scheduled DTO at iteration: " + schedDTOId);
							
							break;
						}
					}
					
					if (EquivDTOHandler.slaveDTOIdListMap.get(pSId).contains(schedDTOId)) {
						
						schedCounter ++;
						schedBool = true;
					}
	
					// Update DTO status list
					if (schedBool) {
	
						logger.trace("Logged scheduled DTO: " + schedDTOId);
	
						// Set planned data
						dtoStatus.setStatus(DtoStatus.Scheduled);
						dtoStatus.setConflictDescription(null);
						dtoStatus.setConflictReasonId(null);
						dtoStatus.setActualBic(BICCalculator.getWorkDTOActualBIC(pSId, pRStatus.getUgsId(),
								pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
								dtoStatus.getDtoId()));
	
						// Update dtoId maps
						planDTOIdListMap.get(pSId).add(schedDTOId);
						rejDTOIdListMap.get(pSId).remove(schedDTOId);
						
						scheduled ++;
	
					} else if (HPCivilianRequestHandler.hpCivilDTOIdListMap.get(pSId).contains(schedDTOId)) {
							
							// TODO: check with Ground!
							dtoStatus.setStatus(DtoStatus.Cancelled);
							dtoStatus.setConflictDescription("Programming Request successfully replaced.");
							dtoStatus.setConflictReasonId(97);
							dtoRepl ++;
	
					} else if (SessionChecker.isDelta(pSId)
							&& (DeltaPlanProcessor.cancTotDTOIdListMap.get(pSId).contains(schedDTOId)
							|| dtoStatus.getStatus().equals(DtoStatus.Cancelled))) {
						
						// TODO: check with Ground!
						dtoStatus.setStatus(DtoStatus.Cancelled);
						dtoStatus.setConflictDescription("Cancelled due to Asynchronous replanning.");
						dtoStatus.setActualBic(0.0);
						dtoStatus.setConflictReasonId(98);
						dtoCanc ++;
							
					} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)
							&& PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(schedDTOId))
							.getKind() != null
							&& PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(schedDTOId))
							.getKind().equals(PRKind.SENTINEL)
							&& !FilterDTOHandler.filtRejDTOIdListMap.get(pSId).contains(schedDTOId)) {
						
						logger.trace("Logged Failed DTO: " + schedDTOId);
						
						// Set conflict data
						dtoStatus.setStatus(DtoStatus.Failed);
						dtoStatus.setActualBic(0.0);
						dtoStatus.setConflictDescription("DTO not filtered by the system.");
	
						dtoFail ++;
						
					} else if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)
						|| dtoStatus.getStatus().equals(DtoStatus.Rejected)) {
						
						logger.trace("Logged rejected DTO: " + schedDTOId);
	
						// Set conflict data
						dtoStatus.setStatus(DtoStatus.Rejected);
						dtoStatus.setActualBic(0.0);
						
						if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)
								&& ! RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId).isEmpty()) {
							dtoStatus.setConflictDescription(
									RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId).get(0).getDescription());
							dtoStatus.setConflictReasonId(
									RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId).get(0).getId());
						}
						
						rejDTOIdListMap.get(pSId)
								.add(ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
										pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
										dtoStatus.getDtoId()));
	
						planDTOIdListMap.get(pSId).remove(schedDTOId);
	
					} else if (!dtoStatus.getStatus().equals(DtoStatus.Rejected)
							&& !dtoStatus.getStatus().equals(DtoStatus.Failed)) {
	
						dtoStatus.setStatus(DtoStatus.Unused);
						dtoStatus.setActualBic(0.0);	
					}
	
					if (PRListProcessor.discardPRIdListMap.get(pSId).contains(
							ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), pRStatus.getProgrammingRequestId()))) {
	
						// TODO: check with Ground!
						dtoStatus.setStatus(DtoStatus.Cancelled);
						dtoStatus.setActualBic(0.0);
						dtoStatus.setConflictDescription("DTO discarded by manual replanning.");
						dtoStatus.setConflictReasonId(99);
						dtoCanc ++;
					}
	
					// Update PRStatus List
					aRStatus.getDtoStatusList().set(dtoCounter, dtoStatus);
	
					dtoCounter ++;
				}
				
				// Update ARStatus List
				if (schedCounter > 0) {
	
					aRStatus.setStatus(AcquisitionRequestStatus.Scheduled);
					aRStatus.setActualBic(BICCalculator.getWorkARActualBIC(pSId, pRStatus.getUgsId(),
							pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId()));
	
					if (PRListProcessor.schedARIdRankMap.get(pSId).containsKey(
							ObjectMapper.parseDMToSchedARId(pRStatus.getUgsId(),
									pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId()))) {
	
						aRStatus.setWeightedRank(PRListProcessor.schedARIdRankMap.get(pSId)
								.get(ObjectMapper.parseDMToSchedARId(pRStatus.getUgsId(),
										pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId())));
					}
	
					schedARCounter ++;
	
				} else {
	
					if (dtoFail > 0 || aRStatus.getStatus().equals(AcquisitionRequestStatus.Failed)) {
						
						aRStatus.setStatus(AcquisitionRequestStatus.Failed);
						
						failARCounter ++;
					
					} else if (dtoRepl == 0 && dtoCanc == 0) {
	
						aRStatus.setStatus(AcquisitionRequestStatus.Rejected);
	
					} else if (dtoRepl > 0) {
	
						aRStatus.setStatus(AcquisitionRequestStatus.Replaced);
	
						replARCounter ++;
	
					} else if (dtoCanc > 0) {
	
						aRStatus.setStatus(AcquisitionRequestStatus.Cancelled);
	
						cancARCounter ++;
					}
	
					aRStatus.setActualBic(0.0);
				}
	
				// Update PRStatus List
				pRStatus.getAcquisitionRequestStatusList().set(aRCounter, aRStatus);
	
				aRCounter ++;
			}
	
			// TODO: changed for FAT 11/05/18
			// if (schedARNum >=
			// pRStatus.getAcquisitionRequestStatusList().size()) {
			if (schedARCounter > 0) {
	
				pRStatus.setStatus(PRStatus.Scheduled);
	
				pRStatus.setActualBic(
						BICCalculator.getWorkPRActualBIC(pSId, pRStatus.getUgsId(), 
								pRStatus.getProgrammingRequestId()));
	
				// } else if (schedARNum > 0) {
				//
				// pRStatus.setStatus(PRStatus.Partial);
	
			} else {
	
				if (failARCounter > 0 || pRStatus.getStatus().equals(PRStatus.Failed)) {
					
					pRStatus.setStatus(PRStatus.Failed);
				
				} else if (replARCounter == 0 && cancARCounter == 0) {
	
					pRStatus.setStatus(PRStatus.Rejected);
	
				} else if (replARCounter > 0 || cancARCounter > 0) {
	
					pRStatus.setStatus(PRStatus.Cancelled);
				}
	
				pRStatus.setActualBic(0.0);
			}
	
			// Update the Planning Session map
			SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()
			.set(pRCounter, pRStatus);
	
			pRCounter ++;
		}
		
		logger.info("A number of " + scheduled + " DTO statuses is SCHEDULED.");
		
	}
	
	/**
	 * Set the rejected set of DTO Ids for the Planning Session
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 */
	@SuppressWarnings("unchecked")
	public void setRejDTOIds(Long pSId) {

		logger.info("Collect Rejected DTO Ids for Planning Session: " + pSId);
		
		if (rejARDTOIdSetMap.get(pSId).isEmpty()) {
	
			for (PlanProgrammingRequestStatus pRStatus : (ArrayList<PlanProgrammingRequestStatus>) 
					((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()).clone()) {
	
				for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {
	
					if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Rejected)) {
	
						for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {
	
							/**
							 * The rejected DTO Id
							 */
							String rejDTOId = ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
									pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
									dtoStatus.getDtoId());
	
							if (rejDTOIdListMap.get(pSId).contains(rejDTOId)) {
		
								// Add rejected DTO Id
								rejARDTOIdSetMap.get(pSId).add(rejDTOId);
							}
						}
					}
				}
			}
		}
	}
		
	/**
	 * Finalize the schedule according to the last rejection reasons
	 * TODO: Updated on 26/07/2022 for DI2S offline management
	 * @param pSId
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public boolean finalizeSchedule(Long pSId) throws Exception {

		logger.debug("Finalize Schedule for Planning Session: " + pSId);
		
		/**
		 * Instance handler
		 */		
		PredecessorCounter predCounter = new PredecessorCounter();
		
		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The output result
		 */
		boolean result = false;

		try {

			finalMap.put(pSId, finalMap.get(pSId) + 1);
			
			/**
			 * The list of scheduled DTOs
			 */
			ArrayList<SchedDTO> schedDTOList = (ArrayList<SchedDTO>) 
					rulesPerformer.getAcceptedDTOs(pSId).clone();
				
			// Added on 26/07/2022 -----
			
			for (SchedDTO inSchedDTO : (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId)) {
			
				if (EquivDTOHandler.di2sLinkedIdsMap.get(pSId) != null
						&& EquivDTOHandler.di2sLinkedIdsMap.get(pSId).containsKey(inSchedDTO.getDTOId())
						&& PRListProcessor.schedDTOMap.get(pSId).containsKey(
								EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId()))) {
					
					logger.debug("Added linked DTO: " + EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId()));
					
					// Add linked DTOs
					schedDTOList.add(PRListProcessor.schedDTOMap.get(pSId).get(
							EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(inSchedDTO.getDTOId())));
				}
			}
			
			// -----
			
			// 1.0. Set the planning statuses
			setPlanStatuses(pSId, schedDTOList, schedDTOList);
			
			// 1.1. Update the rejected DTO statuses
			setDTORejStatuses(pSId);
			
			// 1.2. Set rejected DTOs
			setRejDTOIds(pSId);
			
			// 1.3. Set owner Packet Store Ids
			// Added on 17/02/2021 to manage classified Packet Store Ids for owners
			setOwnerPacketStoreIds(pSId);
			
			// 2.0. Get Planned Tasks
			ArrayList<Task> outTaskList = getFinalPlanTasks(pSId);
		
			// 2.1. Check the download consistency
			if (RulesPerformer.checkDwlConsistency(pSId)) {
				
				logger.info("Planned Downloads are consistent for Planning Session " + pSId);
			
			} else {
				
				logger.warn("Planned Downloads are NOT consistent for Planning Session " + pSId);
			}
			
			// 2.2 Receive tasks for VU and LMP Planning Sessions
			if (SessionChecker.isDelta(pSId)) {

				// Get the Delta-Plan tasks	
				// TODO: it will change	according to sameContent method			
				outTaskList = getDeltaPlanTasks(pSId, outTaskList, 
								getDeltaPlanSatList(pSId ,outTaskList));
				
				outTaskList.addAll(getReprocessedTasks(pSId, true));

			} else {
				
				outTaskList.addAll(getReprocessedTasks(pSId, false));
			}
			
			// ---------
			
			// Added on 04/01/2022 for including downloads of visibilities in MH overlap -----------
			// Excluded on 28/02/2022 for SPLA-4.5.2
			// Re-added on 02/03/2022 for SPLA-4.5.3
			// Moved and updated on 26/05/2022 

			// Add previously scheduled downloads for visibilities in overlap
				
			// Get overlapping visibilities
			ArrayList<Task> prevProdList = getOverlapVisProds(pSId);
			
			// Add previous tasks
			outTaskList.addAll(prevProdList);
			
			if (! prevProdList.isEmpty())  {
				
				// Update DLO start times for visibilities in overlap
				refineOverlapVisDLOs(pSId, planDLOListMap.get(pSId), prevProdList);	
			}
			
			// Added on 09/02/2022 for check about empty DLOs
			for (int i = 0; i < outTaskList.size(); i++) {
				
				if (outTaskList.get(i).getTaskType().equals(TaskType.DWL)
					|| outTaskList.get(i).getTaskType().equals(TaskType.PASSTHROUGH)
					|| outTaskList.get(i).getTaskType().equals(TaskType.BITE)) {
				
					if (! isInsideDLO(outTaskList.get(i), planDLOListMap.get(pSId))) {
						
						logger.warn("Removed previously planned Product Task " + outTaskList.get(i).getTaskId()
								+ " because out of a valid DLO.");
						
						outTaskList.remove(i);
						
						i --;
					}
				}
			}
			
			Collections.sort(outTaskList, new TaskStartTimeComparator());
				
			// --------

			// 2.3 Assign the predecessors of the tasks
			predCounter.assignPredecessors(pSId, outTaskList);
		
			// 2.4. Collect new session tasks list		
			SessionActivator.planSessionMap.get(pSId).getTaskList().clear();
			SessionActivator.planSessionMap.get(pSId).getTaskList()
				.addAll((ArrayList<Task>) outTaskList.clone());
			
			// 3.0. Added on 17/02/2021 for owner Packet Store Id range consistency
			getPacketStoreIdOutRange((ArrayList<Task>) outTaskList.clone());

			// Clear the output tasks
			outTaskList.clear();

//			// 4.0. Get rejected DTOs
//			ArrayList<SchedDTO> finalRejDTOList = getRejDTOs(pSId);
//
//			// 4.1. Clear previously rejected DTOs
//			rulesPerformer.clearRejectedDTOs(pSId);
//
//			// 4.2.Check the rejected DTOList consistency
//			// TODO: consider unused DTOs for timeout
//			// TODO: check with time performance
//			logger.debug("Check the consistency of the rejected DTOs.");
//			rulesPerformer.planDTOList(pSId, finalRejDTOList, false);
//
//			// 4.3. Update the rejected DTO statuses
//			setDTORejStatuses(pSId);
			
//			// 4.4. Update Partners BICs (just once)
//			if (!SessionChecker.isDeltaSession(pSId) && finalMap.get(pSId) <= 1.01) {
//
//				BICCalculator.updatePartnersBICs(pSId);
//			}

//			// 4.5. Update PDHT switches ??
			
			result = true;

		} catch (Exception e) {

			logger.error("Error finalizing schedule {} - {}", pSId, e.getMessage(), e);
		}

		logger.info("Scheduling finalization of Planning Session: " + pSId + " is: " + result);

		return result;
	}

	/**
	 * Get the Delta-Plan satellite list
	 * @param pSId
	 * @param outTaskList
	 * @return
	 */
	private ArrayList<String> getDeltaPlanSatList(Long pSId, ArrayList<Task> outTaskList) {
		
		logger.info("Get the list of satellites impacted by the Delta-Plan for Planning Session: " + pSId);
		
		/**
		 * The list of satellites in the Delta Plan
		 */
		ArrayList<String> satIdList = new ArrayList<String>();
		
		for (Task outTask : outTaskList) {
			
			// Check Task type
			if (outTask.getTaskType().equals(TaskType.ACQ)) {
				
				// Check Ids
				if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
						ObjectMapper.parseDMToSchedPRId(outTask.getUgsId(), 
								outTask.getProgrammingRequestId()))) {
					
					if (! DeltaPlanProcessor.initDTOIdListMap.get(pSId).contains(
							ObjectMapper.parseDMToSchedDTOId(outTask.getUgsId(), 
									outTask.getProgrammingRequestId(), outTask.getAcquisitionRequestId(),
									outTask.getDtoId()))) {
						
						// Check satellite Id
						if (! satIdList.contains(outTask.getSatelliteId())) {
						
							// Add satellite Id
							satIdList.add(outTask.getSatelliteId());
						}
					}
				}
			}
		}
		
		return satIdList;
	}
	
	/**
	 * Set the rejected DTO statuses with final reasons.
	 *
	 * @param pSId
	 */
	private void setDTORejStatuses(Long pSId) {

		logger.info("Update the statuses of the rejected DTOs for Planning Session: " + pSId);
		
		/**
		 * Instance handlers
		 */
		ConflictDTOCalculator conflRankCalculator = new ConflictDTOCalculator();
	
		/**
		 * The PR counter
		 */
		int pRCounter = 0;

		for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()) {

			/**
			 * The AR counter
			 */
			int aRCounter = 0;

			for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {

				/**
				 * The DTO counter
				 */
				int dtoCounter = 0;

				for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {

					/**
					 * The DTO Id
					 */
					String schedDTOId = ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
							pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
							dtoStatus.getDtoId());

					// Update past AR DTO statuses, if changed
					if (rejDTOIdListMap.get(pSId).contains(schedDTOId)
							&& RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)) {

						/** 
						 * The PR Id
						 */
						String schedPRId = ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), 
								pRStatus.getProgrammingRequestId());
						
						if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)
								&& RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId) != null 
								&& !RulesPerformer.rejDTORuleListMap.get(pSId)
										.get(schedDTOId).isEmpty()) {
						
							dtoStatus.setConflictDescription(RulesPerformer.rejDTORuleListMap.get(pSId)
								.get(schedDTOId).get(0).getDescription());
							dtoStatus.setConflictReasonId(RulesPerformer.rejDTORuleListMap.get(pSId)
									.get(schedDTOId).get(0).getId());

							/**
							 * The conflicting request list
							 */
							ArrayList<ConflictingRequest> conflReqList = new ArrayList<ConflictingRequest>();
							
							/**
							 * The conflicting request
							 */
							ConflictingRequest conflReq = new ConflictingRequest();								
							
							// Add rank for National & NEO visibility only
							if (RequestChecker.isNational(PRListProcessor.pRSchedIdMap.get(pSId).get(
									schedPRId).getVisibility())
								|| RequestChecker.isNEO(PRListProcessor.pRSchedIdMap.get(pSId).get(
										schedPRId).getVisibility())
								|| RequestChecker.hasUniqueId(PRListProcessor.pRSchedIdMap.get(pSId).get(
										schedPRId))) {
							
								// The conflicting rank
								Integer rank = conflRankCalculator.getConflictRank(pSId, schedDTOId, 
										RulesPerformer.rejDTORuleListMap.get(pSId).get(schedDTOId)
										.get(0).getReason());
								
								if (rank != null) {
									
									conflReq.setRank(rank);
								}
							}
						
							// Add conflicting data for binary conflict only	
							if (ConflictDTOCalculator.isBinaryReason(
									RulesPerformer.rejDTORuleListMap.get(pSId)
									.get(schedDTOId).get(0).getReason())) {

								// Add conflict section
								addConflictSection(pSId, conflReq, schedDTOId);
								
								conflReqList.add(conflReq);
							}								
							
							dtoStatus.setConflicts(conflReqList);

						} else {
							
							dtoStatus.setConflictDescription("System Conflict.");
							dtoStatus.setConflictReasonId(1);
							logger.warn("Specific conflict description not found for DTO: " + schedDTOId);
						}						
						dtoStatus.setActualBic(0.0);

						logger.debug("Final Rejected DTO: " + schedDTOId);

						// Update the AR status
						aRStatus.getDtoStatusList().set(dtoCounter, dtoStatus);
					}

					if (dtoStatus.getStatus().equals(DtoStatus.Scheduled)) {

						dtoStatus.setConflictDescription(null);
						dtoStatus.setConflictReasonId(null);
					}

					dtoCounter ++;
				}

				if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Rejected)) {

					logger.info("Final Rejected AR: " + ObjectMapper.parseDMToSchedARId(pRStatus.getUgsId(),
							pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId()));
				}

				// Update the PR Status
				pRStatus.getAcquisitionRequestStatusList().set(aRCounter, aRStatus);

				aRCounter ++;
			}

			if (pRStatus.getStatus().equals(PRStatus.Rejected)) {

				logger.info("Complete Rejected PR: "
						+ ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), 
								pRStatus.getProgrammingRequestId()));

//			} else if (pRStatus.getStatus().equals(PRStatus.Partial)) {
//
//				logger.info("Partial Rejected PR: "
//						+ ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), pRStatus.getProgrammingRequestId()));
			}

			// Update the Planning Session map
			SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()
			.set(pRCounter, pRStatus);

			pRCounter ++;
		}
	}

	/**
	 * Add the conflict section to the DtoStatus for binary reason
	 * @param pSId
	 * @param conflReq
	 * @param rejDTOId
	 */
	private void addConflictSection(Long pSId, ConflictingRequest conflReq, String rejDTOId) {
	
		logger.debug("Add conflict section for rejected DTO: "  + rejDTOId);
		
		/**
		 * The iterator on involved elements
		 */
		Iterator<Entry<Double,List<ElementsInvolvedOnOrbit>>> it = RulesPerformer.rejDTORuleListMap.get(pSId)
				.get(rejDTOId).get(0).getElementsInvolved().entrySet().iterator();
	
		while (it.hasNext()) {
	
			/**
			 * The list of conflicting DTO Ids
			 */
			ArrayList<String> conflDTOIdList = new ArrayList<String>();
			
			for (ElementsInvolvedOnOrbit element : it.next().getValue()) {
		
				for (String conflDTOId : element.getElementsInvolved()) {
										
					if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey
							(ObjectMapper.getSchedARId(conflDTOId))) {
						/**
						 * The AR rank
						 */
						Integer rank = PRListProcessor.aRSchedIdMap.get(pSId).get(
								ObjectMapper.getSchedARId(conflDTOId)).getRank();
						
						if (rank != null && ! ObjectMapper.getSchedARId(rejDTOId).equals(
								ObjectMapper.getSchedARId(conflDTOId))) {
										
							logger.info("Conflicting DTO found: " + conflDTOId);
							
							// Add conflicting DTO
							conflDTOIdList.add(conflDTOId);
							
							conflReq.setUgsId(ObjectMapper.getUgsId(conflDTOId)); 
							conflReq.setOwnerId(SessionActivator.ugsOwnerIdMap.get(pSId)
									.get(ObjectMapper.getUgsId(conflDTOId)));
							conflReq.setProgrammingRequestId(ObjectMapper.getPRId(conflDTOId)); 
							conflReq.setAcquisitionRequestId(ObjectMapper.getARId(conflDTOId)); 
							conflReq.setDtoId(ObjectMapper.getDTOId(conflDTOId));
							conflReq.setProgrammingRequestListId(PRListProcessor.pRToPRListIdMap.get(pSId)
									.get(ObjectMapper.getSchedPRId(conflDTOId)).get(0));	
							
							break;
							
						}
					}
				}
			}						
		}
	}
	
	/**
	 * Collect the rejected DTOs
	 *
	 * @param pSId
	 * @return the list of rejected DTOs
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<SchedDTO> getRejectedDTOs(Long pSId) {

		logger.debug("Get rejected DTOs.");
		
		/**
		 * The final rejected DTO List
		 */
		ArrayList<SchedDTO> finalRejDTOList = new ArrayList<>();

		for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

				/**
				 * The list of rejected DTOs
				 */
				ArrayList<SchedDTO> rejDTOList = new ArrayList<>();

				/**
				 * The list of rejected DTO Ids
				 */
				ArrayList<String> rejDTOIdList = new ArrayList<>();

				// Add rejected DTOs
				for (DTO dto : aR.getDtoList()) {
					/**
					 * The scheduled DTO
					 */
					SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId, pR.getUserList().get(0).getUgsId(),
							pR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto,
							pR.getUserList().get(0).getAcquisitionStationIdList(), false);

					if (!planDTOIdListMap.get(pSId).contains(schedDTO.getDTOId())
							&& rejDTOIdListMap.get(pSId).contains(schedDTO.getDTOId())) {

						rejDTOList.add(schedDTO);

						rejDTOIdList.add(schedDTO.getDTOId());
					}
				}

				// if
				// (Collections.disjoint(ScheduleHandler.planDTOIdListMap.get(pSId),
				// rejDTOIdList)) {

				finalRejDTOList.addAll((ArrayList<SchedDTO>) rejDTOList.clone());
				// }
			}
		}

		logger.info("A number of " + finalRejDTOList.size() + 
				" DTOs is finally rejected for Planning Session: " + pSId);

		return finalRejDTOList;
	}

	/**
	 * Get the delta-planned Tasks 
	 * 
													  
	 * @param pSId
	 * @param deltaTaskList
	 * @param deltaPlanSatIdList
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Task> getDeltaPlanTasks(Long pSId, ArrayList<Task> deltaTaskList, 
			ArrayList<String> deltaPlanSatIdList) throws Exception {

		logger.debug("Update Delta-Plan Tasks for Planning Session: " + pSId);
		
		// Sort Tasks
		Collections.sort(deltaTaskList, new TaskStartTimeComparator());
		
		/**
		 * The output task list
		 */
		ArrayList<Task> outTaskList = new ArrayList<>();

		/**
		 * The Asynchronous Plan Offset Time
		 */
		long deltaPlanStartTime = SessionActivator.planSessionMap.get(pSId)
				.getMissionHorizonStopTime().getTime();
		
		if (! DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId).isEmpty()) 
		{
			// Get delta plan start time
			deltaPlanStartTime = Collections.min(DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId));
			
			// Check and update in case of overlap with a macroDLO
			deltaPlanStartTime = checkDLOOverlap(pSId, deltaPlanStartTime);
			
			logger.info("The Delta-Plan start time equals: " + (new Date(deltaPlanStartTime)).toString()
					+ " for Planning Session: " + pSId);
		}

		/**
		 * The date formatter
		 */
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		logger.info("Set tasks statuses according to the Delta-Plan offset time: "
				+ formatter.format(new Date(deltaPlanStartTime)));
		
		logger.debug("Clone the previously scheduled Tasks for the Delta-Plan.");

		/**
		 * The task status number lists
		 */
		List<Integer> unchTaskNumList = new ArrayList<>();
		List<Integer> confTaskNumList = new ArrayList<>();
		List<Integer> newTaskNumList = new ArrayList<>(); 
		List<Integer> delTaskNumList = new ArrayList<>(); 

		// Initialize lists
		for (int i = 0; i < satListMap.get(pSId).size(); i++) 
		{	
			unchTaskNumList.add(0);
			confTaskNumList.add(0);
			newTaskNumList.add(0);
			delTaskNumList.add(0);
		}

		for (Task workTask : (ArrayList<Task>)PersistPerformer.workTaskListMap.get(pSId).clone()) 
		{	
   
			/**
			 * The scheduling DTO Id
			 */
			String schedDTOId = ObjectMapper.parseDMToSchedDTOId(workTask.getUgsId(), workTask.getProgrammingRequestId(), 
			workTask.getAcquisitionRequestId(), workTask.getDtoId());
						
			if (! workTask.getTaskMark().equals(TaskMarkType.DELETED)
					&& workTask.getTaskType().equals(TaskType.DLO)) 
			{
				/**
				 * The DLO task
				 */
				Task dloTask = (Task) workTask.cloneModel();
				dloTask.getMacroActivityList().clear();

				if (isPreviousDLO(pSId, (DLO) dloTask, deltaPlanStartTime)) {
					
					// Check unchanged status
					if (isUnchangedTask(workTask, deltaPlanSatIdList, deltaPlanStartTime)) 
					{
						logger.debug("Task cloned as UNCHANGED: " + workTask.getTaskType() + " for contact counter "
						+ ((DLO) workTask).getContactCounter() + " for acquisition station " 
								+ ((DLO) workTask).getAcquisitionStationId() + " for satellite " + workTask.getSatelliteId());
						dloTask.setTaskMark(TaskMarkType.UNCHANGED);
						
						if (workTask.getSatelliteId().contains("1")) {
							unchTaskNumList.set(0, unchTaskNumList.get(0) + 1);
						} else {
							unchTaskNumList.set(1, unchTaskNumList.get(1) + 1);				
						}
					} 
					// Check confirmed status
					else 
					{
						logger.debug("Task cloned as CONFIRMED: " + workTask.getTaskType() + " for contact counter: "
						+ ((DLO) workTask).getContactCounter() + " for acquisition station: " 
								+ ((DLO) workTask).getAcquisitionStationId() + " for satellite: " + workTask.getSatelliteId());
						dloTask.setTaskMark(TaskMarkType.CONFIRMED);
						
						if (workTask.getSatelliteId().contains("1")) {
							confTaskNumList.set(0, confTaskNumList.get(0) + 1);
						} else {
							confTaskNumList.set(1, confTaskNumList.get(1) + 1);				
						}					
					}					
				} 
				// Check deleted status
				else 
				{
					logger.debug("Task cloned as DELETED: " + workTask.getTaskType(), " for contact counter: "
					+ ((DLO) workTask).getContactCounter() + " for acquisition station: " 
							+ ((DLO) workTask).getAcquisitionStationId() + " for satellite: " + workTask.getSatelliteId());
					
					dloTask.setTaskMark(TaskMarkType.DELETED);
					
					if (workTask.getSatelliteId().contains("1")) {
						delTaskNumList.set(0, delTaskNumList.get(0) + 1);
					} else {
						delTaskNumList.set(1, delTaskNumList.get(1) + 1);				
					}
				}					
				
				// Add DLO
				outTaskList.add(dloTask);
			}
			// Check unchanged
			else if (isUnchangedTask(workTask, deltaPlanSatIdList, deltaPlanStartTime))
			{			
				logger.debug("Task cloned as UNCHANGED: " + workTask.getTaskType() + " for DTO Id: "
						+ schedDTOId + " for satellite: " + workTask.getSatelliteId());
				
				/**
				 * The unchanged task
				 */
				Task unchTask = (Task) workTask.cloneModel();
				
				// Remove all macro-activities associated with the current task
				unchTask.getMacroActivityList().clear();
				unchTask.setTaskMark(TaskMarkType.UNCHANGED);
				outTaskList.add(unchTask);
				
				if (workTask.getSatelliteId().contains("1")) {
					unchTaskNumList.set(0, unchTaskNumList.get(0) + 1);
				} else {
					unchTaskNumList.set(1, unchTaskNumList.get(1) + 1);					
				}
			} 
			// Check confirmed
			else if (isConfirmedTask(pSId, workTask, deltaPlanSatIdList, deltaPlanStartTime)
					&& ! RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTOId)
					&& ! DeltaPlanProcessor.cancTotDTOIdListMap.get(pSId).contains(schedDTOId)) 
			{
				logger.debug("Task cloned as CONFIRMED: " + workTask.getTaskType() + " for DTO Id: "
						+ schedDTOId + " for satellite: " + workTask.getSatelliteId());
				/**
				 * The confirmed task
				 */
				Task confTask = (Task) workTask.cloneModel();
				confTask.getMacroActivityList().clear();
				confTask.setTaskMark(TaskMarkType.CONFIRMED);
				outTaskList.add(confTask);
				if (workTask.getSatelliteId().contains("1")) {
					confTaskNumList.set(0, confTaskNumList.get(0) + 1);
				} else {
					confTaskNumList.set(1, confTaskNumList.get(1) + 1);				
				}
			}   
			else
			// Check deleted status
			{				
				logger.debug("Task cloned as DELETED: " + workTask.getTaskType() 
				+ " for DTO Id: " + schedDTOId);
				/**
				 * The deleted task
				 */
				Task delTask = (Task) workTask.cloneModel();
				
				// Remove all macro-activities associated with the current task
				delTask.getMacroActivityList().clear();
				delTask.setTaskMark(TaskMarkType.DELETED);
				outTaskList.add(delTask);
				if (workTask.getSatelliteId().contains("1")) {
					delTaskNumList.set(0, delTaskNumList.get(0) + 1);
				} else {
					delTaskNumList.set(1, delTaskNumList.get(1) + 1);					
				}
			}
		}

		// Set Starting Delta PacketStore
		setStartDeltaOwnerPSIds(pSId);
		
		// Add the new scheduled tasks in the Delta-Plan
		outTaskList.addAll(getNewDeltaTasks(pSId, deltaTaskList, deltaPlanSatIdList, deltaPlanStartTime,
				 unchTaskNumList, confTaskNumList, newTaskNumList, delTaskNumList));
		
//		// Added on 26/05/2022 to count the acrossMH DLOs
//		// Deleted on 27/05/2022 to count the acrossMH DLOs
//		outTaskList.addAll(getAcrossMHDLOs(pSId, deltaTaskList, deltaPlanSatIdList, deltaPlanStartTime));
		
		// Sort output tasks
		Collections.sort(outTaskList, new TaskStartTimeComparator());
		
		ArrayList<Store> stoList = new ArrayList<Store>();
		
		// Update NEW download packet store data
		for (Task outTask : outTaskList) {
						
			if (outTask.getTaskType().equals(TaskType.STORE)) {
				
				stoList.add((Store)outTask);
			}
									
			// Check Download Type
			if (outTask.getTaskType().equals(TaskType.DWL) 
					&& outTask.getProgrammingRequestId() != null
					&& outTask.getTaskMark().equals(TaskMarkType.NEW)) {
				
				// Reassign Download Packet Store Id for previously planned downloads
				Long packetStoreId = updateDwlPacketStoreId(pSId, (Download)outTask, stoList);
				
				if (packetStoreId > 0) {
					
					((Download) outTask).setPacketStoreId(Long.toString(packetStoreId));	
				
				} else {
					
					// VU/LMP packet store Id
					logger.debug("New Packet Store Id for NEW Download Task: " + outTask.getTaskId());
				}		
			}
		}
		
		for (int i = 0; i < satListMap.get(pSId).size(); i++) {
		
			logger.info("Delta-Plan for satellite " + satListMap.get(pSId)
					.get(i).getCatalogSatellite().getSatelliteId());
			logger.info("Unchanged Task number: " + unchTaskNumList.get(i) + 
					", Confirmed Task number: " + confTaskNumList.get(i) +
					", New Task number: " + newTaskNumList.get(i) + 
					", Deleted Task number: " + delTaskNumList.get(i));
		}
		
		// Sort Tasks by start time
		Collections.sort(outTaskList, new TaskStartTimeComparator());

		return outTaskList;
	}
	
	/**
	 * Get the list of new Task for the Delta-Plan
	 * 
	 * @param pSId
	 * @param deltaTaskList
	 * @param deltaPlanSatIdList
	 * @param deltaPlanStartTime
	 * @param unchTaskNumList
	 * @param confTaskNumList
	 * @param newTaskNumList
	 * @param delTaskNumList
	 * @return
	 */						   
	@SuppressWarnings("unchecked")
	private ArrayList<Task> getNewDeltaTasks(Long pSId, ArrayList<Task> deltaTaskList,
			ArrayList<String> deltaPlanSatIdList, Long deltaPlanStartTime,
			List<Integer> unchTaskNumList, List<Integer> confTaskNumList, 
			List<Integer> newTaskNumList, List<Integer> delTaskNumList) {
		
		logger.info("Compute the NEW scheduled tasks in the Delta-Plan for Planning Session: " + pSId);
		
		/**
		 * Instance handler
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		/**
		 * The new task list
		 */
		ArrayList<Task> newTaskList = new ArrayList<Task>();
		
		for (Task deltaTask : (ArrayList<Task>) deltaTaskList.clone()) {			

			if (!isUnchangedTask(deltaTask, deltaPlanSatIdList, deltaPlanStartTime) 
					&& !isConfirmedTask(pSId, deltaTask, deltaPlanSatIdList, deltaPlanStartTime)) {
			
				// Check Task Type
				if (deltaTask.getTaskType().equals(TaskType.STORE)) {
					
					/**
					 * The owner Id
					 */
					String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(deltaTask.getUgsId());
					
					/**
					 * The scheduling PR Id
					 */
					String schedPRId = ObjectMapper.parseDMToSchedPRId(deltaTask.getUgsId(),
							deltaTask.getProgrammingRequestId());
					
					if (((Store) deltaTask).getPacketStoreIdH() != null 
							&& ((Store) deltaTask).getPacketStoreIdH() > 0) {
						
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(schedPRId) 
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(schedPRId)
								&& (equivDTOHandler.getDI2SVisibility(pSId, deltaTask) == 0)) {
						
							logger.debug("Manage international PS for PR: " +  schedPRId); 
							
							((Store) deltaTask).setPacketStoreIdH((long) 
									intMinPSIdMap.get(pSId));
									
							// Update counter
							intMinPSIdMap.put(pSId, (long) intMinPSIdMap.get(pSId) + 1);
						
						// Standard case
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {
	   						
							((Store) deltaTask).setPacketStoreIdH((long) ownerMinPSIdMap.get(pSId).get(ownerId));
							
							// Update counter
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) + 1);
						}
					}
					
					if (((Store) deltaTask).getPacketStoreIdV() != null 
							&& ((Store) deltaTask).getPacketStoreIdV() > 0) {
						
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(schedPRId) 
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(schedPRId)
								&& (equivDTOHandler.getDI2SVisibility(pSId, deltaTask) == 0)) {
							
							logger.debug("Manage International PS for PR: " +  schedPRId); 
							
							((Store) deltaTask).setPacketStoreIdV((long) intMinPSIdMap.get(pSId));
									
							// Update counter
							intMinPSIdMap.put(pSId, (long) intMinPSIdMap.get(pSId) + 1);
						
						// Standard case
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {

							((Store) deltaTask).setPacketStoreIdV((long) ownerMinPSIdMap.get(pSId).get(ownerId));
							
							// Update counter
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) + 1);			
						}
					}
					
				} else if (deltaTask.getTaskType().equals(TaskType.PASSTHROUGH)) {
					
					/**
					 * The owner Id
					 */
					String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(deltaTask.getUgsId());

					/**
					 * The scheduling PR Id
					 */
					String schedPRId = ObjectMapper.parseDMToSchedPRId(deltaTask.getUgsId(),
							deltaTask.getProgrammingRequestId());
					
					// International case
					if (((PassThrough) deltaTask).getPacketStoreHId() != null 
							&& Long.valueOf(((PassThrough) deltaTask).getPacketStoreHId()) > 0) {
			
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(schedPRId) 
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(schedPRId)
								&& (equivDTOHandler.getDI2SVisibility(pSId, deltaTask) == 0)) {
							
							logger.debug("Manage international PS for PR: " +  schedPRId); 
							
							((PassThrough) deltaTask).setPacketStoreHId(Long.toString(
									intMinPSIdMap.get(pSId)));
									
							// Update counter
							intMinPSIdMap.put(pSId, (long) intMinPSIdMap.get(pSId) + 1);
						
					  
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {
		
							((PassThrough) deltaTask).setPacketStoreHId(Long.toString(
									ownerMinPSIdMap.get(pSId).get(ownerId)));
							
							// Update counter
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) + 1);					
						}
					}
					
					// Standard case
					if (((PassThrough) deltaTask).getPacketStoreVId() != null 
							&& Long.valueOf(((PassThrough) deltaTask).getPacketStoreVId()) > 0) {
							  
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(schedPRId)
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(schedPRId)
								&& (equivDTOHandler.getDI2SVisibility(pSId, deltaTask) == 0)) {
							
							logger.debug("Manage international PS for PR: " +  schedPRId); 
							
							((PassThrough) deltaTask).setPacketStoreHId(Long.toString(
									intMinPSIdMap.get(pSId)));
									
							// Update counter
							intMinPSIdMap.put(pSId, (long) intMinPSIdMap.get(pSId) + 1);
							
						// Standard case
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {

							((PassThrough) deltaTask).setPacketStoreVId(Long.toString(
									ownerMinPSIdMap.get(pSId).get(ownerId)));
							
							// Update counter											 
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) + 1);
						}
					}
				}
			}
			
			// Check DLO 
			if (deltaTask.getTaskType().equals(TaskType.DLO)) {
				
				logger.info("check DLO for Delta-Plan");
				
				if (!isPreviousDLO(pSId, (DLO) deltaTask, deltaPlanStartTime)) {
				
					// Mark new task			
					logger.debug("Task marked as NEW: " + deltaTask.getTaskType() + " for DLO Id: "
							+ ObjectMapper.parseDMToSchedDTOId(deltaTask.getUgsId(), deltaTask.getProgrammingRequestId(),
									deltaTask.getAcquisitionRequestId(), deltaTask.getDtoId())
							+ " for satellite: " + deltaTask.getSatelliteId());
					
					// Remove all the associated macroActivity, if present
					deltaTask.getMacroActivityList().clear();					
					deltaTask.setTaskMark(TaskMarkType.NEW);
					// Compute packet store Id for task
					newTaskList.add(deltaTask);
					
					if (deltaTask.getSatelliteId().contains("1")) {
						newTaskNumList.set(0, newTaskNumList.get(0) + 1);
					} else {
						newTaskNumList.set(1, newTaskNumList.get(1) + 1);					
					}
				}


			} else if (!isUnchangedTask(deltaTask, deltaPlanSatIdList, deltaPlanStartTime) 
						&& !isConfirmedTask(pSId, deltaTask, deltaPlanSatIdList, deltaPlanStartTime)) {
				
				// Mark new task
				logger.debug("Task marked as NEW: " + deltaTask.getTaskType() + " for DTO Id: "
						+ ObjectMapper.parseDMToSchedDTOId(deltaTask.getUgsId(), 
								deltaTask.getProgrammingRequestId(),
								deltaTask.getAcquisitionRequestId(), deltaTask.getDtoId())
						+ " for satellite: " + deltaTask.getSatelliteId());
				
				// Remove all the associated macroActivity, if present
				deltaTask.getMacroActivityList().clear();					
				deltaTask.setTaskMark(TaskMarkType.NEW);
				newTaskList.add(deltaTask);
				
				if (deltaTask.getSatelliteId().contains("1")) {
					newTaskNumList.set(0, newTaskNumList.get(0) + 1);
				} else {
					
					newTaskNumList.set(1, newTaskNumList.get(1) + 1);					
				}			
			}		
		}
		
		return newTaskList;
	}
	
//	/**
//	 * get the DLOs starrting in the previous MH
//	 * // Added on 26/05/2022 to manage across MH DLOs in Delta-Plan
//
//	 * @param pSId
//	 * @param deltaTaskList
//	 * @param deltaPlanSatIdList
//	 * @param deltaPlanStartTime
//	 * @return
//	 */
//	@SuppressWarnings("unchecked")
//	private static ArrayList<Task> getAcrossMHDLOs(Long pSId, ArrayList<Task> deltaTaskList,
//			ArrayList<String> deltaPlanSatIdList, Long deltaPlanStartTime) {
//		
//		logger.info("Compute the PREVIOUS scheduled tasks in the Delta-Plan for Planning Session: " + pSId);
//		
//		// Compute the visibilities in overlap with the MH
//		computeMHOverVisList(pSId);
//		
//		/**
//		 * The new task list
//		 */
//		ArrayList<Task> prevTaskList = new ArrayList<Task>();
//		
//		for (Task deltaTask : (ArrayList<Task>) deltaTaskList.clone()) {			
//
//			// Check DLO 
//			if (deltaTask.getTaskType().equals(TaskType.DLO)) {
//				
//				 if (isPreviousDLO(pSId, (DLO)deltaTask, deltaPlanStartTime)
//						 && isMHOverlapVisTask(pSId, ((DLO)deltaTask).getVisibilityStartDateTime(), 
//								 ((DLO)deltaTask).getAcquisitionStationId(), deltaTask.getSatelliteId())) {
//					 
//					// Mark new task
//					logger.debug("Task marked as UNCHANGED: " + deltaTask.getTaskType() 
//							+ " for satellite: " + deltaTask.getSatelliteId());
//					
//					// Remove all the associated macroActivity, if present
//					deltaTask.getMacroActivityList().clear();					
//					deltaTask.setTaskMark(TaskMarkType.UNCHANGED);
//					 
//					 prevTaskList.add(deltaTask);
//				 }
//			}		
//		}
//		
//		return prevTaskList;
//	}
	
	/**
	 * Get the Download packet store Id
	 * 
	 * @param pSId
	 * @param dwl
	 * @param stoList
	 * @return
	 */
	private Long updateDwlPacketStoreId(Long pSId, Download dwl, ArrayList<Store> stoList) {
		
		try {
		
			// Cycle storage tasks
			for (Task task : stoList) {
								
				Store sto = (Store) task;
	
				if (!sto.getTaskMark().equals(TaskMarkType.DELETED)) {
					
					if (sto.getUgsId().equals(dwl.getUgsId())
						&& sto.getProgrammingRequestId().equals(dwl.getProgrammingRequestId())
						&& sto.getAcquisitionRequestId().equals(dwl.getAcquisitionRequestId())
						&& sto.getDtoId().equals(dwl.getDtoId())) {
						
						logger.debug("Update Packet Store of the relevant storage for NEW Download: " + dwl.getTaskId());
	
						// Check polarization
						if (sto.getSourcePacketNumberH() != null
								&& sto.getSourcePacketNumberH().doubleValue() > 0
								&& dwl.getSourcePacketNumberH() != null 
								&& dwl.getSourcePacketNumberH().doubleValue() > 0) {
							
							// Return PacketStoreId H
							return sto.getPacketStoreIdH();
						
						} else if (sto.getSourcePacketNumberV() != null
								&& sto.getSourcePacketNumberV().doubleValue() > 0
								&& dwl.getSourcePacketNumberV() != null
								&& dwl.getSourcePacketNumberV().doubleValue() > 0) {
							
							// Return PacketStoreId V
							return sto.getPacketStoreIdV();
						}
					}
				}
			}
	
			// Check reference tasks
			if (PersistPerformer.refTaskListMap.containsKey(pSId)) {
				
				for (Task refTask : PersistPerformer.refTaskListMap.get(pSId)) {
					
					if (! refTask.getTaskMark().equals(TaskMarkType.DELETED)) {
				
						if (dwl != null && refTask != null) {
						
							if (refTask.getUgsId() != null && refTask.getUgsId().equals(dwl.getUgsId())
									&& refTask.getProgrammingRequestId().equals(dwl.getProgrammingRequestId())
									&& refTask.getAcquisitionRequestId().equals(dwl.getAcquisitionRequestId())
									&& refTask.getDtoId().equals(dwl.getDtoId())) {
								
								logger.debug("Update Packet Store of the relevant storage for Reference Download: " + dwl.getTaskId());
								
								// Check polarization
								if (((Store)refTask).getSourcePacketNumberH() != null && ((Store)refTask).getSourcePacketNumberH() != 0
										&& dwl.getSourcePacketNumberH().doubleValue() > 0) {
									
									// Return PacketStoreId H
									return ((Store) refTask).getPacketStoreIdH();
								
								} else if (((Store)refTask).getSourcePacketNumberV() != null && ((Store)refTask).getSourcePacketNumberV() != 0
										&& dwl.getSourcePacketNumberV().doubleValue() > 0) {
									
									// Return PacketStoreId V
									return ((Store) refTask).getPacketStoreIdV();		
								}					
							}
							
						} else {
							
							logger.warn("Null download found! ");
						}
					}
				}
			}
			
			// Null packet store id
			logger.warn("Null packet store Id found for Task: " + dwl.getTaskId() + " of DTO: " + ObjectMapper.parseDMToSchedDTOId(
					dwl.getUgsId(), dwl.getProgrammingRequestId(), dwl.getAcquisitionRequestId(), dwl.getDtoId()));
		
		} catch (Exception ex) {
			
			// Null packet store id
			logger.warn("Null packet store Id found for Task: " + dwl.getTaskId() + " of DTO: " + ObjectMapper.parseDMToSchedDTOId(
					dwl.getUgsId(), dwl.getProgrammingRequestId(), dwl.getAcquisitionRequestId(), dwl.getDtoId()));
		}
			
		return 0L;
	}
			
	/**
	 * Check if input DLO equals previous DLO
	 * 
	 * @param pSId
	 * @param dlo
	 * @param deltaPlanStartTime
	 * @return
	 */
	private boolean isPreviousDLO(Long pSId, DLO dlo, long deltaPlanStartTime) {
		
		/**
		 * The previous boolean
		 */
		boolean isPrevious = false;
		
//		for (Task prevTask : (ArrayList<Task>) PersistPerformer.workTaskListMap.get(pSId).clone()) {
//			
//			// Check Task Type
//			if (prevTask.getTaskType().equals(TaskType.DLO)) {				
//				
//				if (((DLO)dlo).getStartTime().getTime() >= (deltaPlanStartTime)) { 
//
//					// Set previous 
//					isPrevious = true;
//					
//					if (dlo.getSatelliteId().equals(prevTask.getSatelliteId()) 
//							&& ((DLO)dlo).getAcquisitionStationId().equals(((DLO)prevTask).getAcquisitionStationId())
//							&& ((DLO)dlo).getContactCounter().equals(((DLO)prevTask).getContactCounter())
//							&& ((DLO)dlo).getStartCarrier().getTime() == ((DLO)prevTask).getStartCarrier().getTime()
//							&& ((DLO)dlo).getStopCarrier().getTime() == ((DLO)prevTask).getStopCarrier().getTime()) {
//							
//						// Set previous 
//						isPrevious = true;
//						
//						
//						logger.info("Found previous DLO for acquisition station " + ((DLO)prevTask).getAcquisitionStationId()
//								+ " and contact counter " + ((DLO)prevTask).getContactCounter()
//								+ " with start time " + ((DLO)prevTask).getStartTime() 
//								+ "and stop time " + ((DLO)prevTask).getStopTime());
//						
//						break;			
//					}
//					
//				} else {
//					
//					// Set previous 
//					isPrevious = true;
//
//					logger.info("Found previous DLO for acquisition station " + dlo.getAcquisitionStationId()
//						+ " and contact counter " + dlo.getContactCounter()
//						+ " with start time " + dlo.getStartTime() 
//						+ "and stop time " + dlo.getStopTime());
//
//					
//					break;
//				}
//			}
//		}
				
		if (((DLO)dlo).getStartTime().getTime() < (deltaPlanStartTime)) { 

			logger.debug("Found previous DLO for acquisition station " + dlo.getAcquisitionStationId()
			+ " and contact counter " + dlo.getContactCounter()
			+ " with start time " + dlo.getStartTime() 
			+ " and stop time " + dlo.getStopTime());
			
			// Set previous 
			isPrevious = true;
							
		} else {
			
			logger.debug("Found new DLO for acquisition station " + dlo.getAcquisitionStationId()
					+ " and contact counter " + dlo.getContactCounter()
					+ " with start time " + dlo.getStartTime() 
					+ " and stop time " + dlo.getStopTime());
		}
		
		return isPrevious;	
	}
	
	/**
	 * Check if the task is CONFIRMED
	 * 
	 * @param workTask
	 */
	private boolean isConfirmedTask(Long pSId, Task task, ArrayList<String> satIdList,
			Long asyncPlanOffsetTime) {
		
		/**
		 * The confirmed boolean
		 */
		boolean isConfirmed = false;
		
		// Check Task Type
		if (!task.getTaskMark().equals(TaskMarkType.DELETED)) {
			
			if (task.getTaskType().equals(TaskType.ACQ) 
//				|| task.getTaskType().equals(TaskType.SILENT)
//				|| task.getTaskType().equals(TaskType.STORE) 
				|| task.getTaskType().equals(TaskType.STOREAUX) 
				|| task.getTaskType().equals(TaskType.BITE)) {
				
				// Set confirmed
				isConfirmed = true;
				
				for (SchedDTO deltaPlanDTO : DeltaPlanProcessor.deltaSchedDTOListMap.get(pSId)) {
					
					if (deltaPlanDTO.getDTOId().contains(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(), 
							task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))) {
						
						// Unset confirmed
						isConfirmed = false;
						
						break;
					}			
				}
			
			} else if (!satIdList.contains(task.getSatelliteId())
							&& task.getStopTime().getTime() >= asyncPlanOffsetTime) {
				
				isConfirmed = true;
			}
		}
		
		return isConfirmed;
	}
	
	/**
	 * Check if the task is UNCHANGED
	 * @param task
	 * @param satIdList
	 * @param asyncPlanOffsetTime
	 * @return
	 */
	private boolean isUnchangedTask(Task task, ArrayList<String> satIdList, 
			Long asyncPlanOffsetTime) {
		
		/**
		 * The unchanged boolean
		 */
		boolean isUnchanged = false;
			
		// Deleted on 18/05/2021
		if (!task.getTaskMark().equals(TaskMarkType.DELETED)) {
			
			if (task.getStopTime().getTime() < asyncPlanOffsetTime) {

				// Set unchanged
				isUnchanged = true;
				
//			} else if ((!satIdList.contains(task.getSatelliteId())
//						&& task.getStopTime().getTime() < asyncPlanOffsetTime)) {
//			
//				// Set unchanged
//				isUnchanged = true;
			}
		}
		
		return isUnchanged;
	}
		

//	/**
//	 * Check if the task is ignored
//	 * @param task
//	 * @param satIdList
//	 * @param asyncPlanOffsetTime
//	 * @return
//	 */
//	private static boolean isIgnoredTask(Task task, ArrayList<String> satIdList, 
//			Long asyncPlanOffsetTime) {
//		
//		/**
//		 * The unchanged boolean
//		 */
//		boolean isIgnored = false;
//			
//		// Deleted on 18/05/2021
//		if (task.getTaskMark().equals(TaskMarkType.DELETED)) {
//			
//			if (task.getStopTime().getTime() < asyncPlanOffsetTime) {
//
//				// Set ignored
//				isIgnored = true;
//			}		
//		}
//		
//		return isIgnored;
//	}

	/**
	 * Get the final planned Tasks
	 *
	 * @param pSId
	 * @return the final list of tasks
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Task> getFinalPlanTasks(Long pSId) throws Exception {

		logger.debug("Get the final set of planned Tasks for Planning Session: " + pSId);

		/**
		 * Instance handler
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		TaskPlanner taskPlanner = new TaskPlanner();
		
		EquivDTOHandler equivDTOHandler =  new EquivDTOHandler();

		/**
		 * The Data Model tasks list
		 */
		ArrayList<Acquisition> outAcqList = new ArrayList<>();

		ArrayList<Store> outStoList = new ArrayList<>();

		ArrayList<Maneuver> outManList = new ArrayList<>();

		ArrayList<PassThrough> outPTList = new ArrayList<>();

		ArrayList<Ramp> outRampList = new ArrayList<>();

		ArrayList<Download> outDwlList = new ArrayList<>();

		ArrayList<Silent> outSilList = new ArrayList<>();

		ArrayList<CMGAxis> outCMGAxisList = new ArrayList<>();

		ArrayList<StoreAux> outStoreAuxList = new ArrayList<>();

		ArrayList<BITE> outBiteList = new ArrayList<>();

		ArrayList<Task> outProdTaskList = new ArrayList<>();
		
		ArrayList<Task> outTaskList = new ArrayList<>();

		try {
			
			// Update scheduled tasks
			rulesPerformer.updateSchedTasks(pSId, true);

			/**
			 * The scheduled task list from BRM
			 */
			ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Task> brmTaskList = RulesPerformer
					.brmWorkTaskListMap.get(pSId);

			// Sort task by time
			Collections.sort(brmTaskList, new BRMTaskTimeComparator());

			/**
			 * The task Id value
			 */
			int taskId = BigInteger.valueOf(pSId % 1000).intValue();
			
			/**
			 * The task counter
			 */
			int taskCount = PersistPerformer.refTaskListMap.get(pSId).size();
			
			/**
			 * The MH task counter output
			 */
			int outMHTaskCount = 0;
			
			/**
			 * The starting packet store Id for the Nominal Plan
			 */
			setStartNomOwnerPSIds(pSId); 
				
			// Init temp store Id list
			storeIdList =  new ArrayList<>();
			
			// Cycle task list
			for (com.nais.spla.brm.library.main.ontology.tasks.Task task : brmTaskList) {							

				taskCount ++;
				
				if (task.getStartTime().compareTo(SessionActivator.planSessionMap.get(pSId)
						.getMissionHorizonStopTime()) <= 0 
						&& task.getEndTime().compareTo(SessionActivator.planSessionMap.get(pSId)
								.getMissionHorizonStartTime()) > 0) {
		
					// Acquisition Task
					if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.ACQUISITION)) {
	
	
						// Evaluate the task schedulability result
						checkSchedulability(pSId, task);
	
						/**
						 * The task Id
						 */
						int acqTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan acquisition tasks
						outAcqList.add(TaskPlanner.planAcqTask(pSId, task, acqTaskId));
	
					// Maneuver Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.MANEUVER)) {
	
						/**
						 * The task Id
						 */
						int manTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan maneuver task
						outManList.add(TaskPlanner.planManTask(pSId, task, manTaskId));
	
					// Ramp Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.RAMP)) {
	
						/**
						 * The task Id
						 */
						int rampTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan ramp task
						outRampList.add(TaskPlanner.planRampTask(pSId, task, rampTaskId));
	
					// Storage Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.STORE)) {

						/**
						 * The task Id
						 */
						int storeTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
							
						/**
						 * The owner Id
						 */
						String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(
								((com.nais.spla.brm.library.main.ontology.tasks.Storage) task).getUgsId());
						
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(task.getIdTask()))
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(ObjectMapper.getSchedPRId(task.getIdTask()))
								&& (equivDTOHandler.getDI2SVisibility(pSId, task) == 0)) {
							
							logger.debug("Manage international PS for PR: " +  ObjectMapper.getSchedPRId(task.getIdTask())); 
							
							// Plan store task
							outStoList.add(TaskPlanner.planStoreTask(pSId, task, storeTaskId, outAcqList, 
									intMinPSIdMap.get(pSId).intValue()));
							
							// Handle multiple polarization
							if (((com.nais.spla.brm.library.main.ontology.tasks.Storage) task).getPacketStoreSizeH() > 0
									&& ((com.nais.spla.brm.library.main.ontology.tasks.Storage) task).getPacketStoreSizeV() > 0) {

								intMinPSIdMap.put(pSId, intMinPSIdMap.get(pSId) + 1);
								
								if (!storeIdList.contains(intMinPSIdMap.get(pSId).intValue())) {									
									
									storeIdList.add(intMinPSIdMap.get(pSId).intValue());								
								
								} else {
									
									logger.warn("Packet store Id " + intMinPSIdMap.get(pSId) 
										+ " already in use!");
								}
							}
								
							intMinPSIdMap.put(pSId, intMinPSIdMap.get(pSId) +  1);
						
					    // Standard case
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {
						
							// Plan store task
							outStoList.add(TaskPlanner.planStoreTask(pSId, task, storeTaskId, outAcqList, 
									ownerMinPSIdMap.get(pSId).get(ownerId).intValue()));
							
							// Handle multiple polarization
							if (((com.nais.spla.brm.library.main.ontology.tasks.Storage) task).getPacketStoreSizeH() > 0
									&& ((com.nais.spla.brm.library.main.ontology.tasks.Storage) task).getPacketStoreSizeV() > 0) {

								ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) +  1);
								
								if (!storeIdList.contains(ownerMinPSIdMap.get(pSId).get(ownerId).intValue())) {									
									
									storeIdList.add(ownerMinPSIdMap.get(pSId).get(ownerId).intValue());								
								
								} else {
									
									logger.warn("Packet store Id " + ownerMinPSIdMap.get(pSId).get(ownerId) 
											+ " already in use!");
								}
							}
								
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) +  1);
							
							if (!storeIdList.contains(ownerMinPSIdMap.get(pSId).get(ownerId).intValue())) {							
								
								storeIdList.add(ownerMinPSIdMap.get(pSId).get(ownerId).intValue());						
							
							} else {
								
								logger.warn("Packet Store Id " + ownerMinPSIdMap.get(pSId).get(ownerId) + " already in use!");
							}
						}

					// PassThrough
					} else if (task.getTaskType()
							.equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.PASSTHROUGH)) {
	
						/**
						 * The owner Id
						 */
						String ownerId = ((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).getUgsOwnerList().get(0);
											
						/**
						 * The task Id
						 */
						int ptTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
			
						// Evaluate the schedulability result
						checkSchedulability(pSId, task);
							
						// International case
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(task.getIdTask()))
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(ObjectMapper.getSchedPRId(task.getIdTask()))
								&& (equivDTOHandler.getDI2SVisibility(pSId, task) == 0)) {					
							
								logger.debug("Manage international PS for PR: " +  ObjectMapper.getSchedPRId(
										ObjectMapper.getSchedPRId(task.getIdTask()))); 

							/**
							 * The Passthrough Task
							 */
							PassThrough pT = new PassThrough();
											
							// Plan passThrough
							pT = TaskPlanner.planPTTask(pSId, task, ptTaskId, intMinPSIdMap.get(pSId).intValue());
						
							// Handle multiple polarization
							if (((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).getPacketStoreSizeH() > 0
									&& ((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).getPacketStoreSizeV() > 0) {
	
								intMinPSIdMap.put(pSId, intMinPSIdMap.get(pSId) +  1);
								
								if (!storeIdList.contains(intMinPSIdMap.get(pSId).intValue())) {							
								
									storeIdList.add(intMinPSIdMap.get(pSId).intValue());						
								
								} else {							
								
									logger.warn("Packet store Id " + intMinPSIdMap.get(pSId) + " already in use!");
								}
							}
														
//							ownerMinPSIdMap.get(pSId).put(ownerId, intMinPSIdMap.get(pSId) +  1);
							intMinPSIdMap.put(pSId, intMinPSIdMap.get(pSId) +  1);

							
							if (!storeIdList.contains(intMinPSIdMap.get(pSId).intValue())) {							
								
								storeIdList.add(intMinPSIdMap.get(pSId).intValue());						
							
							} else {
								
								logger.warn("Packet store Id " + intMinPSIdMap.get(pSId) + " already in use!");				
							}
							
							outPTList.add(pT);
						
						// Standard case
						} else if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)) {
							
							/**
							 * The Passthrough Task
							 */
							PassThrough pT = new PassThrough();
											
							// Plan passThrough
							pT = TaskPlanner.planPTTask(pSId, task, ptTaskId, ownerMinPSIdMap.get(pSId).get(ownerId).intValue());
						
							// Handle multiple polarization
							if (((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).getPacketStoreSizeH() > 0
									&& ((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).getPacketStoreSizeV() > 0) {
	
								ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) +  1);
								
								if (!storeIdList.contains(ownerMinPSIdMap.get(pSId).get(ownerId).intValue())) {							
									
									storeIdList.add(ownerMinPSIdMap.get(pSId).get(ownerId).intValue());						
								
								} else {
									
									logger.warn("Packet store Id " + ownerMinPSIdMap.get(pSId).get(ownerId) + " already in use!");
								}
							}
							
							ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId) +  1);
							
							if (!storeIdList.contains(ownerMinPSIdMap.get(pSId).get(ownerId).intValue())) {							
								
								storeIdList.add(ownerMinPSIdMap.get(pSId).get(ownerId).intValue());						
							
							} else {
								
								logger.warn("Packet store Id " + ownerMinPSIdMap.get(pSId).get(ownerId) + " already in use!");
							}
							
							outPTList.add(pT);
						}
						
					// Download Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.DOWNLOAD)
							|| task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.GPS)) {
	
						/**
						 * The task Id
						 */
						int dlwTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan download task					
						Download dwl = TaskPlanner.planDwlTask(pSId, task, dlwTaskId, outStoList);
						
						if (dwl != null) {
							
							outDwlList.add(dwl);	
						}
						
					// Silent Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.SILENT)) {
	
						/**
						 * The task Id
						 */
						int silTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan silent task
						outSilList.add(TaskPlanner.planSilTask(pSId, task, silTaskId));

					// Axes Reconfiguration Task
					} else if (task.getTaskType()
							.equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.AXES_RECONF)) {
	
						/**
						 * The task Id
						 */
						int cmgAxisTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan CMGAxis task
						outCMGAxisList.add(TaskPlanner.planCMGAxisTask(pSId, task, cmgAxisTaskId));
	
					// StoreAux Task
					} else if (task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.STORE_AUX)) {
	
						/**
						 * The task Id
						 */
						int storeAuxTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
	
						// Plan storeAux task
						outStoreAuxList.add(TaskPlanner.planStoreAuxTask(pSId, task, storeAuxTaskId));
	
					// Bite Task
					} else if (task instanceof Bite 
							|| task.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.BITE)) {
	
						/**
						 * The task Id
						 */
						int biteTaskId = Integer.parseInt(Integer.toString(taskId) + Integer.toString(taskCount));
						
						// Evaluate the task schedulability result
						checkSchedulability(pSId, task);
	
						// Plan bite task
						outBiteList.add(TaskPlanner.planBiteTask(pSId, task, biteTaskId));						

//						outProdTaskList.add(TaskPlanner.planBiteTask(pSId, task, biteTaskId));
					} else {
						
						logger.error("No task associable to: " + task.getIdTask() 
							+ " of type: " + task.getTaskType());
					}

				} else {
					
					logger.debug("The task related to the following DTO: " + task.getIdTask()  
						+ " of type: " + task.getTaskType() + " is outside of the relevant Mission Horizon.");			
					
					outMHTaskCount ++;
				}			
			}
			
			logger.debug("A number of scheduled tasks: " + outMHTaskCount 
				+ " is outside of the relevant Mission Horizon: " + pSId);

			logger.info("The final number of scheduled tasks: " + (taskCount - outMHTaskCount) 
				+ " is planned by BRM for Planning Session: " + pSId);
			
//			// Fill the download data with the MacroDLO data (bite and passthrough data are not assigned) 
			taskPlanner.fillDwlData(pSId, outDwlList);
			
			logger.info("Collect the Tasks list relevant to the current Planning Session: " + pSId);

			// Add scheduled Tasks
			outTaskList.addAll(outAcqList);
			outTaskList.addAll(outManList);
			outTaskList.addAll(outRampList);
			outTaskList.addAll(outStoList);
			outTaskList.addAll(outSilList);
			outTaskList.addAll(outStoreAuxList);
			outTaskList.addAll(outCMGAxisList);
			outTaskList.addAll(outBiteList);
 
			// Add product Tasks
			outProdTaskList.addAll(outDwlList);
			outProdTaskList.addAll(outPTList);
			
			Collections.sort(outProdTaskList, new TaskStartTimeComparator());
			
			// Clear temporary store Id list
			storeIdList.clear();
			
			// Set dummy tasks parameters
			logger.debug("Set dummy tasks parameters where not achievable.");

			/**
			 * The list of start dates
			 */
			ArrayList<String> startDateList = new ArrayList<String>();
			
			for (Task outTask : outTaskList) {
				
				if (startDateList.contains(outTask.getStartTime().toString())) {
					
					logger.warn("Task with same start time " + outTask.getStartTime().toString() 
					+ " is found for task: " + outTask.getTaskId());
				}
				
				startDateList.add(outTask.getStartTime().toString());
								
				if (outTask.getTaskType().equals(TaskType.MANEUVER)) {

					logger.debug(outTask.getTaskType() + " planned between DTOs: "
							+ ((Maneuver) outTask).getAcqIdFrom() + " and " + ((Maneuver) outTask).getAcqIdTo());
				
				} else if (outTask.getTaskType().equals(TaskType.STOREAUX)) {
					
					logger.debug(outTask.getTaskType() + " planned by BRM for PacketStoreId: "
					+ ((StoreAux) outTask).getPacketStoreId());

				} else {

					logger.debug(outTask.getTaskType() + " planned for DTO: "
							+ ObjectMapper.parseDMToSchedDTOId(outTask.getUgsId(), outTask.getProgrammingRequestId(),
									outTask.getAcquisitionRequestId(), outTask.getDtoId()));
				}
			}
			
			/**
			 * The DLO Task list
			 */
			ArrayList<DLO> outDLOList = TaskPlanner.planDLOTaskList(pSId, taskId, taskCount, 
					outProdTaskList, outBiteList);
			
			try {				
				
				if (!Configuration.debugEKMFlag) {
	
					Date outDate = new Date();
					
					/**
					 * The new EkmLib
					 */
					EKMLIB ekmLib = new EKMLIB();
					
					logger.info("Import Product Tasks in EKMLib...");
					
					for (Task outProdTask : outProdTaskList) {
						
						if (startDateList.contains(outProdTask.getStartTime().toString())) {
							
							logger.warn("Task having start time " + outProdTask.getStartTime().toString() 
							+ " is found for product task: " + outProdTask.getTaskId());
						}
												
						startDateList.add(outProdTask.getStartTime().toString());
												
						logger.debug("Input Product to EKMLib: " + outProdTask.getTaskId());

					}
					
					startDateList.clear();
					
					// The output directory
					String outFolder = RulesPerformer.brmParamsMap.get(pSId).getResponceFile();
					
					// Serialize the output Product list
					MessageHandler.serializeObjectToFile(outProdTaskList, 
							outFolder + File.separator + pSId + "-ProductList_" + outDate.getTime() + ".txt");
					
					logger.debug("Input DLO to EKMLib: " + outDLOList.toString());
					
					// Serialize the output DLO list
					MessageHandler.serializeObjectToFile(outDLOList, 
							outFolder + File.separator + + pSId + "-DLOList_" + outDate.getTime() + ".txt");
					
					logger.debug("Number of imported Tasks in EKMLib is: " + outProdTaskList.size());					
					
					// Generate sequence Ids from encryption
					ekmLib.generateSequenceId((ArrayList<DLO>) outDLOList.clone(), outProdTaskList);
						
					logger.debug("Number of exported Tasks from EKMLib is: " + outProdTaskList.size());					
										
					for (Task outProdTask : outProdTaskList) {

						logger.debug(outProdTask.getTaskType() + " product Task planned by BRM: "
								+ outProdTask.getTaskId() + " from: " + outProdTask.getStartTime().toString() 
								+ " to: " + outProdTask.getStopTime().toString()
								+ " for satellite " + outProdTask.getSatelliteId());
						
						if (outProdTask.getProgrammingRequestId() == null) {
							 
							// Added on 29/09/2021 for GPS DWL management from CSPS-1.11.8 
							if (!Configuration.defGPSAddParam1.contentEquals("NA"))  {
							
								if (outProdTask.getTaskType().equals(TaskType.DWL)) {
								
									((Download) outProdTask).setAdditionalPar1(Configuration.defGPSAddParam1);
							
								} else if (outProdTask.getTaskType().equals(TaskType.PASSTHROUGH)) {
								
									((PassThrough) outProdTask).setAdditionalPar1(Configuration.defGPSAddParam1);
									
								}
							}
						}
						
						logger.debug("Output Product from EKMLib: " + outProdTask.getTaskId());	
					}
					
					for (Task outDLO : outDLOList) {

						logger.debug(outDLO.getTaskType() + " Task exploited by BRM: "
								+ outDLO.getTaskId() + " from: " + outDLO.getStartTime().toString() 
								+ " to: " + outDLO.getStopTime().toString()
								+ " for satellite " + outDLO.getSatelliteId());
						
						logger.debug("Output DLO Id from EKMLib: " + outDLO.getTaskId());					
					}
				}
				
			} catch (Exception ex) {
				
				logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
				logger.error("EKMLib output is corrupted. Product sequence Ids NOT generated.");	
			}
			
//			// Added on 04/01/2022 for including downloads of visibilities in MH overlap -----------
//			// Excluded on 28/02/2022 for SPLA-4.5.2
//			// Re-added on 02/03/2022 for SPLA-4.5.3
//			// Commented here and moved to finalizeSchedule on 26/05/2022 
//
////			// Add previously scheduled downloads for visibilities in overlap
////			if (! SessionChecker.isDelta(pSId))  {
//				
//				// Get overlapping visibilities
//				ArrayList<Task> prevProdList = getOverlapVisProds(pSId);
//				
//				// Add previous tasks
//				outProdTaskList.addAll(prevProdList);
//				
//				if (! prevProdList.isEmpty())  {
//					
//					// Update DLO start times for visibilities in overlap
//					refineOverlapVisDLOs(pSId, outDLOList, prevProdList);	
//				}
////			}
//			
//			// Added on 09/02/2022 for check about empty DLOs
//			for (int i = 0; i < outProdTaskList.size(); i++) {
//				
//				if (! isInsideDLO(outProdTaskList.get(i), outDLOList)) {
//					
//					logger.warn("Removed previously planned Product Task " + outProdTaskList.get(i).getTaskId()
//							+ " because out of a valid DLO.");
//					
//					outProdTaskList.remove(i);
//					
//					i --;
//				}
//			}
				
			// --------
			
			// Add last set of scheduled Tasks
			outTaskList.addAll((ArrayList<Task>) outProdTaskList.clone());
			outTaskList.addAll((ArrayList<DLO>) outDLOList.clone());
			
			// Added on 26/05/2022 to manage across MH DLOs in Delta-Plan
			planDLOListMap.put(pSId, (ArrayList<DLO>) outDLOList.clone());

			logger.info("Number of Task scheduled within the MH:");
			logger.info("The number of scheduled Acquisitions is: " + outAcqList.size());
			logger.info("The number of scheduled Silents is: " + outSilList.size());
			logger.info("The number of scheduled Storages is: " + outStoList.size());
			logger.info("The number of scheduled Maneuvers is: " + outManList.size());
			logger.info("The number of scheduled Ramps is: " + outRampList.size());
			logger.info("The number of scheduled Downloads is: " + outDwlList.size());
			logger.info("The number of scheduled Passthroughs is: " + outPTList.size());
			logger.info("The number of scheduled Bite is: " + outBiteList.size());
			logger.info("The number of scheduled StoreAux is: " + outStoreAuxList.size());
			logger.info("The number of exploited DLOs is: " + outDLOList.size());
			
			logger.info("The number of scheduled Tasks within the current Mission Horizon is: " + outTaskList.size());

			// if (SessionChecker.isDeltaPlanSession(pSId)) { // TODO: it is repeated
			//
			// // TODO: check that deleted tasks are not duplicated
			// logger.debug("Update Tasks Statuses for Delta-Plan schedule.");
			// outTaskList = updateTasksStatuses(pSId, outTaskList);
			// }
			
			
		} catch (Exception e) {

			logger.error("Error computing tasks {} - {}", pSId, e.getMessage(), e);
		}
		
		return outTaskList;
	}
	

	/**
	 * Check the schedulability of a task
	 *
	 * @param pSId
	 * @param task
	 * @return
	 */
	private boolean checkSchedulability(Long pSId, com.nais.spla.brm.library.main.ontology.tasks.Task task) {

		/**
		 * The check result
		 */
		boolean result = false;

		// logger.debug("Analyze task schedulability within a number of " +
		// schedARListMap.get(pSId).size() + " requests.");

		for (SchedAR schedAR : schedARListMap.get(pSId)) {

			for (SchedDTO dto : schedAR.getDtoList()) {

				// Update scheduled ARList
				if (task.getIdTask().equals(dto.getDTOId())) {

//					logger.debug("Acquisition of exploited DTO " + dto.getDTOId() + " from - to: "
//							+ dto.getStartTime().toString() + " - " + dto.getStopTime().toString());

					// Update status		
					schedAR.setARStatus(AcquisitionRequestStatus.Scheduled);
					
					if (!SessionChecker.isUnranked(pSId)
							&& !SessionChecker.isSelf(pSId)) {

						// Check Manual Session
						if (SessionChecker.isManual(pSId)) {

							if (ObjectMapper.getSchedARId(task.getIdTask())
									.equals(ManualPlanProcessor.manPlanARMap.get(pSId).getARId())) {
	
								// Set result
								result = true;
	
								logger.info("Schedulability of acquisition related to DTO " 
								+ task.getIdTask() + " is: " + result);
							}							
						
						} else { // Check NOT manual session
							
							if (ObjectMapper.getSchedARId(task.getIdTask())
									.equals(NextARProcessor.nextSchedARMap.get(pSId).getARId())) {
	
								// Set result
								result = true;
	
								logger.info("Schedulability of acquisition related to DTO " 
								+ task.getIdTask() + " is: "+ result);
							}
						}
					}

					break;
				}
			}
		}

		return result;
	}

	/**
	 * Update the Planning Session Progress Report
	 *
	 * @param pSId
	 */
	public void setProgressReport(Long pSId) {
		
		logger.debug("Update the Progress Reports for Planning Session: " + pSId);
	
		/**
		 * Instance handlers
		 */
		DroolsUtils droolsUtils = new DroolsUtils();
		
		try {

			logger.info("Build the INGESTION Progress Reports...");
			
			// Cycle for each UGS Id 
			for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

				/**
				 * The impacted owner
				 */
				Owner owner = SessionActivator.ownerListMap.get(pSId).get(i);

				logger.info("Build Progress Report BICs for owner: " + owner.getCatalogOwner().getOwnerId());
				
//				ownerBICRepMap.get(pSId).clear();
				
				/**
				 * The Partner BIC Report
				 */
				Double[] partnerBICs = BICCalculator.computeBICReport(pSId,
					SessionActivator.partnerListMap.get(pSId).get(i), owner);

				/**
				 * The list of BIC loans
				 */
				List<BICLoan> loanList = new ArrayList<BICLoan>();

				/**
				 * The BRM partner
				 */
				com.nais.spla.brm.library.main.ontology.resources.Partner brmPartner = droolsUtils.
						receivePartnerWithId(owner.getCatalogOwner().getOwnerId(), pSId.toString(), 
								RulesPerformer.brmInstanceMap.get(pSId));

				// Load Partners Debits
				for (DebitCard debitCard : brmPartner.getLoanList()) {

					loanList.add(new BICLoan(debitCard.getCreditor(), debitCard.getBICBorrowed()));
				}

				/**
				 * The BIC Report for partner
				 */
				BICReport bicReport = new BICReport(partnerBICs[0], partnerBICs[1], partnerBICs[2], 
						partnerBICs[3], partnerBICs[4], partnerBICs[6], loanList);

				if (SessionActivator.ownerARIdMap.get(pSId).containsKey(owner.getCatalogOwner().getOwnerId())) {

					// Added on 22/1/2021 for the management of Progress Report per UGS Id 
					for (CatalogUgs catUGS : owner.getCatalogOwner().getUgsList()) {

						/**
						 * The Progress Report
						 */
						ProgressReport progReport = new ProgressReport("PS-" + pSId.toString(), ReportType.INGESTION,
								catUGS.getUgsId(), owner.getCatalogOwner().getOwnerId(), bicReport);

						// Add Progress Report Info within the Planning Session
						SessionActivator.planSessionMap.get(pSId).addProgressReportInfo(progReport);

						logger.debug("Progress Report Info added for UGS: " + catUGS.getUgsId());
					}
				}
			}
			
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Count the request statuses of the session
	 *
	 * @param pSId
	 */
	private void countSessionStatuses(Long pSId) {
		
//		/**
//		 * The PR Status counter [Scheduled/Partial/Rejected/Cancelled/New]
//		 */
//		Integer[] pRStatusCounts = { 0, 0, 0, 0, 0 };
		
		/**
		 * The PR Status counter [Scheduled/Rejected/Cancelled/Failed/New]
		 */
		Integer[] pRStatusCounts = { 0, 0, 0, 0, 0};

		/**
		 * The AR Status counter [Scheduled/Rejected/Cancelled/Replaced/Failed/New]
		 */
		Integer[] aRStatusCounts = { 0, 0, 0, 0, 0, 0};

		/**
		 * The DTO Status counter [Scheduled/Rejected/Cancelled/Failed/Unused]
		 */
		Integer[] dtoStatusCounts = { 0, 0, 0, 0, 0};
		
		ArrayList<PlanDtoStatus> dtoStatusList = new ArrayList<PlanDtoStatus>();
		
		for (PlanProgrammingRequestStatus pRStatus :SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()) {

			if (pRStatus.getStatus().equals(PRStatus.Scheduled)) {

				pRStatusCounts[0]++;

//			} else if (pRStatus.getStatus().equals(PRStatus.Partial)) {
//
//				pRStatusCounts[1]++;

			} else if (pRStatus.getStatus().equals(PRStatus.Rejected)) {

				pRStatusCounts[1]++;

			} else if (pRStatus.getStatus().equals(PRStatus.Cancelled)) {

				pRStatusCounts[2]++;

			} else if (pRStatus.getStatus().equals(PRStatus.Failed)) {

				pRStatusCounts[3]++;
			}

			for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {

				if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Scheduled)) {

					aRStatusCounts[0]++;

				} else if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Rejected)) {

					aRStatusCounts[1]++;

				} else if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Cancelled)) {

					aRStatusCounts[2]++;
				
				} else if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Replaced)) {

					aRStatusCounts[3]++;
				
				} else if (aRStatus.getStatus().equals(AcquisitionRequestStatus.Failed)) {

					aRStatusCounts[4]++;
				
				} else {
					
					aRStatusCounts[5]++;
				}

				for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {

					if (dtoStatus.getStatus().equals(DtoStatus.Scheduled)) {

						dtoStatusCounts[0]++;

					} else if (dtoStatus.getStatus().equals(DtoStatus.Rejected)) {

						dtoStatusCounts[1]++;					
						dtoStatusList.add(dtoStatus);

					} else if (dtoStatus.getStatus().equals(DtoStatus.Cancelled)) {

						dtoStatusCounts[2]++;
						dtoStatusList.add(dtoStatus);

					} else if (dtoStatus.getStatus().equals(DtoStatus.Failed)) {

						dtoStatusCounts[3]++;
					
					} else {
						
						dtoStatusCounts[4]++;
					}
				}
			}
		}

//		logger.info("Statuses of the incoming PRs in the Planning Session " + pSId + ": [Scheduled = "
//				+ pRStatusCounts[0] + "]" + "[Partial = " + pRStatusCounts[1] + "]" + "[Rejected = " + pRStatusCounts[2]
//				+ "]" + "[Cancelled = " + pRStatusCounts[3] + "]" + "[New = " + pRStatusCounts[4] + "]");

		logger.info("Statuses of the incoming PRs in the Planning Session " + pSId + ": [Scheduled = "
				+ pRStatusCounts[0] + "]" + "[Rejected = " + pRStatusCounts[1] + "]" + "[Cancelled = " 
				+ pRStatusCounts[2] + "]" + "[Failed = " + pRStatusCounts[3] + "]" + "[New = " 
				+ pRStatusCounts[4] + "]");

		
		logger.info("Statuses of the incoming ARs in the Planning Session " + pSId + ": [Scheduled = "
				+ aRStatusCounts[0] + "]" + "[Rejected = " + aRStatusCounts[1] + "]" + "[Cancelled = "
				+ aRStatusCounts[2] + "]" + "[Replaced = " + aRStatusCounts[3] + "]" + "[Failed = " 
				+ aRStatusCounts[4] + "]" + "[New = " + aRStatusCounts[5] + "]");

		logger.info("Statuses of the incoming DTOs in the Planning Session " + pSId + ": [Scheduled = "
				+ dtoStatusCounts[0] + "]" + "[Rejected = " + dtoStatusCounts[1] + "]" + "[Cancelled = "
				+ dtoStatusCounts[2] + "]" + "[Failed = " + dtoStatusCounts[3] + "]" + "[Unused = " 
				+ dtoStatusCounts[4] + "]");
		
		logger.info("Number of jumps achieved for the Planning Session " + pSId 
				+ ": " +  RankPerformer.jumpMap.get(pSId));
	}
	
	/**
	 * Set the starting owner packet store Ids for Nominal Plan
	 * TODO: Modified on 05/05/2022 for international PS management 
	 * TODO: Adapted on 10/05/2022 for international PS rolling 
	 * @param pSId
	 * @return
	 */
	private void setStartNomOwnerPSIds(Long pSId) {
		
		logger.info("Get the first Id usable for Packet Store of Nominal Plan "
				+ "for Planning Session: " + pSId);

		if (PersistPerformer.refTaskListMap.containsKey(pSId)) {
		
			for (Task task : PersistPerformer.refTaskListMap.get(pSId)) {
				
				if (RequestChecker.isInsidePrevMH(pSId, task.getStartTime().getTime())) {
				
					// Check STORE Task
					if (task.getTaskType().equals(TaskType.STORE)) {
						
						logger.debug("Check PS of PR: " 
						+ ObjectMapper.parseDMToSchedPRId(task.getUgsId(), task.getProgrammingRequestId()));
						
						/**
						 * The owner Id
						 */
						String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(task.getUgsId());
						
						if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)
								&& ownerMinPSIdMap.get(pSId).get(ownerId) != null) {
							
							// Updated on 21/04/2022 and 24/05/2022 to handle owner packet Store Id range
							if (((Store) task).getPacketStoreIdH() != null
									&& ((Store) task).getPacketStoreIdH() > ownerMinPSIdMap.get(pSId).get(ownerId)
									&& checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdH())) {

									
								// Update PS Id
								ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdH());
	
							} else if (((Store) task).getPacketStoreIdV() != null
																				
									&&((Store) task).getPacketStoreIdV() > ownerMinPSIdMap.get(pSId).get(ownerId) 
									&& checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdV())) {
									
								// Update PS Id
								ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdV());

							}
							
							// Updated on 21/04/2022 and 24/05/2022 to handle International packet Store Id range
							if (((Store) task).getPacketStoreIdH() != null
																						
									&&((Store) task).getPacketStoreIdH() > intMinPSIdMap.get(pSId)
									&& checkPacketStoreIdRange("SUBSCRIPTIONS", ((Store) task).getPacketStoreIdH())) {

								
																	 
								intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdH());
								
							} else if (((Store) task).getPacketStoreIdV() != null
																						
									&&((Store) task).getPacketStoreIdV() > intMinPSIdMap.get(pSId)				
									&& checkPacketStoreIdRange("SUBSCRIPTIONS", ((Store) task).getPacketStoreIdV())) {
								
																	 
								intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdV());						
							}
	 
						} else {
							
							logger.warn("No configured minimum PS Id found for owner of ugs: " 
									+ task.getUgsId());
						}
					}
				}
			}
		}
				
		// Initialize output map
		for (String ownerId : ownerMinPSIdMap.get(pSId).keySet()) {

			// Update Owner PS Ids according to MH change
			ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId)
				+ Configuration.deltaPacketNumber);
	
			if (catOwnerMaxPSIdMap.containsKey(ownerId)) {
			
				if (ownerMinPSIdMap.get(pSId).get(ownerId) > catOwnerMaxPSIdMap.get(ownerId) - 1000) {
					
					// Reset PS Ids
					logger.debug("Reset the minimum PS Id for the Owner " + ownerId 
							+ " which is: " + ownerMinPSIdMap.get(pSId).get(ownerId));
					
					ownerMinPSIdMap.get(pSId).put(ownerId, catOwnerMinPSIdMap.get(ownerId));
				}
				
			} else {
				
				logger.warn("No configured minimum PS Id found for owner: " + ownerId);
				 
			}
			
			logger.debug("PS Id range for Nominal Plan of owner: " + ownerId
					+ " from/to [" + ownerMinPSIdMap.get(pSId).get(ownerId) + "; "
					+ catOwnerMaxPSIdMap.get(ownerId) + "] for Planning Session: " + pSId);
		}
				
		// Update International PS Ids according to MH change
		intMinPSIdMap.put(pSId, (long) (intMinPSIdMap.get(pSId)
			+ ((long)Configuration.deltaPacketNumber / 10.0)));

		
		if (intMinPSIdMap.get(pSId) > catIntMaxPSId - 100) {
			
			// Reset International PS Ids
			logger.debug("Reset the minimum international PS Id "
					+ "which is: " + intMinPSIdMap.get(pSId));

			intMinPSIdMap.put(pSId, catIntMaxPSId);
		}
		
		logger.debug("PS Id range for Nominal Plan of international services: "
				+ " from/to [" + intMinPSIdMap.get(pSId) + "; "
				+ catIntMaxPSId + "] for Planning Session: " + pSId);
	}
	
	/**
	 * Set the starting owner packet store Id for Delta Plan
	 * TODO: Modified on 05/05/2022 for international PS Ids management 
	 * TODO: Adapted on 10/05/2022 for international PS Ids rolling 
	 * @param pSId
	 * @return
	 */
	private void setStartDeltaOwnerPSIds(Long pSId) {
			
		logger.info("Get the first Id usable for Packet Stores of Delta Plan "
				+ "for Planning Session: " + pSId);
		
//		// TODO: erased on 10/05/2022 to manage the cases in which the VU/LMP shall reset the PSId rannges
//		
//		if (PersistPerformer.refTaskListMap.containsKey(pSId)) {
//			
//			for (Task task : PersistPerformer.refTaskListMap.get(pSId)) {
//							
//				if (RequestChecker.isInsidePrevMH(pSId, task.getStartTime().getTime())) {
//					
//					// Check STORE Task
//					if (task.getTaskType().equals(TaskType.STORE)) {
//
//						/**
//						 * The owner Id
//						 */
//						String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(task.getUgsId());
//						
//						if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)
//								&& ownerMinPSIdMap.get(pSId).get(ownerId) != null) {
//							
//							// Updated on 21/04/2022 for owner packet Store Id range
//							if (((Store) task).getPacketStoreIdH() != null
//								&& checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdH())
//								&&((Store) task).getPacketStoreIdH() > ownerMinPSIdMap.get(pSId)
//										.get(ownerId)) {
//									
//								// Update min PS Id
//								ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdH());
//			
//							} else if (((Store) task).getPacketStoreIdV() != null
//									&& checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdV())
//								    && ((Store) task).getPacketStoreIdV() > ownerMinPSIdMap.get(pSId)
//										.get(ownerId)) {
//									
//								// Update min PS Id
//								ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdV());								
//
//							// Updated on 21/04/2022 to handle International packet Store Id range
//							} else if (((Store) task).getPacketStoreIdH() != null){
//								
//								logger.debug ("Update International Packet Store Id range.");
//								intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdH());
//								
//							} else if (((Store) task).getPacketStoreIdV() != null){
//								
//								logger.debug ("Update International Packet Store Id range.");
//								intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdV());							
//							}
//							
//						} else {
//							
//							logger.warn("No configured packet store Id  found for owner of ugs: " 
//									+ task.getUgsId());
//						}
//					}
//				}
//			}
//		}
		
		if (PersistPerformer.workTaskListMap.containsKey(pSId)) {
		
			for (Task task : PersistPerformer.workTaskListMap.get(pSId)) {
				
				// Check STORE Task
				if (task.getTaskType().equals(TaskType.STORE)) {
					
					logger.debug("Check PS of PR: " 
							+ ObjectMapper.parseDMToSchedPRId(task.getUgsId(), task.getProgrammingRequestId()));
					
					/**
					 * The owner Id
					 */
					String ownerId = SessionActivator.ugsOwnerIdMap.get(pSId).get(task.getUgsId());
					
					if (ownerMinPSIdMap.get(pSId).containsKey(ownerId)
							&& ownerMinPSIdMap.get(pSId).get(ownerId) != null) {
						
						// Updated on 21/04/2022 and 24/05/2022 to handle owner packet Store Id range
						if (((Store) task).getPacketStoreIdH() != null
								&& ((Store) task).getPacketStoreIdH() > ownerMinPSIdMap.get(pSId).get(ownerId)
								&& checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdH())) {
								
							// Update PS Id
							ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdH());

						} else if (((Store) task).getPacketStoreIdV() != null
																			   
								&&((Store) task).getPacketStoreIdV() > ownerMinPSIdMap.get(pSId).get(ownerId)
							    && checkPacketStoreIdRange(ownerId, ((Store) task).getPacketStoreIdV())) {
							    
							// Update PS Id
							ownerMinPSIdMap.get(pSId).put(ownerId, ((Store) task).getPacketStoreIdV());				
						}
						
						// Updated on 21/04/2022 and 24/05/2022 to handle International packet Store Id range
						if (((Store) task).getPacketStoreIdH() != null
																					   
								&&((Store) task).getPacketStoreIdH() > intMinPSIdMap.get(pSId)
								&& checkPacketStoreIdRange("SUBSCRIPTIONS", ((Store) task).getPacketStoreIdH())) {
							
							logger.debug ("Update International Packet Store Id range." );
							intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdH());
							
						} else if (((Store) task).getPacketStoreIdV() != null
								&& ((Store) task).getPacketStoreIdV() > intMinPSIdMap.get(pSId)
								&& checkPacketStoreIdRange("SUBSCRIPTIONS", ((Store) task).getPacketStoreIdV())) {
							
							logger.debug ("Update International Packet Store Id range.");
							intMinPSIdMap.put(pSId, ((Store) task).getPacketStoreIdV());						
						}
 
					} else {
						
						logger.warn("No configured minimum PS Id found for owner: " + ownerId);
						   

					}
				}
			}
		}
		
		// Initialize output map
		for (String ownerId : ownerMinPSIdMap.get(pSId).keySet()) {
			
			// Update Owner PS Ids according to MH change
			ownerMinPSIdMap.get(pSId).put(ownerId, ownerMinPSIdMap.get(pSId).get(ownerId)
				+ Configuration.deltaPacketNumber);
	
			if (catOwnerMaxPSIdMap.containsKey(ownerId)) {
			
				if (ownerMinPSIdMap.get(pSId).get(ownerId) > catOwnerMaxPSIdMap.get(ownerId) - 100) {
					
					// Reset PS Ids
					logger.debug("Reset the minimum PS Id for the Owner " + ownerId 
							+ " at: " + ownerMinPSIdMap.get(pSId).get(ownerId));
					
					ownerMinPSIdMap.get(pSId).put(ownerId, catOwnerMinPSIdMap.get(ownerId));
				}
			
			} else {
				
				logger.warn("No configured maximum PS Id found for owner of ugs: " 
						+ ownerId );
			}
			
			logger.debug("PS Id range for Delta Plan of owner: " + ownerId
					+ " from/to [" + ownerMinPSIdMap.get(pSId).get(ownerId) + "; "
					+ catOwnerMaxPSIdMap.get(ownerId) + "] for Planning Session: " + pSId);
		}
		
		// Update International PS Ids according to MH change
		intMinPSIdMap.put(pSId, (long) (intMinPSIdMap.get(pSId)
			+ ((long)Configuration.deltaPacketNumber / 10.0)));
		
		if (intMinPSIdMap.get(pSId) > catIntMaxPSId - 100) {
			
			// Reset International PS Ids
			logger.debug("Reset the minimum international PS Id "
					+ "at: " + intMinPSIdMap.get(pSId));

			intMinPSIdMap.put(pSId, catIntMinPSId);
		}
		
		logger.debug("PS Id range for Delta Plan of international services "
				+ " from/to [" + intMinPSIdMap.get(pSId) + "; "
				+ catIntMaxPSId + "] for Planning Session: " + pSId);
	}

	/**
	 * Get the reprocessed Tasks 
	 * 
	 * @param pSId
	 * @param isDeltaPlan
	 * @return
	 */
	private ArrayList<Task> getReprocessedTasks(long pSId, boolean isDeltaPlan) {
		
		logger.info("Search for REPROCESSED tasks of Planning Session: " + pSId);
		
		/**
		 * The reprocessed tasks list
		 */
		ArrayList<Task> reprTaskList = new ArrayList<Task>();
		
		/**
		 * The reprocessed task start time list
		 */
		ArrayList<Long> reprTaskTimeList = new ArrayList<Long>();
		
		for (MacroDLO macroDLO : macroDLOListMap.get(pSId)) {
						
			/**
			 * The applicable list of visibilities
			 */
			ArrayList<Visibility> visList = getDLOMHOverlapVisList(pSId, macroDLO, isDeltaPlan);
			
			for (Visibility vis: visList) {

				logger.debug("Visibility between: "
				+ vis.getVisibilityStartTime() + " and " + vis.getVisibilityStopTime() 
				+ ", for the X-Band status " + vis.isXbandFlag() + ", for the station " 
				+ vis.getAcquisitionStationId() + " for contact counter " + vis.getContactCounter() 
				+ " and look side " + vis.getLookSide() + " is overlapping the start of the Mission Horizon.");

				for (Task task : PersistPerformer.refTaskListMap.get(pSId)) {
					
					if (! reprTaskTimeList.contains(task.getStartTime().getTime())) {
					
						if (task.getTaskType().equals(TaskType.DWL)
							&& !task.getTaskMark().equals(TaskMarkType.DELETED)) {
							
							if (((Download) task).getAcquisitionStationId().equals(
									vis.getAcquisitionStationId())
									&& ((Download) task).getContactCounter().equals(
											vis.getContactCounter())) {
								
								task.setTaskMark(TaskMarkType.REPROCESSED);
								
								logger.debug("Task " + task.getTaskId() + " of type " + task.getTaskType()
								+ " is marked as " + task.getTaskMark());
								
								reprTaskList.add(task);	
								
								reprTaskTimeList.add(task.getStartTime().getTime());
							}
							
						} else if (task.getTaskType().equals(TaskType.PASSTHROUGH)
								&& !task.getTaskMark().equals(TaskMarkType.DELETED)) {
						
							if (((PassThrough) task).getAcquisitionStationId().equals(
									vis.getAcquisitionStationId())
									&& ((PassThrough) task).getContactCounter().equals(
											vis.getContactCounter())) {
								
								task.setTaskMark(TaskMarkType.REPROCESSED);
								
								logger.debug("Task: " + task.getTaskId() + " of type " + task.getTaskType()
								+ " is marked as " + task.getTaskMark());
								
								reprTaskList.add(task);
								
								reprTaskTimeList.add(task.getStartTime().getTime());
							}
						}
					}
				}				
			}
		}
		
		return reprTaskList;
	}	
	
	/**
	 * Get the visibilities in overlap with the Macro DLO and with the plan start time, if existing.
	 * @param pSId
	 * @param macroDLO
	 * @param isDeltaPlan
	 * @return
	 */
	private ArrayList<Visibility> getDLOMHOverlapVisList(long pSId, MacroDLO macroDLO, 
			boolean isDeltaPlan) {
		
		/**
		 * The reprocessed list of visibilities
		 */
		ArrayList<Visibility> repVisList = new ArrayList<Visibility>();
		
		/**
		 * The Plan Start Time
		 */
		long planStartTime = SessionActivator.planSessionMap.get(pSId)
				.getMissionHorizonStartTime().getTime();
		
		if (isDeltaPlan)  {

			if (! DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId).isEmpty()) {
				
				// Get Delta-Plan start time
				planStartTime = Collections.min(DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId));
			}
		}
		
		ArrayList<String> visIdList = new ArrayList<String>();
		
		if (macroDLO.getStartTime().getTime() > (planStartTime - Configuration.deltaTime)
				&& macroDLO.getStopTime().getTime() < (planStartTime + Configuration.deltaTime)) {
			
			for (Visibility vis : macroDLO.getVisList()) {
			
				String visId = vis.getContactCounter() + "_" + vis.getAcquisitionStationId() 
					+ "_" + vis.getVisibilityStartTime().getTime();
				
				visIdList.add(visId);
				
				if (vis.getVisibilityStartTime().getTime() < planStartTime
						&& vis.getVisibilityStopTime().getTime() > planStartTime) {
					
					// Only a visibility for same pass is added 
					if (! visIdList.contains(visId)) {
											
						repVisList.add(vis);
					}
				}
			}
		}

		return repVisList;		
	}
	
	/**
	 * Get the visibilities in overlap with the MH start time
	 * 
	 * @param pSId
	 * @return
	 */
	public ArrayList<Visibility> getMHOverlapVisList(Long pSId) {
	
		/**
		 * The overlap visibilities
		 */
		ArrayList<Visibility> overVisList = new ArrayList<Visibility>();
				
		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {
				
			for (Visibility vis : sat.getVisibilityList()) {
				
				if (vis.getVisibilityStartTime()
						.before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& vis.getVisibilityStopTime()
						.after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())) {
					
					overVisList.add(vis);
				}
			}
		}
		
		return overVisList;
	}

	/**
	 * Check and update time in case of overlap with a MacroDLO
	 * @param pSId
	 * @param time
	 * @return
	 */
	private long checkDLOOverlap(Long pSId, long time) {
		
		long marginTime = 10000;
		
		for (MacroDLO macroDLO : macroDLOListMap.get(pSId)) {
			
			if (macroDLO.getStartTime().getTime() - marginTime <= time 
					&& macroDLO.getStopTime().getTime() + marginTime >= time) {
				
				time = macroDLO.getStartTime().getTime() - marginTime; // TODO: TBD
			}
		}
		
		return time;
	}

	/**
	 * set Owner Packet Store Ids from EKMLib
	 *  
	 * @param pSId
	 * @throws Exception 
	 * @throws NumberFormatException 
	 */
	private void setOwnerPacketStoreIds(Long pSId) {

		logger.info("Set Owner Packet Store Id ranges.");

		try {
											   
			/**
			 * Instance maps
			 */
			catOwnerMinPSIdMap = new HashMap<String, Long>();
			
			catOwnerMaxPSIdMap = new HashMap<String, Long>();
			
			catIntMinPSId = 0L;
			
			catIntMaxPSId = 0L;
			
			/**
			 * Instance EKMLib
			 */
			EKMLIB ekmLib = new EKMLIB();
			
			for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {
 				
				/**
				 * The owner Id
				 */
				String ownerId = owner.getCatalogOwner().getOwnerId();
				
				// Instance ranges 
				ownerMinPSIdMap.get(pSId).put(ownerId, ekmLib.getPacketStoreIdRange(
						Long.parseLong(ownerId)).get(0));
			
				catOwnerMinPSIdMap.put(ownerId, ekmLib.getPacketStoreIdRange(
						Long.parseLong(ownerId)).get(0));
				
				catOwnerMaxPSIdMap.put(ownerId, ekmLib.getPacketStoreIdRange(
						Long.parseLong(ownerId)).get(1));		
				
				intMinPSIdMap.put(pSId, (long) ekmLib.getPacketStoreIdRange(
						null).get(0));
				
				catIntMinPSId = (long) ekmLib.getPacketStoreIdRange(
						null).get(0);
				
				catIntMaxPSId = (long) ekmLib.getPacketStoreIdRange(
						null).get(1);
			}
		
		} catch (Exception e) {
			
			logger.error("Error in EkmLib Owner Packet Store retrieve {} ",  e.getMessage(), e);
		}
	}
	
//	/**
//	 * Get GPS Packet Store Id from EKMLib
//	 * // TODO: Hp: first value returned from EKMLib is taken
//	 * @param pSId
//	 * @throws Exception 
//	 * @throws NumberFormatException 
//	 */
//	private static Long getGPSPacketStoreId() {
//		
//		try {
//		
//			logger.info("Get GPS Packet Store Id.");
//			
//			/**
//			 * Instance EKMLib
//			 */
//			EKMLIB ekmLib = new EKMLIB();
//				
//			return ekmLib.getGPSPacketStoreIdList().get(0);
//		
//		} catch (Exception e) {
//
//			logger.error("Error in EkmLib GPS Packet Store retrieve {} ",  e.getMessage(), e);
//		}
//		
//		return 1L;
//	}
	
	/**
	 * Get the list of the Packet Store Ids out of the range of the EKMLib
	 * @param taskList
	 * @return
	 * @throws NumberFormatException
	 * @throws Exception
	 */
	private static List<BigDecimal> getPacketStoreIdOutRange(ArrayList<Task> taskList) {

		logger.info("Check the Packet Store Id ranges for the Store tasks.");
		
		/**
		 * The list of task Ids
		 */
		List<BigDecimal> taskIdList = new ArrayList<BigDecimal>();
		
		try {
		
			/**
			 * Instance EKMLib
			 */
			EKMLIB ekmLib = new EKMLIB();
			
			/**
			 * The list of product tasks
			 */
			ArrayList<Task> prodTaskList = new ArrayList<Task>();
			
			for (Task task : taskList) {
				
				if (task.getUgsId() != null && (task.getTaskType().equals(TaskType.DWL))) {

						//// Commented on 01/02/2022
//						|| task.getTaskType().equals(TaskType.PASSTHROUGH))) {
					
					prodTaskList.add(task);
				}
			}
			
			// Check the packet store Id range
			taskIdList = ekmLib.checkListPacketStoreId(prodTaskList);
			
			if (taskIdList.size() > 0)  {
				
				logger.info("Some packet store Ids are not in the expected range for the relevant owner.");
			
				for (BigDecimal taskId : taskIdList) {
					
					logger.info("Packet Store of task Id: " + taskId + " is out of the expected range.");
				}
			}
		
		} catch (Exception e) {

			logger.error("Error in EkmLib Packet Store check {} ",  e.getMessage(), e);
		}
		
		return taskIdList;
	}
	
	/**
	 * Check the Packet Store Id in the range of the EKMLib
	 * 
	 * @param ownerId
	 * @param pSId
	 * @return
	 */
	private static boolean checkPacketStoreIdRange(String ownerId, Long packetStoreId) {
		
		/**
		 * The list of task Ids
		 */
		boolean isValid = false;
		
		try {
		
			/**
			 * Instance EKMLib
			 */
			EKMLIB ekmLib = new EKMLIB();
			
			// Check the packet store Id range
			if (ekmLib.checkPacketStoreIdValues(ownerId, packetStoreId)) {
				
				isValid =  true;
				
				logger.debug("Update minimum Packet Store Id: " + packetStoreId 
						+ " for owner " + ownerId);			
			}

		
		} catch (Exception e) {

			logger.error("Error in EkmLib Packet Store check {} ",  e.getMessage(), e);
		}
		
		return isValid;
	}
	
	/**
	 * Get products in overlap with visibility in the MH
	 * @param pSId
	 * @return
	 * @throws SPLAException 
	 */
	private ArrayList<Task> getOverlapVisProds(Long pSId) throws SPLAException {
		
		// Compute the visibilities in overlap with the MH
		computeMHOverVisList(pSId);
		
		/**
		 * Previous product list
		 */
		ArrayList<Task> prevProdList = new ArrayList<Task>();
		
		@SuppressWarnings("unchecked")
		ArrayList<Task> refTaskList = (ArrayList<Task>) PersistPerformer.refTaskListMap.get(pSId).clone();
		
		for (Task task : refTaskList) {
			
			if (task.getTaskType().equals(TaskType.DWL)) {
				
				if (isMHOverlapVisTask(pSId, ((Download) task).getStartTime(),
					((Download) task).getAcquisitionStationId(), ((Download) task).getSatelliteId())) {
					
					logger.debug("Added previous task " + task.getTaskId() + " for Planning Session " + pSId);

					if (SessionChecker.isDelta(pSId)) {
						
						task.setTaskMark(TaskMarkType.UNCHANGED);
					}
					
					prevProdList.add((Task) task.cloneModel());
				}
				
			} else if (task.getTaskType().equals(TaskType.PASSTHROUGH)) {
					
				if (isMHOverlapVisTask(pSId, ((PassThrough) task).getStartTime(),
					((PassThrough) task).getAcquisitionStationId(), ((PassThrough) task).getSatelliteId())) {
					
					logger.debug("Added previous task " + task.getTaskId() + " for Planning Session " + pSId);
					
					if (SessionChecker.isDelta(pSId)) {
						
						task.setTaskMark(TaskMarkType.UNCHANGED);
					}
					
					prevProdList.add((Task) task.cloneModel());
				}
			}
		}
		
		return prevProdList;	
	}
	
	/**
	 * Check if the task is within a visibility in overlap with the previous/next MH 
	 * @param pSId
	 * @param taskStartTime
	 * @param acqStationId
	 * @param overVisList
	 * @return
	 */
	private static boolean isMHOverlapVisTask(Long pSId, Date taskStartTime, String acqStationId,
			String satId) {
		
		// The overlap boolean
		boolean isOverlapVisTask = false;
		
		if (SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime()
				- taskStartTime.getTime() < 1800000
				&& SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime()
				- taskStartTime.getTime() > 0) {
		
			for (Visibility vis : overMHVisListMap.get(satId)) {
				
				if (vis.getAcquisitionStationId().equals(acqStationId)) {
					
					isOverlapVisTask = true;
					
					break;
				}
			}
		}
		
		return isOverlapVisTask;
	}
	
	/**
	 * Compute list of visibilities in overlap with the previous/next MH
	 * @param pSId
	 */
	private static void computeMHOverVisList(Long pSId) {
	
		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {
		
			overMHVisListMap.put(sat.getCatalogSatellite().getSatelliteId(), new ArrayList<>());
			
			for (Visibility vis : sat.getVisibilityList()) {
				
				if (vis.getVisibilityStartTime().before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& vis.getVisibilityStopTime().after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())) {
					
					overMHVisListMap.get(sat.getCatalogSatellite().getSatelliteId()).add(vis);
				}
			}
		}
	}
	
	/**
	 * Added from Refine DLOs in overlap with visibility with the previous/next MH
	 * // Updated on 13/03/2022 for across-plan activities management
	 * @param pSId
	 * @param dloTaskList
	 * @param prevTaskList
	 */
	private void refineOverlapVisDLOs(Long pSId, ArrayList<DLO> dloTaskList, 
			ArrayList<Task> prevTaskList) {
		
		/**
		 * Instance handler
		 */
		PersistPerformer persistPerformer = new PersistPerformer();
		
		// The across MH task list
		ArrayList<Task> acrossMHTaskList = new ArrayList<Task>();
				
		for (Task dloTask : dloTaskList) {
								
			Date startTime = dloTask.getStopTime();
			
			for (Task prevTask :  prevTaskList) {
				
				if (prevTask.getTaskType().equals(TaskType.DWL)) {
					
					if (((Download) prevTask).getAcquisitionStationId()
							.equals(((DLO) dloTask).getAcquisitionStationId())
							&& ((Download) prevTask).getContactCounter()
							.equals(((DLO) dloTask).getContactCounter())
							&& ((Download) prevTask).getSatelliteId()
							.equals(((DLO) dloTask).getSatelliteId())) {
						
						logger.info("Add DWL task across MH: " + prevTask.getTaskId());
						
						acrossMHTaskList.add(prevTask);
						
						if (prevTask.getStartTime().compareTo(startTime) < 0) {
							
							startTime = prevTask.getStartTime();
						}
					}
				} else if (prevTask.getTaskType().equals(TaskType.PASSTHROUGH)) {
					
					if (((PassThrough) prevTask).getAcquisitionStationId()
							.equals(((DLO) dloTask).getAcquisitionStationId())
							&& ((PassThrough) prevTask).getContactCounter()
							.equals(((DLO) dloTask).getContactCounter())
							&& ((PassThrough) prevTask).getSatelliteId()
							.equals(((DLO) dloTask).getSatelliteId())) {
						
						logger.info("Add PT task across MH: " + prevTask.getTaskId());
						
						acrossMHTaskList.add(prevTask);
						
						if (prevTask.getStartTime().compareTo(startTime) < 0) {
													
							startTime = prevTask.getStartTime();
							
						}
					}
				}
			}
			
			if (startTime.compareTo(((DLO)dloTask).getStartTime()) < 0) {
				
				((DLO)dloTask).setStartTime(startTime);
				
				logger.debug("Found DLO with previous start time " + startTime.toString()				 
				+ " in contact counter " + ((DLO) dloTask).getContactCounter()
				+ " for acquisition station " + ((DLO) dloTask).getAcquisitionStationId()
				+ " for satellite " + ((DLO) dloTask).getSatelliteId());
				
				logger.info("Add DLO task across MH: " + dloTask.getTaskId());

			}
		}
						
		// Added on 13/03/2022
		persistPerformer.addAcrossPlanPRStatuses(pSId, acrossMHTaskList);

	}
	
	/**
	 * Check if the download is inside a scheduled DLO
	 * 
	 * @param prodTask
	 * @param dloList
	 * @return
	 */
	boolean isInsideDLO(Task prodTask, ArrayList<DLO> dloList) {
		
		for  (DLO dlo : dloList) {
			
			// Check Download task
			if (prodTask.getTaskType().equals(TaskType.DWL)) {
				
				if (((Download) prodTask).getSatelliteId().equals(dlo.getSatelliteId())
						&& ((Download) prodTask).getAcquisitionStationId().equals(dlo.getAcquisitionStationId())
						&& ((Download) prodTask).getContactCounter().equals(dlo.getContactCounter())) {
					
					return true;
				}
			}
			
			// Check BITE task
			if (prodTask.getTaskType().equals(TaskType.BITE)) {
				
				if (((BITE) prodTask).getSatelliteId().equals(dlo.getSatelliteId())
						&& ((BITE) prodTask).getAcquisitionStationId().equals(dlo.getAcquisitionStationId())
						&& ((BITE) prodTask).getContactCounter().equals(dlo.getContactCounter())) {
					
					return true;
				}
			}	
			
			// Check PT Task
			if (prodTask.getTaskType().equals(TaskType.PASSTHROUGH)) {
				
				if (((PassThrough) prodTask).getSatelliteId().equals(dlo.getSatelliteId())
						&& ((PassThrough) prodTask).getAcquisitionStationId().equals(dlo.getAcquisitionStationId())
						&& ((PassThrough) prodTask).getContactCounter().equals(dlo.getContactCounter())) {
					
					return true;
				}
			}	
		}	
		
		return false;
		
	}
	
	/**
	 * Close the session thread
	 *
	 * @param pSId
	 * @return
	 */
	public boolean closeSession(Long pSId) throws Exception {

		/**
		 * The output boolean
		 */
		boolean isClosed = true;
		
		try {
			
			// Count the session statuses
			countSessionStatuses(pSId);

			/**
			 * Check Session
			 */
			if (Configuration.debugDPLFlag || SessionChecker.isFinal(pSId)) {
			
				logger.debug("Clear all BRM variables for the Planning Session: " + pSId);
			
				// Clear BRM session
				RulesPerformer.clearBRMSession(pSId);
				
				// Remove BRM operations
				if (RulesPerformer.brmOperMap.get(pSId) != null) {
					RulesPerformer.brmOperMap.remove(pSId);
				}
	
//				// Remove BRM parameters
//				if (RulesPerformer.brmParamsMap.get(pSId) != null) {
//					RulesPerformer.brmParamsMap.remove(pSId);
//				}
				
				if (RulesPerformer.brmInstanceMap.get(pSId) != null) {
					RulesPerformer.brmInstanceListMap.remove(pSId);
					RulesPerformer.brmInstanceMap.remove(pSId);
				}
	
				// Remove BRM working tasks
				if (RulesPerformer.brmWorkTaskListMap.get(pSId) != null) {
					RulesPerformer.brmWorkTaskListMap.remove(pSId);
				}
				
				// Remove BRM reference acquisitions
				if (RulesPerformer.brmRefAcqListMap.get(pSId) != null) {
					RulesPerformer.brmRefAcqListMap.remove(pSId);
				}
	
				// Remove rejected DTO rule list
				if (RulesPerformer.rejDTORuleListMap.get(pSId) != null) {
					RulesPerformer.rejDTORuleListMap.remove(pSId);
				}
	
				// Remove initial DTO list
				if (DeltaPlanProcessor.initDTOListMap.get(pSId) != null) {
					DeltaPlanProcessor.initDTOListMap.remove(pSId);
				}
				
				// Remove initial DTO Id list
				if (DeltaPlanProcessor.initDTOIdListMap.get(pSId) != null) {
					DeltaPlanProcessor.initDTOIdListMap.remove(pSId);
				}
			
				// Remove current DTO list
				if (DeltaPlanProcessor.currDTOListMap.get(pSId) != null) {
					DeltaPlanProcessor.currDTOListMap.remove(pSId);
				}
				
				// Remove Delta-Plan DTO list
				if (DeltaPlanProcessor.deltaSchedDTOListMap.get(pSId) != null) {
					DeltaPlanProcessor.deltaSchedDTOListMap.remove(pSId);
				}
				
//				// Remove unavailable DTO list
//				if (DeltaPlanProcessor.unavDTOIdListMap.get(pSId) != null) {
//					DeltaPlanProcessor.unavDTOIdListMap.remove(pSId);
//				}
//
//				// Remove delta-plan satellite visibility time
//				if (DeltaPlanProcessor.deltaSatVisTimeMap.get(pSId) != null) {
//					DeltaPlanProcessor.deltaSatVisTimeMap.remove(pSId);
//				}

				// Remove current plan offset time
				if (DeltaPlanProcessor.currPlanOffsetTimeMap.get(pSId) != null) {
					DeltaPlanProcessor.currPlanOffsetTimeMap.remove(pSId);
				}
	
				// Remove replaced DTO Id list
				if (DeltaPlanProcessor.cancDTOIdListMap.get(pSId) != null) {
					DeltaPlanProcessor.cancDTOIdListMap.remove(pSId);
				}
				
				// Remove total replaced DTO Id list
				if (DeltaPlanProcessor.cancTotDTOIdListMap.get(pSId) != null) {
					DeltaPlanProcessor.cancTotDTOIdListMap.remove(pSId);
				}
							
				// Remove Owner BICs
				if (ownerBICMap != null) {
					ownerBICMap.clear();
				}
				
				// Remove DTO Image Id
				if (dtoImageIdMap != null) {
					dtoImageIdMap.clear();
				}

			}
			
			// Added
			TaskPlanner.schedDTODwlPSIdMap.clear();
			
			// Remove final maps
			if (SessionChecker.isFinal(pSId)) {

				logger.info("Clear all CSPS variables for the Planning Sessions: " + pSId);

				if (NextARProcessor.nextARSubDateMap != null) {

					NextARProcessor.nextARSubDateMap.clear();
				}

				for (int i = 0; i < SessionActivator.mhPSIdListMap.get(pSId).size(); i++) {
					
					/**
					 * The Mission Horizon Planning Session Id list
					 */
					Long mhPSId = SessionActivator.mhPSIdListMap.get(pSId).get(i);
					
					// Remove previous sessions maps
					logger.info("Clear variables for the Planning Session: " + mhPSId); 
					
					// Remove plan policy
					if (SessionActivator.planPolicyMap.get(mhPSId) != null) {
						SessionActivator.planPolicyMap.remove(mhPSId);
					}

					// Remove plan date
					if (SessionActivator.planDateMap.get(mhPSId) != null) {
						SessionActivator.planDateMap.remove(mhPSId);
					}

					// Remove owner list
					if (SessionActivator.ownerListMap.get(mhPSId) != null) {
						SessionActivator.ownerListMap.remove(mhPSId);
					}

					// Remove owner AR Ids
					if (SessionActivator.ownerARIdMap != null) {
						SessionActivator.ownerARIdMap.remove(mhPSId);
					}

					// Remove partner list
					if (SessionActivator.partnerListMap.get(mhPSId) != null) {
						SessionActivator.partnerListMap.remove(mhPSId);
					}
					
					// Remove UGS partner Ids
					if (SessionActivator.ugsOwnerIdMap.get(mhPSId) != null) {
						SessionActivator.ugsOwnerIdMap.remove(mhPSId);
					}
					
					// Remove UGS isTUP flag
					if (SessionActivator.ugsIsTUPMap.get(mhPSId) != null) {
						SessionActivator.ugsIsTUPMap.remove(mhPSId);
					}
					
					// Remove ugs Id subscription compatibility
					if (SessionActivator.ugsIdSubCompatibilityMap.get(mhPSId) != null) {
						SessionActivator.ugsIdSubCompatibilityMap.remove(mhPSId);
					}

					// Remove scm availability
					if (SessionActivator.scmAvailMap.get(mhPSId) != null) {
						SessionActivator.scmAvailMap.remove(mhPSId);
					}
					
					// Remove scm response wait
					if (SessionActivator.scmResWaitMap.get(mhPSId) != null) {
						SessionActivator.scmResWaitMap.remove(mhPSId);
					}

					// Remove owner acquisition station list
					if (SessionActivator.ownerAcqStationListMap.get(mhPSId) != null) {
						SessionActivator.ownerAcqStationListMap.remove(mhPSId);
					}
					
					// Remove UGS backup station list
					if (SessionActivator.ugsBackStationIdListMap.get(mhPSId) != null) {
						SessionActivator.ugsBackStationIdListMap.remove(mhPSId);
					}

					// Remove working planning session Ids
					if (SessionActivator.workPSIdMap.get(mhPSId) != null) {
						SessionActivator.workPSIdMap.remove(mhPSId);
					}

					// Remove reference planning session Ids
					if (SessionActivator.refPSIdMap.get(mhPSId) != null) {
						SessionActivator.refPSIdMap.remove(mhPSId);
					}

					// Remove first session
					if (SessionActivator.firstSessionMap.get(mhPSId) != null) {
						SessionActivator.firstSessionMap.remove(mhPSId);
					}

					// Remove initial scheduled DTO list
					if (SessionActivator.initSchedDTOListMap.get(mhPSId) != null) {
						SessionActivator.initSchedDTOListMap.remove(mhPSId);
					}
					
					// Remove initial AR Id scheduled DTO
					if (SessionActivator.initSchedARIdDTOMap.get(mhPSId) != null) {
						SessionActivator.initSchedARIdDTOMap.remove(mhPSId);
					}
					
					// Remove initial equivalent DTO
					if (SessionActivator.initEquivDTOMap.get(mhPSId) != null) {
						SessionActivator.initEquivDTOMap.remove(mhPSId);
					}
					
					// Remove initial AR Id equivalent DTO
					if (SessionActivator.initARIdEquivDTOMap.get(mhPSId) != null) {
						SessionActivator.initARIdEquivDTOMap.remove(mhPSId);
					}
					
					// Remove PRList
					if (PRListProcessor.pRListMap.get(mhPSId) != null) {
						PRListProcessor.pRListMap.remove(mhPSId);
					}
					
					// Remove reference PRList
					if (PRListProcessor.refPRListMap.get(mhPSId) != null) {
						PRListProcessor.refPRListMap.remove(mhPSId);
					}

					// Remove PR scheduling Ids
					if (PRListProcessor.pRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.pRSchedIdMap.remove(mhPSId);
					}
					
					// Remove session PR scheduling Ids
					if (PRListProcessor.pSPRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.pSPRSchedIdMap.remove(mhPSId);
					}
					
					// Remove reference PR scheduling Ids
					if (PRListProcessor.workPRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.workPRSchedIdMap.remove(mhPSId);
					}
					
					// Remove PR international boolean
					if (PRListProcessor.pRIntBoolMap.get(mhPSId) != null) {
						PRListProcessor.pRIntBoolMap.remove(mhPSId);
					}

					// Remove AR Ids
					if (PRListProcessor.aRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.aRSchedIdMap.remove(mhPSId);
					}

					// Remove DTO Ids
					if (PRListProcessor.dtoSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.dtoSchedIdMap.remove(mhPSId);
					}

					// Remove scheduled DTOs
					if (PRListProcessor.schedDTOMap.get(mhPSId) != null) {
						PRListProcessor.schedDTOMap.remove(mhPSId);
					}

					// Remove equivalent theatres
					if (PRListProcessor.equivTheatreMap.get(mhPSId) != null) {
						PRListProcessor.equivTheatreMap.remove(mhPSId);
					}
					
					// Remove equivalent DTO Ids
					if (PRListProcessor.equivStartTimeIdMap.get(mhPSId) != null) {
						PRListProcessor.equivStartTimeIdMap.remove(mhPSId);
					}
					
					// Remove equivalent scheduling AR Ids
					if (PRListProcessor.equivIdSchedARIdMap.get(mhPSId) != null) {
						PRListProcessor.equivIdSchedARIdMap.remove(mhPSId);
					}
					
					// Remove equivalent maneuvers
					if (PRListProcessor.equivStartTimeManMap.get(mhPSId) != null) {
						PRListProcessor.equivStartTimeManMap.remove(mhPSId);
					}

					// Remove replacing PRList
					if (PRListProcessor.replPRListMap.get(mhPSId) != null) {
						PRListProcessor.replPRListMap.remove(mhPSId);
					}
					
					// Remove crisis PRList
					if (PRListProcessor.crisisPRListMap.get(mhPSId) != null) {
						PRListProcessor.crisisPRListMap.remove(mhPSId);
					}

					// Remove new AR sizes
					if (PRListProcessor.newARSizeMap != null) {
						PRListProcessor.newARSizeMap.remove(mhPSId);
					}

					// Remove PRList Ids
					if (PRListProcessor.pRToPRListIdMap != null) {
						PRListProcessor.pRToPRListIdMap.remove(mhPSId);
					}

					// Remove discarded PR Ids
					if (PRListProcessor.discardPRIdListMap != null) {
						PRListProcessor.discardPRIdListMap.remove(mhPSId);
					}
					
					// Remove HP-Civilian DTO Ids
					if (HPCivilianRequestHandler.hpCivilDTOIdListMap.get(mhPSId) != null) {
						HPCivilianRequestHandler.hpCivilDTOIdListMap.remove(mhPSId);
					}
					
					// Remove HP-Civilian Unique Ids
					if (HPCivilianRequestHandler.hpCivilUniqueIdListMap.get(mhPSId) != null) {
						HPCivilianRequestHandler.hpCivilUniqueIdListMap.remove(mhPSId);
					}

					// Remove DTO Intersection Matrix
					if (IntMatrixCalculator.intDTOMatrixMap != null) {
						IntMatrixCalculator.intDTOMatrixMap.remove(mhPSId);
					}

					// Remove Task Intersection Matrix
					if (IntMatrixCalculator.intTaskMatrixMap != null) {
						IntMatrixCalculator.intTaskMatrixMap.remove(mhPSId);
					}

					// Remove all session maps
					if (SessionActivator.planSessionMap.get(mhPSId) != null) {
						SessionActivator.planSessionMap.remove(mhPSId);
					}
					
					// Remove all scheduling DTO Id status maps
					if (SessionActivator.schedDTOIdStatusMap.get(mhPSId) != null) {
						SessionActivator.schedDTOIdStatusMap.remove(mhPSId);
					}
					
					// Remove all persisting tasks
					if (PersistPerformer.workTaskListMap.get(mhPSId) != null) {
						PersistPerformer.workTaskListMap.remove(mhPSId);
					}
					
					if (PersistPerformer.refTaskListMap.get(mhPSId) != null) {
						PersistPerformer.refTaskListMap.remove(mhPSId);
					}

					if (PersistPerformer.refPSTaskListMap.get(mhPSId) != null) {
						PersistPerformer.refPSTaskListMap.remove(mhPSId);
					}
					
					if (PersistPerformer.refAcqIdMap.get(mhPSId) != null) {
						PersistPerformer.refAcqIdMap.remove(mhPSId);
					}
					
					// Remove all scheduling maps

					if (satListMap.get(mhPSId) != null) {
						satListMap.remove(mhPSId);
					}

//					if (persistenceMap.get(mhPSId) != null) {
//						persistenceMap.remove(mhPSId);
//					}
					
					if (finalMap.get(mhPSId) != null) {
						finalMap.remove(mhPSId);
					}

					if (schedARListMap.get(mhPSId) != null) {
						schedARListMap.remove(mhPSId);
					}

					if (planDTOIdListMap.get(mhPSId) != null) {
						planDTOIdListMap.remove(mhPSId);
					}

					if (rejDTOIdListMap.get(mhPSId) != null) {
						rejDTOIdListMap.remove(mhPSId);
					}

					if (rejARDTOIdSetMap.get(mhPSId) != null) {
						rejARDTOIdSetMap.remove(mhPSId);
					}

					if (dtoImageIdMap.get(mhPSId) != null) {
						dtoImageIdMap.remove(mhPSId);
					}

					if (macroDLOListMap.get(mhPSId) != null) {
						macroDLOListMap.remove(mhPSId);
					}
					
					if (planDLOListMap.get(mhPSId) != null) {
						planDLOListMap.remove(mhPSId);
					}

					// Remove nextAR maps
					
					if (NextARProcessor.nextARIterMap.get(mhPSId) != null) {
						NextARProcessor.nextARIterMap.remove(mhPSId);
					}

					if (NextARProcessor.nextSchedARMap.get(mhPSId) != null) {
						NextARProcessor.nextSchedARMap.remove(mhPSId);
					}

					if (NextARProcessor.nextSchedDTOListMap.get(mhPSId) != null) {
						NextARProcessor.nextSchedDTOListMap.remove(mhPSId);
					}

					if (NextARProcessor.bestRankSolMap.get(mhPSId) != null) {
						NextARProcessor.bestRankSolMap.remove(mhPSId);
					}

					// Remove unranked ARList
					if (UnrankARListProcessor.unrankSchedARListMap.get(mhPSId) != null) {
						UnrankARListProcessor.unrankSchedARListMap.remove(mhPSId);
					}

					// Remove manualAR maps
					if (ManualPlanProcessor.manPlanIterMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanIterMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.manPlanARMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanARMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.manPlanDTOListMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanDTOListMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.bestRankSolMap.get(mhPSId) != null) {
						ManualPlanProcessor.bestRankSolMap.remove(mhPSId);
					}
					
					// Remove processing maps
					if (PRListProcessor.schedARIdRankMap.get(mhPSId) != null) {
						PRListProcessor.schedARIdRankMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.schedDTODomainMap.get(mhPSId) != null) {
						RankPerformer.schedDTODomainMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.iterMap.get(mhPSId) != null) {
						RankPerformer.iterMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.jumpMap.get(mhPSId) != null) {
						RankPerformer.jumpMap.remove(mhPSId);
					}
					
					// Remove 
					if (EquivDTOHandler.di2sMasterSchedDTOMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sMasterSchedDTOMap.remove(mhPSId);
					}
					
					// Remove 					
					if (EquivDTOHandler.di2sSlaveSchedDTOMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sSlaveSchedDTOMap.remove(mhPSId);
					}
					
					// Remove 
					if (EquivDTOHandler.slaveDTOIdListMap.get(mhPSId) != null) {
						EquivDTOHandler.slaveDTOIdListMap.remove(mhPSId); 
					}
					
					// Remove 
					if (EquivDTOHandler.di2sLinkedIdsMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sLinkedIdsMap.remove(mhPSId); 
					}
					
					// Remove 					
					if (FilterDTOHandler.filtRejReqListMap.get(mhPSId) != null) {
						FilterDTOHandler.filtRejReqListMap.remove(mhPSId);
					}
					
					// Remove 
					if (FilterDTOHandler.filtRejDTOIdListMap.get(mhPSId) != null) {
						FilterDTOHandler.filtRejDTOIdListMap.remove(mhPSId);
					}
					
					// Remove 
					if (FilterDTOHandler.isWaitFiltResultMap.get(mhPSId) != null) {
						FilterDTOHandler.isWaitFiltResultMap.remove(mhPSId);
					}
					
					// Remove
					if (ownerMinPSIdMap.get(mhPSId) != null) {
						ownerMinPSIdMap.remove(mhPSId);
					} 
					
					// Remove
					if (intMinPSIdMap.get(mhPSId) != null) {
						intMinPSIdMap.remove(mhPSId);
					} 
					
				}
				
				// Added on 21/04/2022 for cache clearance
				clearCache(pSId);
				
				// TODO: check
				System.gc();
				
			}

		} catch (Exception e) {

			logger.error("Error saving resources {} - {}", pSId, e.getMessage(), e);

			isClosed = false;

		} finally {

			logger.info("SaveResourceValue processing ended.");
			

		}

		return isClosed;
	}
	
	/**
	 * Close the session thread
	 *
	 * @param pSId
	 * @return
	 */
	private static boolean clearCache(Long pSId) throws Exception {
		
		try {	

			for (long mhPSId : SessionActivator.mhPSIdListMap.keySet()) {
				
				if (mhPSId < pSId) {
		
					logger.debug("Clear all resources in the CSPS cache for Planning Session:"
							+ mhPSId);
						
					// Clear BRM session
					RulesPerformer.clearBRMSession(mhPSId);
					
					// Remove BRM operations
					if (RulesPerformer.brmOperMap.get(mhPSId) != null) {
						RulesPerformer.brmOperMap.remove(mhPSId);
					}
		
	//					// Remove BRM parameters
	//					if (RulesPerformer.brmParamsMap.get(pSId) != null) {
	//						RulesPerformer.brmParamsMap.remove(pSId);
	//					}
					
					if (RulesPerformer.brmInstanceMap.get(mhPSId) != null) {
						RulesPerformer.brmInstanceListMap.remove(mhPSId);
						RulesPerformer.brmInstanceMap.remove(mhPSId);
					}
		
					// Remove BRM working tasks
					if (RulesPerformer.brmWorkTaskListMap.get(mhPSId) != null) {
						RulesPerformer.brmWorkTaskListMap.remove(mhPSId);
					}
					
					// Remove BRM reference acquisitions
					if (RulesPerformer.brmRefAcqListMap.get(mhPSId) != null) {
						RulesPerformer.brmRefAcqListMap.remove(mhPSId);
					}
		
					// Remove rejected DTO rule list
					if (RulesPerformer.rejDTORuleListMap.get(mhPSId) != null) {
						RulesPerformer.rejDTORuleListMap.remove(mhPSId);
					}
		
					// Remove initial DTO list
					if (DeltaPlanProcessor.initDTOListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.initDTOListMap.remove(mhPSId);
					}
					
					// Remove initial DTO Id list
					if (DeltaPlanProcessor.initDTOIdListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.initDTOIdListMap.remove(mhPSId);
					}
				
					// Remove current DTO list
					if (DeltaPlanProcessor.currDTOListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.currDTOListMap.remove(mhPSId);
					}
					
					// Remove Delta-Plan DTO list
					if (DeltaPlanProcessor.deltaSchedDTOListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.deltaSchedDTOListMap.remove(mhPSId);
					}
					
	//					// Remove unavailable DTO list
	//					if (DeltaPlanProcessor.unavDTOIdListMap.get(mhPSId) != null) {
	//						DeltaPlanProcessor.unavDTOIdListMap.remove(mhPSId);
	//					}
	//
	//					// Remove delta-plan satellite visibility time
	//					if (DeltaPlanProcessor.deltaSatVisTimeMap.get(mhPSId) != null) {
	//						DeltaPlanProcessor.deltaSatVisTimeMap.remove(mhPSId);
	//					}
	
					// Remove current plan offset time
					if (DeltaPlanProcessor.currPlanOffsetTimeMap.get(mhPSId) != null) {
						DeltaPlanProcessor.currPlanOffsetTimeMap.remove(mhPSId);
					}
		
					// Remove replaced DTO Id list
					if (DeltaPlanProcessor.cancDTOIdListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.cancDTOIdListMap.remove(mhPSId);
					}
					
					// Remove total replaced DTO Id list
					if (DeltaPlanProcessor.cancTotDTOIdListMap.get(mhPSId) != null) {
						DeltaPlanProcessor.cancTotDTOIdListMap.remove(mhPSId);
					}
								
					// Remove Owner BICs
					if (ownerBICMap != null) {
						ownerBICMap.clear();
					}
					
					// Remove DTO Image Id
					if (dtoImageIdMap != null) {
						dtoImageIdMap.clear();
					}
				
					// Remove
					if (TaskPlanner.schedDTODwlPSIdMap != null) {
		
						TaskPlanner.schedDTODwlPSIdMap.clear();
					}
		
					// Remove
					if (NextARProcessor.nextARSubDateMap != null) {
		
						NextARProcessor.nextARSubDateMap.clear();
					}
						
					// Remove previous sessions maps
					
					// Remove plan policy
					if (SessionActivator.planPolicyMap.get(mhPSId) != null) {
						SessionActivator.planPolicyMap.remove(mhPSId);
					}
	
					// Remove plan date
					if (SessionActivator.planDateMap.get(mhPSId) != null) {
						SessionActivator.planDateMap.remove(mhPSId);
					}
	
					// Remove owner list
					if (SessionActivator.ownerListMap.get(mhPSId) != null) {
						SessionActivator.ownerListMap.remove(mhPSId);
					}
	
					// Remove owner AR Ids
					if (SessionActivator.ownerARIdMap != null) {
						SessionActivator.ownerARIdMap.remove(mhPSId);
					}
	
					// Remove partner list
					if (SessionActivator.partnerListMap.get(mhPSId) != null) {
						SessionActivator.partnerListMap.remove(mhPSId);
					}
					
					// Remove UGS partner Ids
					if (SessionActivator.ugsOwnerIdMap.get(mhPSId) != null) {
						SessionActivator.ugsOwnerIdMap.remove(mhPSId);
					}
					
					// Remove UGS isTUP flag
					if (SessionActivator.ugsIsTUPMap.get(mhPSId) != null) {
						SessionActivator.ugsIsTUPMap.remove(mhPSId);
					}
					
					// Remove ugs Id subscription compatibility
					if (SessionActivator.ugsIdSubCompatibilityMap.get(mhPSId) != null) {
						SessionActivator.ugsIdSubCompatibilityMap.remove(mhPSId);
					}
	
					// Remove scm availability
					if (SessionActivator.scmAvailMap.get(mhPSId) != null) {
						SessionActivator.scmAvailMap.remove(mhPSId);
					}
					
					// Remove scm response wait
					if (SessionActivator.scmResWaitMap.get(mhPSId) != null) {
						SessionActivator.scmResWaitMap.remove(mhPSId);
					}
	
					// Remove owner acquisition station list
					if (SessionActivator.ownerAcqStationListMap.get(mhPSId) != null) {
						SessionActivator.ownerAcqStationListMap.remove(mhPSId);
					}
					
					// Remove UGS backup station list
					if (SessionActivator.ugsBackStationIdListMap.get(mhPSId) != null) {
						SessionActivator.ugsBackStationIdListMap.remove(mhPSId);
					}
	
					// Remove working planning session Ids
					if (SessionActivator.workPSIdMap.get(mhPSId) != null) {
						SessionActivator.workPSIdMap.remove(mhPSId);
					}
	
					// Remove reference planning session Ids
					if (SessionActivator.refPSIdMap.get(mhPSId) != null) {
						SessionActivator.refPSIdMap.remove(mhPSId);
					}
	
					// Remove first session
					if (SessionActivator.firstSessionMap.get(mhPSId) != null) {
						SessionActivator.firstSessionMap.remove(mhPSId);
					}
	
					// Remove initial scheduled DTO list
					if (SessionActivator.initSchedDTOListMap.get(mhPSId) != null) {
						SessionActivator.initSchedDTOListMap.remove(mhPSId);
					}
					
					// Remove initial AR Id scheduled DTO
					if (SessionActivator.initSchedARIdDTOMap.get(mhPSId) != null) {
						SessionActivator.initSchedARIdDTOMap.remove(mhPSId);
					}
					
					// Remove initial equivalent DTO
					if (SessionActivator.initEquivDTOMap.get(mhPSId) != null) {
						SessionActivator.initEquivDTOMap.remove(mhPSId);
					}
					
					// Remove initial AR Id equivalent DTO
					if (SessionActivator.initARIdEquivDTOMap.get(mhPSId) != null) {
						SessionActivator.initARIdEquivDTOMap.remove(mhPSId);
					}
					
					// Remove PRList
					if (PRListProcessor.pRListMap.get(mhPSId) != null) {
						PRListProcessor.pRListMap.remove(mhPSId);
					}
					
					// Remove reference PRList
					if (PRListProcessor.refPRListMap.get(mhPSId) != null) {
						PRListProcessor.refPRListMap.remove(mhPSId);
					}
	
					// Remove PR scheduling Ids
					if (PRListProcessor.pRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.pRSchedIdMap.remove(mhPSId);
					}
					
					// Remove session PR scheduling Ids
					if (PRListProcessor.pSPRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.pSPRSchedIdMap.remove(mhPSId);
					}
					
					// Remove reference PR scheduling Ids
					if (PRListProcessor.workPRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.workPRSchedIdMap.remove(mhPSId);
					}
					
					// Remove PR international boolean
					if (PRListProcessor.pRIntBoolMap.get(mhPSId) != null) {
						PRListProcessor.pRIntBoolMap.remove(mhPSId);
					}
	
					// Remove AR Ids
					if (PRListProcessor.aRSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.aRSchedIdMap.remove(mhPSId);
					}
	
					// Remove DTO Ids
					if (PRListProcessor.dtoSchedIdMap.get(mhPSId) != null) {
						PRListProcessor.dtoSchedIdMap.remove(mhPSId);
					}
	
					// Remove scheduled DTOs
					if (PRListProcessor.schedDTOMap.get(mhPSId) != null) {
						PRListProcessor.schedDTOMap.remove(mhPSId);
					}
	
					// Remove equivalent theatres
					if (PRListProcessor.equivTheatreMap.get(mhPSId) != null) {
						PRListProcessor.equivTheatreMap.remove(mhPSId);
					}
					
					// Remove equivalent DTO Ids
					if (PRListProcessor.equivStartTimeIdMap.get(mhPSId) != null) {
						PRListProcessor.equivStartTimeIdMap.remove(mhPSId);
					}
					
					// Remove equivalent scheduling AR Ids
					if (PRListProcessor.equivIdSchedARIdMap.get(mhPSId) != null) {
						PRListProcessor.equivIdSchedARIdMap.remove(mhPSId);
					}
					
					// Remove equivalent maneuvers
					if (PRListProcessor.equivStartTimeManMap.get(mhPSId) != null) {
						PRListProcessor.equivStartTimeManMap.remove(mhPSId);
					}
	
					// Remove replacing PRList
					if (PRListProcessor.replPRListMap.get(mhPSId) != null) {
						PRListProcessor.replPRListMap.remove(mhPSId);
					}
					
					// Remove crisis PRList
					if (PRListProcessor.crisisPRListMap.get(mhPSId) != null) {
						PRListProcessor.crisisPRListMap.remove(mhPSId);
					}
	
					// Remove new AR sizes
					if (PRListProcessor.newARSizeMap != null) {
						PRListProcessor.newARSizeMap.remove(mhPSId);
					}
	
					// Remove PRList Ids
					if (PRListProcessor.pRToPRListIdMap != null) {
						PRListProcessor.pRToPRListIdMap.remove(mhPSId);
					}
	
					// Remove discarded PR Ids
					if (PRListProcessor.discardPRIdListMap != null) {
						PRListProcessor.discardPRIdListMap.remove(mhPSId);
					}
					
					// Remove HP-Civilian DTO Ids
					if (HPCivilianRequestHandler.hpCivilDTOIdListMap.get(mhPSId) != null) {
						HPCivilianRequestHandler.hpCivilDTOIdListMap.remove(mhPSId);
					}
					
					// Remove HP-Civilian Unique Ids
					if (HPCivilianRequestHandler.hpCivilUniqueIdListMap.get(mhPSId) != null) {
						HPCivilianRequestHandler.hpCivilUniqueIdListMap.remove(mhPSId);
					}
	
					// Remove DTO Intersection Matrix
					if (IntMatrixCalculator.intDTOMatrixMap != null) {
						IntMatrixCalculator.intDTOMatrixMap.remove(mhPSId);
					}
	
					// Remove Task Intersection Matrix
					if (IntMatrixCalculator.intTaskMatrixMap != null) {
						IntMatrixCalculator.intTaskMatrixMap.remove(mhPSId);
					}
	
					// Remove all session maps
					if (SessionActivator.planSessionMap.get(mhPSId) != null) {
						SessionActivator.planSessionMap.remove(mhPSId);
					}
					
					// Remove all scheduling DTO Id status maps
					if (SessionActivator.schedDTOIdStatusMap.get(mhPSId) != null) {
						SessionActivator.schedDTOIdStatusMap.remove(mhPSId);
					}
					
					// Remove all persisting tasks
					if (PersistPerformer.workTaskListMap.get(mhPSId) != null) {
						PersistPerformer.workTaskListMap.remove(mhPSId);
					}
					
					if (PersistPerformer.refTaskListMap.get(mhPSId) != null) {
						PersistPerformer.refTaskListMap.remove(mhPSId);
					}
	
					if (PersistPerformer.refPSTaskListMap.get(mhPSId) != null) {
						PersistPerformer.refPSTaskListMap.remove(mhPSId);
					}
					
					if (PersistPerformer.refAcqIdMap.get(mhPSId) != null) {
						PersistPerformer.refAcqIdMap.remove(mhPSId);
					}
					
					// Remove all scheduling maps
	
					if (satListMap.get(mhPSId) != null) {
						satListMap.remove(mhPSId);
					}
	
	//						if (persistenceMap.get(mhPSId) != null) {
	//							persistenceMap.remove(mhPSId);
	//						}
					
					if (finalMap.get(mhPSId) != null) {
						finalMap.remove(mhPSId);
					}
	
					if (schedARListMap.get(mhPSId) != null) {
						schedARListMap.remove(mhPSId);
					}
	
					if (planDTOIdListMap.get(mhPSId) != null) {
						planDTOIdListMap.remove(mhPSId);
					}
	
					if (rejDTOIdListMap.get(mhPSId) != null) {
						rejDTOIdListMap.remove(mhPSId);
					}
	
					if (rejARDTOIdSetMap.get(mhPSId) != null) {
						rejARDTOIdSetMap.remove(mhPSId);
					}
	
					if (dtoImageIdMap.get(mhPSId) != null) {
						dtoImageIdMap.remove(mhPSId);
					}
	
					if (macroDLOListMap.get(mhPSId) != null) {
						macroDLOListMap.remove(mhPSId);
					}
					
					if (planDLOListMap.get(mhPSId) != null) {
						planDLOListMap.remove(mhPSId);
					}
	
					// Remove nextAR maps
					
					if (NextARProcessor.nextARIterMap.get(mhPSId) != null) {
						NextARProcessor.nextARIterMap.remove(mhPSId);
					}
	
					if (NextARProcessor.nextSchedARMap.get(mhPSId) != null) {
						NextARProcessor.nextSchedARMap.remove(mhPSId);
					}
	
					if (NextARProcessor.nextSchedDTOListMap.get(mhPSId) != null) {
						NextARProcessor.nextSchedDTOListMap.remove(mhPSId);
					}
	
					if (NextARProcessor.bestRankSolMap.get(mhPSId) != null) {
						NextARProcessor.bestRankSolMap.remove(mhPSId);
					}
	
					// Remove unranked ARList
					if (UnrankARListProcessor.unrankSchedARListMap.get(mhPSId) != null) {
						UnrankARListProcessor.unrankSchedARListMap.remove(mhPSId);
					}
	
					// Remove manualAR maps
					if (ManualPlanProcessor.manPlanIterMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanIterMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.manPlanARMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanARMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.manPlanDTOListMap.get(mhPSId) != null) {
						ManualPlanProcessor.manPlanDTOListMap.remove(mhPSId);
					}
					
					// Remove 
					if (ManualPlanProcessor.bestRankSolMap.get(mhPSId) != null) {
						ManualPlanProcessor.bestRankSolMap.remove(mhPSId);
					}
					
					// Remove processing maps
					if (PRListProcessor.schedARIdRankMap.get(mhPSId) != null) {
						PRListProcessor.schedARIdRankMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.schedDTODomainMap.get(mhPSId) != null) {
						RankPerformer.schedDTODomainMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.iterMap.get(mhPSId) != null) {
						RankPerformer.iterMap.remove(mhPSId);
					}
					
					// Remove 
					if (RankPerformer.jumpMap.get(mhPSId) != null) {
						RankPerformer.jumpMap.remove(mhPSId);
					}
					
					// Remove 
					if (EquivDTOHandler.di2sMasterSchedDTOMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sMasterSchedDTOMap.remove(mhPSId);
					}
					
					// Remove 					
					if (EquivDTOHandler.di2sSlaveSchedDTOMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sSlaveSchedDTOMap.remove(mhPSId);
					}
					
					// Remove 
					if (EquivDTOHandler.slaveDTOIdListMap.get(mhPSId) != null) {
						EquivDTOHandler.slaveDTOIdListMap.remove(mhPSId); 
					}
					
					// Remove 
					if (EquivDTOHandler.di2sLinkedIdsMap.get(mhPSId) != null) {
						EquivDTOHandler.di2sLinkedIdsMap.remove(mhPSId); 
					}
					
					// Remove 					
					if (FilterDTOHandler.filtRejReqListMap.get(mhPSId) != null) {
						FilterDTOHandler.filtRejReqListMap.remove(mhPSId);
					}
					
					// Remove 
					if (FilterDTOHandler.filtRejDTOIdListMap.get(mhPSId) != null) {
						FilterDTOHandler.filtRejDTOIdListMap.remove(mhPSId);
					}
					
					// Remove 
					if (FilterDTOHandler.isWaitFiltResultMap.get(mhPSId) != null) {
						FilterDTOHandler.isWaitFiltResultMap.remove(mhPSId);
					}
					
					// Remove
					if (ownerMinPSIdMap.get(mhPSId) != null) {
						ownerMinPSIdMap.remove(mhPSId);
					} 
					
					// Remove
					if (intMinPSIdMap.get(mhPSId) != null) {
						intMinPSIdMap.remove(mhPSId);
					} 
						
					SessionActivator.mhPSIdListMap.get(mhPSId).clear();
					
				}
			}
						
			/**
			 * The map of the PRs
			 */
			Iterator<Long> it = SessionActivator.mhPSIdListMap.keySet()
					.iterator();
			
			while (it.hasNext()) {
				
				// The next  MH PS Id iterator
				Long mhPSId = it.next();

				if (mhPSId <= pSId) { // TODO: test '=' condition
					
					it.remove();
//					SessionActivator.mhPSIdListMap.remove(it.next());

				}
			}

									
		} catch (Exception e) {

			logger.error("Error clearing Cache: " + e.getMessage());


		} finally {

			logger.info("Clear Cache ended.");

		}
		return true;

	}


}
	

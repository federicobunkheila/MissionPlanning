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

package com.telespazio.csg.spla.csps.performer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.drools.DroolsEnvironment;
import com.nais.spla.brm.library.main.drools.DroolsOperations;
import com.nais.spla.brm.library.main.drools.DroolsParameters;
import com.nais.spla.brm.library.main.drools.functions.EssEnergyManagement;
import com.nais.spla.brm.library.main.drools.functions.PdhtManagement;
import com.nais.spla.brm.library.main.drools.utils.DroolsUtils;
import com.nais.spla.brm.library.main.drools.utils.TaskPlanned;
import com.nais.spla.brm.library.main.ontology.enums.PRMode;
import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.nais.spla.brm.library.main.ontology.enums.TaskType;
import com.nais.spla.brm.library.main.ontology.enums.TypeOfAcquisition;
import com.nais.spla.brm.library.main.ontology.resourceData.CreditCard;
import com.nais.spla.brm.library.main.ontology.resourceData.DTO;
import com.nais.spla.brm.library.main.ontology.resourceData.DebitCard;
import com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO;
import com.nais.spla.brm.library.main.ontology.resourceData.MissionHorizon;
import com.nais.spla.brm.library.main.ontology.resourceData.PAW;
import com.nais.spla.brm.library.main.ontology.resourceData.SatelliteState;
import com.nais.spla.brm.library.main.ontology.resourceData.Visibility;
import com.nais.spla.brm.library.main.ontology.resources.CMGA;
import com.nais.spla.brm.library.main.ontology.resources.Eclipse;
import com.nais.spla.brm.library.main.ontology.resources.PDHT;
import com.nais.spla.brm.library.main.ontology.resources.Partner;
import com.nais.spla.brm.library.main.ontology.resources.ReasonOfRejectElement;
import com.nais.spla.brm.library.main.ontology.resources.Satellite;
import com.nais.spla.brm.library.main.ontology.tasks.Acquisition;
import com.nais.spla.brm.library.main.ontology.tasks.Download;
import com.nais.spla.brm.library.main.ontology.tasks.Task;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.AcqPriorityComparator;
import com.telespazio.csg.spla.csps.utils.BICCalculator;
import com.telespazio.csg.spla.csps.utils.DefaultDebugger;
import com.telespazio.csg.spla.csps.utils.SchedDTOPriorityComparator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SchedARIdRankComparator;

import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.enums.TaskMarkType;
import it.sistematica.spla.datamodel.core.model.PlanningSession;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Transaction;

/**
 * The BRM rules performer class
 */
public class RulesPerformer {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(RulesPerformer.class);

	/**
	 * The BRM instance list map
	 */
	public static HashMap<Long, ArrayList<Integer>> brmInstanceListMap;
	
	/**
	 * The BRM task map
	 */
	public static Map<Long, ArrayList<Task>> brmWorkTaskListMap;

	/**
	 * The BRM reference acquisition map
	 */
	public static Map<Long, ArrayList<Acquisition>> brmRefAcqListMap;
	
	/**
	 * The BRM operation map
	 */
	public static Map<Long, DroolsOperations> brmOperMap;

	/**
	 * The BRM parameters map
	 */
	public static Map<Long, DroolsParameters> brmParamsMap;

	/**
	 * The BRM instance map
	 */
	public static Map<Long, Integer> brmInstanceMap;
	
	/**
	 * The map of the rejected list of rule elements
	 */
	public static HashMap<Long, HashMap<String, List<ReasonOfRejectElement>>> rejDTORuleListMap;
	
	/**
	 * The list of BRM working AR Id ranks
	 */
	public static HashMap<String, Integer> brmARIdRankMap;
	
	/**
	 * The list of BRM working AR Id ranks
	 */
	public static ArrayList<String> brmARIdRankList;
	
	/**
	 * The collection of BRM working DTOs
	 */
	public static Collection<SchedDTO> brmWorkDTOsList;
	
	/**
	 * Check the consistency of a DTO to be planned with the set of scheduling rules
	 * within BRM
	 * // TODO: changed on 20/07/2022 to manage common download on
	 *
	 * @param pSId - the Planning Session Id
	 * @param inputDTO - the input DTO
	 * @return the acceptance boolean
	 */
	public boolean planSchedDTO(Long pSId, SchedDTO inputDTO) {
		
		logger.debug("Try to plan DTO " + inputDTO.getDTOId() + " on satellite " + inputDTO.getSatelliteId());
				
		/**
		 * The boolean for the nextAR acceptance
		 */
		boolean result = true;

		/**
		 * Instance handler
		 */
		TaskPlanned taskPlanned = new TaskPlanned();

		
		try {
			
			// Set DTO decrement BIC if (required)
			inputDTO.setDecrBIC(BICCalculator.isDecrBIC(pSId, inputDTO));
			
			// 1.0 Filter DTO based on the MH applicability
			if (! RequestChecker.isInsideMH(pSId, inputDTO)) {

				logger.info("The internal DTO " + inputDTO.getDTOId() + " is rejected "
						+ "because outside of the relevant Mission Horizon.");

				result = false;
			}

			// 1.1. Filter DTOs if already passed to BRM
			if (taskPlanned.receiveAcceptedAcquisitionWithId(inputDTO.getDTOId(), 
					pSId.toString(), brmInstanceMap.get(pSId), brmParamsMap.get(pSId)) != null) {

				logger.info("The internal DTO " + inputDTO.getDTOId() + " is removed "
						+ "because relevant acquisition task already exists.");
				
				result = false;
			}

			if (result) {

				logger.debug("DTO: " + inputDTO.getDTOId() + " from - to: " + inputDTO.getStartTime().toString() + " - "
						+ inputDTO.getStopTime().toString() + " is inserted into BRM.");
				
				boolean forceIntDwlFlag = false;
	
				if (inputDTO.getUserInfoList().size() > 1 && SessionChecker.isCommonExtStation(
						pSId, inputDTO.getUserInfoList().get(0).getUgsId(),
						inputDTO.getUserInfoList().get(1).getUgsId())) {
					
					forceIntDwlFlag = true;
					
					logger.debug("Force Download on a common external acquisition station.");
					
					// Insert Equivalent DTO (forceIntDwlFlag added on 20/07/2022)
					result = brmOperMap.get(pSId).insertDto(brmParamsMap.get(pSId), ObjectMapper.parseSchedToBRMDTO(
							pSId, inputDTO), pSId.toString(), brmInstanceMap.get(pSId), inputDTO.isPrevPlanned(), forceIntDwlFlag);
				
				} else {


					// 2.0. Insert DTO and  check planning result from BRM
					result = brmOperMap.get(pSId).insertDto(brmParamsMap.get(pSId), ObjectMapper.parseSchedToBRMDTO(
							pSId, inputDTO), pSId.toString(), brmInstanceMap.get(pSId), inputDTO.isPrevPlanned());				
				}
				
				if (result) {

					logger.info("Incoming list of DTOs is scheduled.");
					
					// 2.1.1. Remove rejections
					rejDTORuleListMap.get(pSId).remove(inputDTO.getDTOId());

				} else {

					logger.info("Incoming list of DTOs is rejected.");

					// 2.1.2. Update rejected DTOs from BRM
					setRejectedDTOs(pSId);
				}

			} else {

				logger.warn("No new DTOs to be analyzed.");
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
			
			result = false;
		}

		logger.info("Acceptance of the incoming AR is: " + result);

		return result;
	}

	/**
	 * Check the consistency of a list of DTOs to be planned with the set of
	 * scheduling rules within BRM
	 *
	 * @param pSId - the Planning Session Id
	 * @param inputDTOList - the list of input DTOs
	 * @param isDecrBIC - the decrement BIC flag
	 * @return the acceptance boolean
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public boolean planSchedDTOList(Long pSId, ArrayList<SchedDTO> inputDTOList, boolean isDecrBIC) {
		
		
		logger.info("Try the scheduling of the requested DTOs.");

		/**
		 * The boolean for the nextAR acceptance
		 */
		boolean result = false;

		/**
		 * Instance handler
		 */
		TaskPlanned taskPlanned = new TaskPlanned();
		
		try {

			/**
			 * The input list of DTOs
			 */
			ArrayList<SchedDTO> schedDTOList = (ArrayList<SchedDTO>) inputDTOList.clone();

			// 1.0 Filter DTOs based on the MH applicability
			for (int i = 0; i < schedDTOList.size(); i++) {

				// Set decrement BIC
				schedDTOList.get(i).setDecrBIC(isDecrBIC);
				
				if (! RequestChecker.isInsideMH(pSId, schedDTOList.get(i))) {

					logger.warn("The internal DTO " + schedDTOList.get(i).getDTOId() + " is removed "
							+ "because outside of the relevant Mission Horizon. ");

					// Remove DTO
					schedDTOList.remove(i);

					i--;				

				// 1.1. Filter DTOs if already passed to BRM
				} else if (taskPlanned.receiveAcceptedAcquisitionWithId(schedDTOList.get(i).getDTOId(), 
						pSId.toString(), brmInstanceMap.get(pSId), brmParamsMap.get(pSId)) != null) {

					logger.debug("The internal DTO " + schedDTOList.get(i).getDTOId() + " is removed "
							+ "because relevant acquisition task already exists. ");
					
					// Remove DTO
					schedDTOList.remove(i);

					i--;
				}
			}

			// 1.2. Order DTO List according to priority
			Collections.sort(schedDTOList, new SchedDTOPriorityComparator());

			// 2.0. Schedule DTO
			if (schedDTOList.size() > 0) {

				for (int i = 0; i < schedDTOList.size(); i++) {

					// Insert DTO 
					logger.debug("DTO: " + schedDTOList.get(i).getDTOId() + " from - to: "
							+ schedDTOList.get(i).getStartTime().toString() + " - " 
							+ schedDTOList.get(i).getStopTime().toString()
							+ " is inserted into BRM.");
				}

				// 2.1 Check DTO result in BRM
				result = brmOperMap.get(pSId).insertListDto(
						ObjectMapper.parseSchedToBRMDTOList(pSId, schedDTOList), 
						pSId.toString(), brmInstanceMap.get(pSId), brmParamsMap.get(pSId), false);

				if (result) {

					logger.info("Incoming list of DTOs is valid.");

					// 2.2.1 Remove feasible DTO from rejection list
					for (SchedDTO schedDTO : schedDTOList) {

						rejDTORuleListMap.get(pSId).remove(schedDTO.getDTOId());
					}

				} else {

					// 2.2.2 Update rejected DTOs from BRM
					logger.debug("Update rejected DTOs from BRM.");
					setRejectedDTOs(pSId);
				}

			} else {

				logger.warn("No new DTOs to be analyzed.");
			}

			// 3.0 Update scheduled tasks from BRM
			updateSchedTasks(pSId, false);

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		logger.info("Acceptance of the incoming DTOs is: " + result);

		return result;
	}

	/**
	 * Check the consistency of an equivalent DTO to be planned with the set of
	 * scheduling rules within BRM
	 * 
	 * TODO: Updated on 20/07/2022 to manage international DI2S downloads, 
	 * with the flag forceIntDwlFlag as a temporary solution.
	 *
	 * @param pSId - the Planning Session Id
	 * @param brmEquivDTO - the Equivalent DTO
	 * @param isPrevPlan - the previous planned flag
	 * @return the acceptance boolean
	 */
	public boolean planEquivDTO(Long pSId, EquivalentDTO brmEquivDTO, boolean isPrevPlan) {

		/**
		 * The boolean for the nextAR acceptance
		 */
		boolean result = false;

		try {
			
			logger.trace("Inserted equivalent DTO: " + brmEquivDTO.toString());
			
			// Check PR Mode
			if (brmEquivDTO.getEquivType().equals(PRMode.DI2S)) {

				/**
				 * The equivalent BIC value
				 */
				double equivBIC = 0;

				/**
				 * The flag to force dwl on international station
				 */
				boolean forceIntDwlFlag = false;
				

				logger.debug("Insert DI2S DTOs relevant to Equivalent DTO: " + brmEquivDTO.getEquivalentDtoId());

				for (DTO dto : brmEquivDTO.getAllDtoInEquivalentDto()) {
									
					// Update equivalent BIC
					equivBIC += dto.getImageBIC();
					
					if (!dto.getUserInfo().isEmpty() && dto.getDi2sInfo() != null
							&& SessionChecker.isCommonExtStation(pSId, dto.getUserInfo().get(0).getUgsId(),
							dto.getDi2sInfo().getUgsId())) {
						
						forceIntDwlFlag = true;

						logger.debug("Positively Checked DTO: " + dto.toString());
						logger.debug("Force Download on a common external acquisition station.");
						
						// Insert Equivalent DTO (forceIntDwlFlag added on 20/07/2022)
						result = brmOperMap.get(pSId).insert_DI2S(brmParamsMap.get(pSId), brmEquivDTO, 
								equivBIC, pSId.toString(), brmInstanceMap.get(pSId), isPrevPlan, forceIntDwlFlag);
						
						break;
					}
				}

				if (! forceIntDwlFlag) {
					
					// Insert Equivalent DTO 
					result = brmOperMap.get(pSId).insert_DI2S(brmParamsMap.get(pSId), brmEquivDTO, 
							equivBIC, pSId.toString(), brmInstanceMap.get(pSId), isPrevPlan);
				}
					
			} else if (brmEquivDTO.getEquivType().equals(PRMode.Theatre)) {
				
				logger.debug("Insert Theatre DTOs relevant to Equivalent DTO: " + brmEquivDTO.getEquivalentDtoId());
				
				if (brmEquivDTO.getManAssociated() != null
						&& ! brmEquivDTO.getManAssociated().isEmpty()) {
					
					if (brmEquivDTO.getManAssociated().get(0).getPitchIntervals() != null
							&& ! brmEquivDTO.getManAssociated().get(0).getPitchIntervals().isEmpty()) {
				
						logger.debug("Start Maneuver has Pitch Interval size: " + 
						brmEquivDTO.getManAssociated().get(0).getPitchIntervals().size());
					
					} else {
										
						logger.debug("No Pitch Intervals found.");			
					}
				
				} else {
					
					logger.debug("No Maneuvers found.");			
				}
				
				// Insert Theatre
				result = brmOperMap.get(pSId).insert_Theatre(brmParamsMap.get(pSId), brmEquivDTO, 
						pSId.toString(), brmInstanceMap.get(pSId), isPrevPlan);

			} else {

				logger.debug("Insert Experimental DTOs relevant to Equivalent DTO: " + brmEquivDTO.getEquivalentDtoId());
		
				if (brmEquivDTO.getManAssociated() != null
						&& ! brmEquivDTO.getManAssociated().isEmpty()) {
					
					if (brmEquivDTO.getManAssociated().get(0).getPitchIntervals() != null
							&& ! brmEquivDTO.getManAssociated().get(0).getPitchIntervals().isEmpty()) {
				
						logger.debug("Start Maneuver has Pitch Interval size: " + 
						brmEquivDTO.getManAssociated().get(0).getPitchIntervals().size());
					
					} else {
										
						logger.debug("No Pitch Intervals found.");			
					}
				
				} else {
					
					logger.debug("No Maneuvers found.");			
				}
				
				// Insert Experimental
				result = brmOperMap.get(pSId).insert_Experimental(brmParamsMap.get(pSId), brmEquivDTO, 
						pSId.toString(), brmInstanceMap.get(pSId), isPrevPlan);
			}

			if (result) {

				logger.info("Incoming Equivalent DTO is valid.");

			} else {

				logger.info("Incoming Equivalent DTO is NOT valid, latest AR is rejected.");
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return result;
	}
	
	/**
	 * Plan Downloads (to be called after initBRMWorkDTOs)
	 * 
	 * @param pSId - the Planning Session Id
	 * @return the acceptance boolean
	 */
	public boolean planBRMDownloads(Long pSId) {

		/**
		 * The output result
		 */
		boolean result = true;
		
		/**
		 * Instance handler
		 */
		TaskPlanned taskPlanned = new TaskPlanned();
		
		try {

			logger.info("Plan downloads relevant the working "
					+ "and reference Planning Sessions for Planning Session: " + pSId);
			
			// Process Downloads from working session
			brmOperMap.get(pSId).processDownloadsFromPrevSession(
					brmParamsMap.get(pSId), pSId.toString(), brmInstanceMap.get(pSId));
			
			/**
			 * The list of downloads from both satellites
			 */
			List<Download> dwlList = taskPlanned.receiveAllDownloads(pSId.toString(), brmInstanceMap.get(pSId), 
					brmParamsMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(0).getCatalogSatellite().getSatelliteId());
			
			if (SessionScheduler.satListMap.get(pSId).size() > 1) {

				dwlList.addAll(taskPlanned.receiveAllDownloads(pSId.toString(), brmInstanceMap.get(pSId), 
						brmParamsMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(1).getCatalogSatellite().getSatelliteId()));
			}
			
//			logger.debug("A number of initial downloads: " + dwlList.size() 
//			+ " is raised for Planning Session: " + pSId);
			
			// Update initial scheduled tasks
			updateSchedTasks(pSId, true);

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
			
			result = false;
		}

		return result;
	}

	/**
	 * Purge scheduled requests 
	 * // TODO: purge specific sensor modes only --> with BRM
	 *
	 * @param pSId - the Planning Session Id
	 * @return the acceptance boolean 
	 */
	public boolean purgeSchedTasks(Long pSId) {

		boolean result = false;

		try {

			if (SessionActivator.planSessionMap.get(pSId) != null) {

				// 1.0. Purge scheduled tasks from BRM
				logger.info("Purge scheduled tasks for Planning Session: " + pSId);
				brmOperMap.get(pSId).clearSessionInstance(pSId.toString(), brmInstanceMap.get(pSId), 
						brmParamsMap.get(pSId), true);
				
				// 1.1. Update scheduled tasks from BRM in the Planning Session
				updateSchedTasks(pSId, true);
				
				result = true;

			} else {

				logger.info("Resources associated to Planning Session " + pSId + " previously purged.");
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return result;
	}

	/**
	 * Retract a DTO by Id
	 *
	 * @param pSId - the Planning Session Id
	 * @param retrDTOId - the retracted DTO id
	 * @param reason - the reason of rejection
	 * @return the acceptance boolean
	 */
	@SuppressWarnings("static-access")
	public boolean retractDTOById(Long pSId, String retrDTOId, ReasonOfReject reason) throws Exception {
		
		logger.debug("Internal DTO: " + retrDTOId + " is retracted from BRM.");

				// Retract single acquisition					   
		brmOperMap.get(pSId).retractSingleAcq(brmParamsMap.get(pSId), retrDTOId, pSId.toString(), 
				brmInstanceMap.get(pSId), reason);

		return true;
	}

	/**
	 * Retract a DTO from the plan of BRM // TODO: TBD if other data are necessary
	 *
	 * @param pSId - the Planning Session Id
	 * @param retrDTOId - the retracted DTO
	 * @param reason - the reason of rejection
	 * @return the acceptance boolean
	 */
	@SuppressWarnings("static-access")
	public boolean retractDTO(Long pSId, SchedDTO retrDTO, ReasonOfReject reason) throws Exception {
		
		logger.debug("Internal DTO: " + retrDTO.getDTOId() + " is retracted from BRM.");

		brmOperMap.get(pSId).retractSingleAcq(brmParamsMap.get(pSId),retrDTO.getDTOId(), pSId.toString(), 
				brmInstanceMap.get(pSId), reason);

		return true;
	}

	/**
	 * Retract a DTO by Id
	 *
	 * @param pSId - the Planning Session Id
	 * @param retrDTOId - the retracted DTO Id
	 * @param reason - the reason of rejection
	 * @return the acceptance boolean
	 */
	@SuppressWarnings("static-access")
	public boolean retractSubDTOById(Long pSId, String retrDTOId, ReasonOfReject reason) throws Exception {
		
		logger.debug("Internal DTO: " + retrDTOId + " is retracted from BRM.");

		/**
		 * The list of scheduled DTOs
		 */
		ArrayList<SchedDTO> schedDTOList = getAcceptedDTOs(pSId);
		
		for (SchedDTO schedDTO : schedDTOList) {
		
			if (schedDTO.getPRId().equals(ObjectMapper.getPRId(retrDTOId)))  {

				// Retract single acquisition
				brmOperMap.get(pSId).retractSingleAcq(brmParamsMap.get(pSId), retrDTOId, pSId.toString(), 
						brmInstanceMap.get(pSId), reason);
								
				// Replan SchedDTO	
				logger.debug("Replan DTO filtered by subscribers: " + schedDTO.getDTOId());
				
				planSchedDTO(pSId, schedDTO);

			}
		}

		return true;
	}

	/**
	 * Retract a list DTO from the plan of BRM // Finalize HpExclusionOrbitalSegment
	 * retrieval
	 *
	 * @param pSId - the Planning Session Id
	 * @param retrDTOList - the list of retracted DTO
	 * @param reason - the reason of rejection
	 * @return the acceptance boolean
	 */
	public boolean retractDTOList(Long pSId, ArrayList<SchedDTO> retrDTOList, ReasonOfReject reason) 
			throws Exception {
		
		if (retrDTOList != null && !retrDTOList.isEmpty()) {
		
			logger.info("Retract DTOs relevant to AR: " + retrDTOList.get(0).getARId());
			
			for (int i = 0; i < retrDTOList.size(); i++) {
	
				retractDTO(pSId, retrDTOList.get(i), reason);
			}
		}

		// // Update planned tasks by BRM
		// // TODO: confirm deletion
		// ArrayList<Task> brmTasks = (ArrayList<Task>)
		// brmOperMap.get(pSId).receiveDtoAccepted(0);
		//
		// brmTaskMap.put(pSId, brmTasks);

		return true;
	}

	/**
	 * Setup the BRM session
	 *
	 * // TODO: separate CMGAs for each satellite
	 *
	 * @param pS - the Planning Session
	 * @param satList - the list of satellite
	 * @return the setup boolean
	 */
	public static boolean setupBRMSession(PlanningSession pS,
			List<it.sistematica.spla.datamodel.core.model.resource.Satellite> satList) {

		/**
		 * The output result
		 */
		boolean result = false;

		try {

			logger.info("Setup resources for the BRM session...");
			Long pSId = pS.getPlanningSessionId();
			
			/**
			 * The BRM resources
			 */
			List<Satellite> brmSatList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<Visibility> brmVisList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<PAW> brmPawList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<PDHT> brmPdhtList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<Eclipse> brmEclipseList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<SatelliteState> brmStateList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			List<CMGA> brmCMGList = new ArrayList<>();
			/**
			 * The BRM resources
			 */
			Map<String, Map<Integer, Double>> hpOrbitLimitMap = new HashMap<String, Map<Integer, Double>>();
			/**
			 * The BRM resources
			 */
			List<com.nais.spla.brm.library.main.ontology.resourceData.HPExclusion> hpOrbitExcl = new ArrayList<com.nais.spla.brm.library.main.ontology.resourceData.HPExclusion>();

			// Provide satellite data to BRM
			logger.info("Provide satellite data to BRM.");

			if (satList.size() == 0) {

				logger.warn("No satellite data are retrieved in DPL. ");
				logger.warn("A default satellite is built to support the scheduling process.");
				satList.add(DefaultDebugger.getDefaultSatellite(pS, pS.getMissionHorizonStopTime().getTime(), "SSAR1"));
				satList.add(DefaultDebugger.getDefaultSatellite(pS, pS.getMissionHorizonStopTime().getTime(), "SSAR2"));
			}

			for (int i = 0; i < satList.size(); i++) {

				if (satList.get(i).getCatalogSatellite() != null) {

					/**
					 * The satellite Id
					 */
					String satId = satList.get(i).getCatalogSatellite().getSatelliteId();

//					hpOrbitLimitMap.put(satId, new HashMap<Integer, Double>());

					logger.debug("Parse satellite data.");
					brmSatList.add(ObjectMapper.parseDMToBRMSat(pSId, satList.get(i)));

					logger.debug("Parse PAW data.");
					brmPawList.addAll(ObjectMapper.parseDMToBRMPAWList(satList.get(i).getPlatformActivityWindowList(),
							satList.get(i).getCatalogSatellite().getSatelliteId()));

					logger.debug("Parse PDHT data.");
					brmPdhtList.add(ObjectMapper.parseDMToBRMPDHT(pSId, satList.get(i).getPdht(),
							satList.get(i).getCatalogSatellite()));

					if (satList.get(i).getEclipseList().size() > 0) {

						logger.debug("Parse eclipse data.");
						for (int j = 0; j < satList.get(i).getEclipseList().size(); j++) {

							brmEclipseList
									.add(ObjectMapper.parseDMToBRMEclipse(pSId, satList.get(i).getEclipseList().get(j),
											satList.get(i).getCatalogSatellite().getSatelliteId()));
						}
					}

					logger.debug("Parse visibility data.");
					brmVisList.addAll(ObjectMapper.parseDMToBRMVisibilityList(pSId, satList.get(i).getVisibilityList(),
							satList.get(i).getCatalogSatellite().getSatelliteId()));

					logger.debug("Parse status data.");
					brmStateList.addAll(ObjectMapper
							.parseDMToBRMStateList(satList.get(i).getSar().getUnavailabilityWindows(), satList));

					if ((satList.get(i).getCatalogSatellite().getHpLimitation() != null)) {

						logger.debug("Parse HP Limitations.");
						hpOrbitLimitMap.put(satId,
								ObjectMapper.parseDMToBRMHPLimit(satList.get(i).getCatalogSatellite().getHpLimitation(),
										satList.get(i).getCatalogSatellite().getSatelliteId()));
					}

					logger.debug("Parse HP Exclusions.");
					if ((satList.get(i).getHpExclusionList() != null)) {

						for (it.sistematica.spla.datamodel.core.model.resource.HPExclusion hpExcl : satList.get(i)
								.getHpExclusionList()) {

							hpOrbitExcl.add(ObjectMapper.parseDMToBRMHPExcl(hpExcl,
									satList.get(i).getCatalogSatellite().getSatelliteId()));
						}
					}

					if ((satList.get(i).getCmga().getCatalogCMGA1() != null)
							&& (satList.get(i).getCmga().getCatalogCMGA2() != null)
							&& (satList.get(i).getCmga().getCatalogCMGA3() != null)) {

						logger.debug("Parse CMGA data.");
						brmCMGList.addAll(ObjectMapper.parseDMToBRMCMGA(satList.get(i).getCmga(),
								satList.get(i).getCatalogSatellite().getSatelliteId()));

					} else {

						logger.warn("No CMGA catalogs are found in DPL.");
						logger.warn("A default CMGA is built to support the scheduling process.");
						brmCMGList.addAll(ObjectMapper.parseDMToBRMCMGA(DefaultDebugger.getDefaultCMGAs(),
								satList.get(i).getCatalogSatellite().getSatelliteId()));
					}

				} else {

					logger.warn("No satellite catalogs are found in DPL.");
					logger.warn("A default satellite is built to support the scheduling process.");
					satList.set(i, DefaultDebugger.getDefaultSatellite(pS, pS.getMissionHorizonStopTime().getTime(),
							satList.get(i).getCatalogSatellite().getSatelliteId()));

					brmSatList.add(ObjectMapper.parseDMToBRMSat(pSId, satList.get(i)));
				}
			}

			/**
			 * The incoming Mission Horizon
			 */
			MissionHorizon mh = new MissionHorizon();
			mh.setStart(pS.getMissionHorizonStartTime());
			mh.setStop(pS.getMissionHorizonStopTime());

			// Parse partners data
			logger.debug("Parse partners data.");
			ArrayList<Partner> partnerList = ObjectMapper.parseDMToBRMPartners(pSId);

			// Setup BRM parameters
			logger.info("Setup BRM parameters...");			
			brmParamsMap.get(pSId).setTimeEnableCarrier(PersistPerformer.getCarrierResources(pSId)[0]); 
			brmParamsMap.get(pSId).setTimeDisableCarrier(PersistPerformer.getCarrierResources(pSId)[1]); 
			brmOperMap.get(pSId).setParameters(brmParamsMap.get(pSId), brmSatList, brmPdhtList, brmVisList, brmPawList,
					brmStateList, brmCMGList, brmEclipseList, partnerList, mh, hpOrbitLimitMap, hpOrbitExcl,
					Configuration.maxHPNumber, Configuration.extraLeftCost);

			// Setup BRM Environment
			logger.info("Setup BRM Environment...");
			DroolsEnvironment drlEnv = brmOperMap.get(pSId).setUpEnvironment(pSId.toString(),
					brmParamsMap.get(pSId), new String(), false);
			brmOperMap.get(pSId).setDroolsEnv(drlEnv);

			// Setup BRM instance
			brmInstanceMap.put(pSId, getBRMFreeInstance(pSId));
			
			/**
			 * The maneuvering times
			 */
			Long[] manTimes = PersistPerformer.getManResources(pSId);
			
			// Setup BRM session
			logger.debug("Setup BRM Planning Session...");
			brmOperMap.get(pSId).getDroolsEnvironment().setUpSession(pSId.toString(), brmParamsMap.get(pSId), 
					brmInstanceMap.get(pSId), ObjectMapper.parseDMToBRMSessionType(pSId), 
					Configuration.splitChar, manTimes[0].intValue(), manTimes[1].intValue());
			
			// Set response file
			String taskReportDir = Configuration.outputFileFolder + pSId + "-TaskReport-"
					+ SessionActivator.planDateMap.get(pSId).getTime();
			Files.createDirectory(new File(taskReportDir).toPath(), new FileAttribute<?>[0]);
				
			brmParamsMap.get(pSId).setResponceFile(taskReportDir);

			result = true;

		} catch (Exception ex) {

			logger.error("BRM setup is NOT successfully executed: " + ex.getStackTrace()[0].toString());

		}

		return result;

	}

	/**
	 *  Initialize BRM with the reference and working Planning Session
	 *  	
	 * @param pSId - the Planning Session Id
	 * @return the init boolean
	 */
	public boolean initBRMPlan(Long pSId) {

		logger.info("Initialize the BRM resources for Planning Session: " + pSId);
		
		// 1.0. Initialize BRM Tasks for the reference Planning Sessions
		boolean active1 = initBRMRefTasks(pSId);

		// 2.0. Initialize BRM Tasks for the working Planning Sessions
		boolean active2 = initBRMWorkDTOs(pSId);
		
		// 3.0. Plan BRM Downloads
		boolean active3 = planBRMDownloads(pSId);
	
		if (active1 && active2 && active3) {
			
			return true;
		
		} else {
		
			logger.error("Some resources are NOT correctly initialized by BRM for Planning Session: " + pSId);
		}
		
		return true;  // TODO! to be reset as false 
	}
		
	/**
	 * Set initial tasks within the BRM
	 * // TODO: check extraPitch BICs
	 * 
	 * @param pSId - the Planning Session Id
	 * @return  the init boolean
	 */
	@SuppressWarnings("unchecked")
	public boolean initBRMRefTasks(Long pSId) {

		/**
		 * The output boolean
		 */
		boolean initBool = false;
		
		/**
		 * The list of BRM tasks
		 */
		ArrayList<Task> brmTaskList = new ArrayList<Task>();
		
		try {

			/**
			 * The list of BRM previous tasks
			 */
			ArrayList<Task> brmRefTaskList = new ArrayList<Task>();

			// 1.0. Collect Reference Tasks
			logger.info("Collect a number of " + PersistPerformer.refTaskListMap.get(pSId).size() 
					+ " reference tasks to be inserted in BRM.");

			if (! PersistPerformer.refTaskListMap.get(pSId).isEmpty()) {

				logger.debug("Set " + PersistPerformer.refTaskListMap.get(pSId).size() 
						+ " BRM Tasks relevant to the reference Planning Sessions.");
				
				for (it.sistematica.spla.datamodel.core.model.Task refTask : PersistPerformer.refTaskListMap.get(pSId)) {

					if (! refTask.getTaskType().equals(it.sistematica.spla.datamodel.core.enums.TaskType.DLO) 
							&& ! refTask.getTaskMark().equals(TaskMarkType.DELETED)) {

						logger.debug("Insert reference Task [Type: " + refTask.getTaskType() + ", ID: " + refTask.getTaskId()
						+ ", Satellite: " + refTask.getSatelliteId() + ", Start Time: " + refTask.getStartTime()
						+ ", Stop Time: " + refTask.getStopTime() + "]");
						
						/** 
						 * The parsed DM to BRM task
						 */
						Task brmTask = ObjectMapper.parseDMToBRMTask(pSId, refTask);
																		
						logger.trace("Retrieved reference Task: " + refTask.toString());
						
						if (brmTask.getTaskType().equals(TaskType.ACQUISITION)) {
							
							// Add reference Acquisition 
							brmRefAcqListMap.get(pSId).add((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) brmTask);
							
							PersistPerformer.refAcqIdMap.get(pSId).put(brmTask.getIdTask(), refTask);													
							SessionScheduler.dtoImageIdMap.get(pSId).put(brmTask.getIdTask(), 
								((it.sistematica.spla.datamodel.core.model.task.Acquisition) refTask).getImageIdentifier());
						}

						if (refTask.getStartTime().compareTo(
								SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) < 0) {

							brmRefTaskList.add(brmTask);

						} else {

							brmTaskList.add(brmTask);
						}
					}
				}

				// TODO: add acquisition priority 
//				logger.debug("Sort previous acquisitions by priority.");
//				setPrevAcqIdRank(pSId, (ArrayList<Task>) brmTaskList.clone(), prevAcqList);
			
				/**
				 * The list of equivalent DTOs for experimental/theatre
				 */
				ArrayList<EquivalentDTO> brmEquivDTOList = ObjectMapper.parsePrevBRMEquivDTOList(
						pSId, PersistPerformer.getRefEquivDTOList(pSId));
				
				// 1.1. Update Reference Tasks
				updateRefTasks(pSId, brmRefTaskList);
				
				// 1.2. Initialize BRM with tasks relevant to the previous MH		
				logger.info("Initialize BRM Plan with tasks relevant to the reference Planning Sessions.");
				brmOperMap.get(pSId).initPlan(brmParamsMap.get(pSId), (List<Task>) brmRefTaskList.clone(),
						brmEquivDTOList, pSId.toString(), brmInstanceMap.get(pSId), true, false);			
				
			} else {
				
				logger.info("No applicable reference Planning Sessions exist "
						+ "for Planning Session: " + pSId);				
			}
			
			initBool = true;
			
		} catch (Exception ex) {

			logger.error("Some expected reference Tasks are NOT imported in BRM: " + ex.getStackTrace()[0].toString());

			initBool = false;
		}	
		
		return initBool;
	}

	/**
	 * Update Reference tasks
	 * @param pSId
	 * @param brmRefTaskList
	 */
	private void updateRefTasks(Long pSId, ArrayList<Task> brmRefTaskList) {
		
		try {
		
			for (Task brmTask : brmRefTaskList) {
				
				if (brmTask.getTaskType().equals(TaskType.ACQUISITION) 
						&& brmTask.getIdTask() != null) {
				
					// Set reference EquivalentDTO			
					if (PRListProcessor.equivIdSchedARIdMap.get(pSId)
							.containsValue(ObjectMapper.getSchedARId(brmTask.getIdTask()))
						&& PRListProcessor.aRSchedIdMap.get(pSId).containsKey(
								ObjectMapper.getSchedARId(brmTask.getIdTask()))
						&& PRListProcessor.aRSchedIdMap.get(pSId).get(
								ObjectMapper.getSchedARId(brmTask.getIdTask())).getEquivalentDTO() != null) {
						
						brmTask.setReferredEquivalentDto(PRListProcessor.aRSchedIdMap.get(pSId).get(
								ObjectMapper.getSchedARId(brmTask.getIdTask())).getEquivalentDTO().getEquivalentDtoId());			
					}
				}
			}
		
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}
	
	/**
	 * Set the BRM initial DTO list
	 * 
	 * @param pSId - the Planning Session Id
	 * @return the init boolean
	 */
	@SuppressWarnings("unchecked")
	public boolean initBRMWorkDTOs(Long pSId) {

		/**
		 * The initialization boolean
		 */
		boolean initBool = false;
		
		try {

			/**
			 * The initial acquisitions list
			 */
			ArrayList<Acquisition> initAcqList = new ArrayList<Acquisition>();
			
			/**
			 * The list of previous acquisitions
			 */
			ArrayList<it.sistematica.spla.datamodel.core.model.task.Acquisition> brmWorkAcqList = new ArrayList<>();

			/**
			 * The list of master DI2S DTO Ids
			 */
			ArrayList<String> masterDI2SDTOIdList = new ArrayList<String>();
			
			/**
			 * The list of slave DI2S DTO Ids
			 */
			ArrayList<String> slaveDI2SDTOIdList = new ArrayList<String>();
			
			/**
			 * The list of missing DTO Ids
			 */
			ArrayList<String> missAcqIdList = new ArrayList<String>();
			
			if (SessionActivator.planSessionMap.get(pSId).getPlanningSessionType().equals(
					PlanningSessionType.ManualPlanning)) {
				
				logger.info("Skip DTO initialization for ManualPlanning of Planning Session: " + pSId);
				
				PersistPerformer.workTaskListMap.get(pSId).clear();
				
				return true;
			}	
			
			logger.info("Collect a number of " + PersistPerformer.workTaskListMap.get(pSId).size() 
					+ " working tasks to be inserted in BRM.");

			if (!PersistPerformer.workTaskListMap.get(pSId).isEmpty()) {
				
				int count = 0;
				
				for (it.sistematica.spla.datamodel.core.model.Task task : PersistPerformer.workTaskListMap.get(pSId)) {

					logger.debug("Working Task [Type: " + task.getTaskType() + ", ID: " + task.getTaskId() + " MARK: " + task.getTaskMark()
					+ ", Satellite: " + task.getSatelliteId() + ", Start Time: " + task.getStartTime()
					+ ", Stop Time: " + task.getStopTime() + "]");
					
					if (!task.getTaskMark().equals(TaskMarkType.DELETED)) {
					
						if (task.getTaskType().equals(it.sistematica.spla.datamodel.core.enums.TaskType.ACQ)) {
						
							/**
							 * The scheduling DTO Id
							 */
							String schedDTOId = ObjectMapper.parseDMToSchedDTOId(task.getUgsId(), 
									task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId());
							
							brmWorkAcqList.add((it.sistematica.spla.datamodel.core.model.task.Acquisition) task);
							
							missAcqIdList.add(schedDTOId);
	
							if (task.getDi2s() != null) {
								
								/**
								 * The slave DTO Id
								 */						
								String slaveDTOId = ObjectMapper.parseDMToSchedDTOId(task.getDi2s().getUgsId(), 
										task.getDi2s().getProgrammingRequestId(), task.getDi2s().getAcquisitionRequestId(), 
										task.getDi2s().getDtoId());								
	
								if (!slaveDTOId.contains("null")) {
	
									logger.debug("DI2S Info found for Master DTO: " + schedDTOId);								
	
									masterDI2SDTOIdList.add(schedDTOId);
																	
									if (!slaveDTOId.equals(schedDTOId)) {
										
										logger.debug("DI2S Info found for Slave DTO: " + slaveDTOId);
										
										slaveDI2SDTOIdList.add(slaveDTOId);		
									}
									
									EquivDTOHandler.di2sLinkedIdsMap.get(pSId).put(schedDTOId, slaveDTOId);
								}
							}
						}
						
						count ++;
					}
				}
				
				logger.info("A number of " + count + " working tasks is applicable for Planning Session: " + pSId);

				// Add slave DTO Id list			
				EquivDTOHandler.slaveDTOIdListMap.get(pSId).addAll((ArrayList<String>) 
						slaveDI2SDTOIdList.clone());							

				// changed 11/07/2020 ------------	 
				
				brmWorkDTOsList = (ArrayList<SchedDTO>) SessionActivator.initSchedDTOListMap.get(pSId).clone();
				
				brmARIdRankMap = (HashMap<String, Integer>) PRListProcessor.schedARIdRankMap.get(pSId).clone();
				
				brmARIdRankList = new ArrayList<>(brmARIdRankMap.keySet());
					
				// Sort working AR Id rank
				Collections.sort(brmARIdRankList, new SchedARIdRankComparator());
			
				for (String schedARId : brmARIdRankList) {
					
					if (SessionActivator.initARIdEquivDTOMap.get(pSId).containsKey(schedARId)) {
						
						// Plan Equivalent DTO
						planEquivDTO(pSId, SessionActivator.initARIdEquivDTOMap.get(pSId).get(schedARId), true);
					
					} else if (SessionActivator.initSchedARIdDTOMap.get(pSId).containsKey(schedARId)) {
							
						// Plan DTO
						planSchedDTO(pSId, SessionActivator.initSchedARIdDTOMap.get(pSId).get(schedARId));
					}
				}
					
				// TODO: check remaining data
				brmWorkDTOsList.clear();
				brmARIdRankMap.clear();
				brmARIdRankList.clear();
				
				// --------------------------------------------------------				
				
				// 3.0. Check the initial acquisition tasks consistency 			
				for (it.sistematica.spla.datamodel.core.model.resource.Satellite sat : 
					SessionScheduler.satListMap.get(pSId)) {
							
					// The working Mission Horizon
					MissionHorizon mh = new MissionHorizon();
					mh.setStart(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime());
					mh.setStop(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime());
					
					initAcqList.addAll(brmOperMap.get(pSId).receiveAllAcquisitionsForMH(
							pSId.toString(), brmInstanceMap.get(pSId), 
							sat.getCatalogSatellite().getSatelliteId(), mh));
								
					// Check Acquisition consistency inside the Mission Horizon
					for  (int i = 0; i < initAcqList.size(); i++) {
												
						if (missAcqIdList.contains(((Acquisition) initAcqList.get(i)).getId())) {
							
							missAcqIdList.remove(initAcqList.get(i).getId());
						}
						
						if (! RequestChecker.isInsideMH(pSId, initAcqList.get(i))) {
														
							logger.debug("Removed acquisiton relevant to DTO: " + initAcqList.get(i).getId());
							initAcqList.remove(i);
							
							i --;
						}
					}
				}
				
				logger.info("Inserted number of working Acquisitions: " + brmWorkAcqList.size()); 
				logger.info("Inserted number of reference Acquisitions: " + brmRefAcqListMap.get(pSId).size());
				logger.info("Inserted number of total Acquisitions: " + (brmWorkAcqList.size() 
						+ brmRefAcqListMap.get(pSId).size()));
				
				logger.info("Scheduled number of initial working Acquisitions: " + initAcqList.size());
				
				// 4.1 Check the acquisition consistency
				if (brmWorkAcqList.size() == initAcqList.size()) {
					
					logger.info("The number of initial working Acquisitions is consistent "
							+ "for the Planning Session: " + pSId);
				
					initBool = true;
					
				} else {
					
					for (it.sistematica.spla.datamodel.core.model.task.Acquisition acq : brmWorkAcqList) {
						
						logger.trace("Working acquisition: " + ObjectMapper.parseDMToSchedDTOId(
								acq.getUgsId(), acq.getProgrammingRequestId(), 
								acq.getAcquisitionRequestId(), acq.getDtoId()));
					}
					
					logger.error("The initial number of acquisitions is NOT consistent "
							+ "for the Planning Session: " + pSId);
					
					logger.info("Compute Missed Acquisitions for the Planning Session: " + pSId);
							
					for (String missAcqId : missAcqIdList) {
						
						logger.info("Missed Acquisition: " + missAcqId);
					}
				}
				
			} else {
				
				logger.info("No applicable working Planning Session exist "
						+ "for the Planning Session: " + pSId);
				
				initBool = true;
			}
			
		} catch (Exception ex) {

			logger.error("Some expected working Tasks are NOT imported in BRM: " + ex.getStackTrace()[0].toString());
			
			initBool = false;
		}
		
		return initBool;
	}
	
	/**
	 * Update the scheduled tasks based on the BRM output in the Planning Session
	 *
	 * @param pSId - the Planning Session Id
	 * @return the update boolean
	 */
	@SuppressWarnings("unchecked")
	public void updateSchedTasks(Long pSId, boolean isToList) {

		try {

			logger.info("Update scheduled tasks for Planning Session: " + pSId);
			
			/**
			 * The list of tasks from BRM
			 */
			ArrayList<Task> brmTaskList = new ArrayList<>();

			/**
			 * The list of acquisitions from BRM
			 */
			List<Acquisition> brmAcqList = brmOperMap.get(pSId).receiveAllAcquisitions(pSId.toString(), 
					brmInstanceMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(0)
					.getCatalogSatellite().getSatelliteId());
			
			if (SessionScheduler.satListMap.get(pSId).size() > 1) {
				
				brmAcqList.addAll(brmOperMap.get(pSId).receiveAllAcquisitions(pSId.toString(), 
						brmInstanceMap.get(pSId), SessionScheduler.satListMap.get(pSId).get(1)
						.getCatalogSatellite().getSatelliteId()));
			}
			
			// Remove out of MH acquisitions
			for (int i = 0; i < brmAcqList.size(); i++) {
				
				if (!RequestChecker.isInsideMH(pSId, brmAcqList.get(i))) {
					
					brmAcqList.remove(i);
					
					i --;
				}			
			}

			/**
			 * The BRM operation iterator
			 */
			Iterator<Entry<String, Task>> it = brmOperMap.get(pSId)
					.getAllTasksAcceptedAsMap(brmParamsMap.get(pSId), pSId.toString(), brmInstanceMap.get(pSId))
					.entrySet().iterator();

			/**
			 * The dwl number
			 */
		    int dwlNum = 0;	
		    
			while (it.hasNext()) {

				/**
				 * The BRM task
				 */
				Task task = it.next().getValue();
		
				if (task.getStartTime().after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& task.getStartTime().before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime())) {
					
					if (isToList) {
					
						logger.info("Retrieved task from BRM: " + task.getTaskType() + ", " + task.getIdTask() 
						+ ", with start time: " + task.getStartTime().toString() + " and stop time: " + task.getEndTime().toString());				
					}
					
					if (task.getStartTime().getTime() > SessionActivator.planSessionMap.get(pSId)
							.getMissionHorizonStartTime().getTime()) 
					{
	
						if (! task.getTaskType().equals(TaskType.ACQUISITION)) 
						{
							brmTaskList.add(0, task);
						}
						
						if (task.getTaskType().equals(TaskType.DOWNLOAD) 
								|| task.getTaskType().equals(TaskType.GPS))  
						{
							dwlNum ++;
						}
					}
				}
			}
			
			logger.info("The number of scheduled downloads equal to: " + dwlNum 
					+ " is planned for Planning Session: " + pSId);
			
			if (!SessionChecker.isFinal(pSId)) {

				logger.debug("Sort acquisitions by priority.");
				Collections.sort(brmAcqList, new AcqPriorityComparator());
			}

			// Add acquisition list
			brmTaskList.addAll(0, (ArrayList<Acquisition>) brmAcqList);

			logger.info("The number of scheduled tasks: " + brmTaskList.size() 
				+ " is collected by BRM for Planning Session: " + pSId);

			brmWorkTaskListMap.put(pSId, (ArrayList<Task>) brmTaskList.clone());

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}
	
	/**
	 * Set the rejected DTOs based on the BRM output in the Planning Session
	 * // TODO: check theatre case (for multiple DTOs)
	 *
	 * @param pSId - the Planning Session Id
	 * @throws Exception
	 */
	public void setRejectedDTOs(Long pSId) throws Exception {

		logger.info("Update rejected DTOs.");

		rejDTORuleListMap.get(pSId).clear();

		/**
		 * The map of the rejected elements from BRM
		 */
		Iterator<Entry<String, Acquisition>> it = brmOperMap.get(pSId).receiveDtoRejected(
				pSId.toString(), brmInstanceMap.get(pSId)).entrySet().iterator();

		while (it.hasNext()) {

			/**
			 * The acquisition map
			 */
			Map.Entry<String, Acquisition> entry = it.next();

			if (entry.getValue() != null) {

				// Add rejected Id into the map
				rejDTORuleListMap.get(pSId).put(entry.getKey(), entry.getValue().getReasonOfReject());
				
				logger.debug("Set rejected DTO: " + entry.getKey());

//				if (!entry.getValue().getReasonOfReject().isEmpty()) {
//					
//					if (!entry.getValue().getReasonOfReject().get(0).getReason()
//							.equals(ReasonOfReject.deletedByCsps)) {
//
//						// TODO: dto key!
//						if (rejDTORuleListMap.get(pSId).containsKey(schedARId.getDTOId()) {
//						 
//							logger.debug("Rejected DTO for the Planning Session: " + entry.getKey() 
//							+ " -> " + entry.getValue());
//						}
//					}
//				}
			}
		}
	}

	/**
	 * Clear the rejected DTOs from BRM
	 *
	 * @param pSId - the Planning Session Id
	 * @throws Exception
	 */
	public void clearRejectedDTOs(Long pSId) throws Exception {

		logger.info("Clear the previously rejected DTOs for Planning Session: " + pSId);
		
		/**
		 * Clear map of rejected elements from BRM
		 */
		brmOperMap.get(pSId).receiveDtoRejected(pSId.toString(), brmInstanceMap.get(pSId)).clear();

		logger.debug("Rejected DTOs cleared.");
	}

	/**
	 * Get the accepted DTOs from BRM for the current Mission Horizon
	 *
	 * @param pSId - the Planning Session Id
	 * @throws Exception
	 * @return the list of accepted DTos
	 */
	public ArrayList<SchedDTO> getAcceptedDTOs(Long pSId) throws Exception {

		/**
		 * The list of scheduled DTOs
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<>();

		/**
		 * The list of scheduled DTO Ids
		 */
		ArrayList<String> schedDTOIdList = new ArrayList<>();
		
		/**
		 * The iterator of scheduled acquisitions
		 */
		Iterator<Entry<String, Task>> it = brmOperMap.get(pSId).getAllTasksAcceptedAsMap(
				brmParamsMap.get(pSId), pSId.toString(), brmInstanceMap.get(pSId))
				.entrySet().iterator();
		
		/**
		 * The acceptance index
		 */		
		int i  = 0;

		while (it.hasNext() ) {

			/**
			 * The task mark
			 */
			Map.Entry<String, Task> entry = it.next();

			// Check task compatibility
			if (entry.getValue() != null && RequestChecker.isInsideMH(pSId, entry.getValue())) {
			
				// Get scheduled master DTO
				if ((entry.getValue().getTaskType().equals(TaskType.ACQUISITION)
						|| entry.getValue().getTaskType().equals(TaskType.BITE))
						&& !schedDTOIdList.contains(entry.getValue().getIdTask())) {
	
					schedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId, entry.getValue().getIdTask(), 
							PRListProcessor.dtoSchedIdMap.get(pSId).get(entry.getValue().getIdTask()), false));
				
					schedDTOIdList.add(entry.getValue().getIdTask());
					
					// Get DI2S DTO
					if (entry.getValue().getDi2sInfo() != null) {
						
						if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(entry.getValue().getDi2sInfo().getRelativeSlaveId())) {
						
							schedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId, entry.getValue().getDi2sInfo().getRelativeSlaveId(),
									PRListProcessor.dtoSchedIdMap.get(pSId).get(entry.getValue().getDi2sInfo().getRelativeSlaveId()), false));				
						
							schedDTOIdList.add(entry.getValue().getIdTask());
						
						} else {
							
							logger.warn("No slave DTO found relevant to the DI2S Master DTO Id: " + entry.getValue().getIdTask());
						}
					}
				}
			}
			
			i ++;
		}

		logger.debug("Scheduled DTO size: " + schedDTOIdList.size());
		logger.debug("Scheduled Products size: " + i);

		return schedDTOList;
	}
	
	/**
	 * Get the accepted Downloads for the given Planning Session
	 * @param pSId - the Planning Session Id
	 * @return the list of accepted download
	 */
	public List<Download> getAcceptedDownloads(Long pSId) {

		/**
		 * Instance handler
		 */
		TaskPlanned taskPlanned = new TaskPlanned();
		
		/**
		 * The BRM list of Downloads
		 */
		List<Download> brmDwlList = taskPlanned.receiveAllDownloads(pSId.toString(),
				brmInstanceMap.get(pSId), brmParamsMap.get(pSId), 
				SessionScheduler.satListMap.get(pSId).get(0).getCatalogSatellite().getSatelliteId());
		
		if (SessionScheduler.satListMap.get(pSId).size() > 1) {

			brmDwlList.addAll(taskPlanned.receiveAllDownloads(		
					pSId.toString(), brmInstanceMap.get(pSId), brmParamsMap.get(pSId),
					SessionScheduler.satListMap.get(pSId).get(1).getCatalogSatellite().getSatelliteId()));
		}
		
		return brmDwlList;
	}

	/**
	 * Get the accepted DTOs from BRM
	 *
	 * @param pSId - the Planning Session Id
	 * @throws Exception
	 * @return the list of accepted DTOs
	 */
	public ArrayList<SchedDTO> printAcceptedDTOs(Long pSId) throws Exception {

		/**
		 * The list of scheduled DTOs
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<>();

		for (it.sistematica.spla.datamodel.core.model.resource.Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			/**
			 * The iterator of scheduled acquisitions
			 */
			Iterator<Entry<String, Acquisition>> it = brmOperMap.get(pSId)
					.receiveAllAcquisitionsAsMap(pSId.toString(), brmInstanceMap.get(pSId), 
							brmParamsMap.get(pSId), sat.getCatalogSatellite().getSatelliteId())
					.entrySet().iterator();

			while (it.hasNext()) {

				Map.Entry<String, Acquisition> entry = it.next();

				if (entry.getValue() != null) {

					schedDTOList.add(ObjectMapper.parseBRMAcqToSchedDTO(pSId, entry.getValue()));

					logger.trace("Accepted DTO for the session: " + (entry.getValue()).getId());
				}
			}
		}

		return schedDTOList;
	}

	/**
	 * Get the planned DTO Ids
	 *
	 * @param pSId - the current Planning Session Id
	 * @throws Exception
	 * @return the list of accepted DTO Ids
	 */
	public static ArrayList<String> getPlannedDTOIds(Long pSId) {

		/**
		 * The list of scheduled DTO Ids
		 */
		ArrayList<String> schedDTOIdList = new ArrayList<>();

		for (it.sistematica.spla.datamodel.core.model.resource.Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			/**
			 * The iterator of scheduled acquisitions
			 */
			Iterator<Entry<String, Acquisition>> it = brmOperMap.get(pSId)
					.receiveAllAcquisitionsAsMap(pSId.toString(), brmInstanceMap.get(pSId), 
							brmParamsMap.get(pSId), sat.getCatalogSatellite().getSatelliteId())
					.entrySet().iterator();

			while (it.hasNext()) {

				Map.Entry<String, Acquisition> entry = it.next();

				if (entry.getValue() != null) {

					schedDTOIdList.add(entry.getValue().getId());
				}
			}
		}

		return schedDTOIdList;
	}
		
	/**
	 * Get the planned AR Ids
	 *
	 * @param pSId - the Planning Session Id
	 * @return the list of planned AR Ids
	 */
	public static ArrayList<String> getPlannedARIds(Long pSId) {

		/**
		 * The list of scheduled AR Ids
		 */
		ArrayList<String> schedARIdList = new ArrayList<>();

		for (it.sistematica.spla.datamodel.core.model.resource.Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			/**
			 * The iterator of scheduled acquisitions
			 */
			Iterator<Entry<String, Acquisition>> it = brmOperMap.get(pSId)
					.receiveAllAcquisitionsAsMap(pSId.toString(), brmInstanceMap.get(pSId), 
							brmParamsMap.get(pSId), sat.getCatalogSatellite().getSatelliteId())
					.entrySet().iterator();

			while (it.hasNext()) {

				Map.Entry<String, Acquisition> entry = it.next();

				if (entry.getValue() != null) {

					schedARIdList.add(ObjectMapper.getSchedARId(entry.getValue().getId()));
				}
				
				if (entry.getValue().getDi2sInfo() != null) {
					
					schedARIdList.add(ObjectMapper.getSchedARId(
							entry.getValue().getDi2sInfo().getRelativeSlaveId()));
				}
			}
		}

		return schedARIdList;
	}

	/**
	 * Check the scheduling of an AR by Id
	 *
	 * @param pSId - the Planning Session Id
	 * @param schedARId - the scheduling AR Id
	 * @return the check boolean
	 */
	public static boolean checkARIdScheduling(Long pSId, String schedARId) {
		
		/**
		 * The check result
		 */
		boolean result = false;

		if (RulesPerformer.getPlannedARIds(pSId).contains(schedARId)) {
					
			logger.info("Acquisition relevant to the AR: " + schedARId + " is scheduled.");
			
			// Update result
			result = true;
		}
		
//		for (PlanProgrammingRequestStatus pRStatus: SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()) {
//			
//			if (pRStatus.getUgsId().equals(ObjectMapper.getUgsId(schedARId)) 
//					&& pRStatus.getProgrammingRequestId().equals(ObjectMapper.getPRId(schedARId))) {
//			
//				for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {
//	
//					if (aRStatus.getAcquisitionRequestId().equals(ObjectMapper.getARId(schedARId))
//							&& aRStatus.getStatus().equals(AcquisitionRequestStatus.Scheduled)) {
//					
//						logger.info("Acquisition relevant to the AR: " + schedARId + " is scheduled.");
//		
//						// Update result
//						result = true;
//		
//						break;
//					}
//				}
//			}
//		}
			
		if (! result) {

			logger.info("Acquisition relevant to the AR: " + schedARId + " is NOT scheduled.");
		}

		return result;
	}
	
	/**
	 * Write BRM report file
	 *
	 * @param pSId - the Planning Session Id
	 * @throws IOException
	 */
	public static void writeBRMReportFile(Long pSId) throws IOException {

		logger.debug("Write BRM Task Report file for session: " + pSId);

		try {
		
		// Write to file		
		brmOperMap.get(pSId).writeToFile(pSId.toString(), brmInstanceMap.get(pSId), 
				brmParamsMap.get(pSId));
		
		} catch (Exception ex) {
			
			logger.error("Exception raised: "  + ex.getMessage());
		}
	}
	
	/**
	 * Write BRM report file
	 *
	 * @param pSId - the Planning Session Id
	 * @throws IOException
	 */
	public static void writeBRMReportFileName(Long pSId) throws IOException {

		logger.debug("Write BRM Task Report file for session: " + pSId);

		try {
		

		// Write to file		
		brmOperMap.get(pSId).writeToFile(pSId.toString(), brmInstanceMap.get(pSId), 
				brmParamsMap.get(pSId));
		
		} catch (Exception ex) {
			
			logger.error("Exception raised: "  + ex.getMessage());
		}
	}

	/**
	 * Setup the BRM partners with AR Id list
	 *
	 * @param pSId - the Planning Session Id
	 */
	@SuppressWarnings("unchecked")
	public static void setupBRMPartners(Long pSId) {

		logger.debug("Setup AR Ids for BRM partners.");
		
		for (Partner brmPartner : brmParamsMap.get(pSId).getAllPartners()) {

			// Set AR Id for Partner
			brmPartner.setArIdForPartner(
					(ArrayList<String>) SessionActivator.ownerARIdMap.get(pSId)
					.get(brmPartner.getPartnerId()).clone());
		}
	}
	
	/**
	 * Get free first free BRM instance
	 * 
	 * @param pSId - the Planning Session Id
	 * @return the free instance
	 */
	private static Integer getBRMFreeInstance(Long pSId) {
		
		/**
		 * The BRM instance
		 */
		Integer instance = 0;
		
		for (int i = 0; i < brmParamsMap.get(pSId).getNumberOfSessions(); i++) {
			
			if (! brmInstanceListMap.get(pSId).contains(i)) {
				
				// Update instance
				
				instance = i;
				
				brmInstanceListMap.get(pSId).add(instance);
				
				return instance;
			}
		}
		
		logger.warn("No free BRM instances found! A default instance is set for Planning Session: " + pSId);

		return instance;
	}
	
	/**
	 * Get the ESS ratio
	 * 
	 * @param pSId - the Planning Session Id
	 * @param dto - the input dto
	 * @return the ESS ratio
	 */
	public static double getESSRatio(Long pSId, DTO dto) {
		
		/**
		 * The ESS energy management
		 */
		EssEnergyManagement essManagement = new EssEnergyManagement();

		/**
		 * The ESS ratio
		 */
		double essRatio = essManagement.computeEssFromDTO(dto, brmParamsMap.get(pSId)) 
				/ (brmParamsMap.get(pSId).getPowersSensorMode().get(TypeOfAcquisition.STRIPMAP) * 1000.0); 
		
		return essRatio;
	}
	
	/**
	 * Update the number of the PDHT switches
	 * 
	 * @param pSId - the Planning Session Id
	 * @param satId - the satellite Id
	 * @param inSwithNum - the input number of switches
	 * @return - the output number of switches
	 */
	public int updatePdhtSwitches(long pSId, String satId, int inSwitchNum) {
		
		logger.debug("Update PDHT switches for satellite: " + satId);
		
		PdhtManagement pdhtManagement = new PdhtManagement();
		
		// Update PDHT // TODO: last factor?
		int outSwitchNum = pdhtManagement.updatePdhtSwitches(Long.toString(pSId), 
				brmInstanceMap.get(pSId), inSwitchNum, brmParamsMap.get(pSId), satId, 0L);
		
		return outSwitchNum;
	}
	
	/**
	 * Check the download consistency wrt overlaps and sizes
	 * 
	 * @param pSId - the Planning Session Id
	 * @return the check boolean
	 */
	public static boolean checkDwlConsistency(long pSId) {
		
		/**
		 * The consistency boolean
		 */
		boolean isConsistent = true;
		
		for (it.sistematica.spla.datamodel.core.model.resource.Satellite sat : SessionScheduler.satListMap.get(pSId)) {
		
			// Check download consistency for satellite
			isConsistent = brmOperMap.get(pSId).checkConsistenceDownloads(brmParamsMap.get(pSId), 
					sat.getCatalogSatellite().getSatelliteId());
				
				if  (!isConsistent) {
					
					break;
				}
				
		}
		
		return isConsistent;
		
	}
	
	/**
	 * Get the list of loan transactions for the owner Id from DPL
	 *
	 * // TODO: check loans and transactions according to Planning Session Id Hp:
	 * overwrite of transactions relevant to older sessions
	 * // TODO: manage transactions according to the Planning Session Id
	 *
	 * @param pSId - the Planning Session Id
	 * @param ownerId - the owner Id
	 * @param chartType - the transaction chart type
	 * @return the list of transactions relevant to the owner 
	 */
	public List<Transaction> getPartnerLoanTransList(Long pSId, String ownerId, String chartType) 
			throws Exception {

		/**
		 * Instance handler
		 */
		DroolsUtils droolsUtils = new DroolsUtils();
		
		/**
		 * The BRM partner
		 */
		com.nais.spla.brm.library.main.ontology.resources.Partner brmPartner = droolsUtils.receivePartnerWithId(
				ownerId, pSId.toString(), brmInstanceMap.get(pSId));

		logger.debug("Compute transactions for owner: " + ownerId);

		/**
		 * The list of owner balance transactions
		 */
		List<Transaction> balanceTransList = new ArrayList<>();

		/**
		 * The reference date of saving
		 */
		Date refDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime();

		if (SessionChecker.isFinal(pSId)) {
			// Get final reference date
			refDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime();
		}

		// Partners Credits
		for (CreditCard creditCard : brmPartner.getGivenLoan()) {

			if (!creditCard.isPrevious()) {

				/**
				 * The payoffs
				 */
				double payoffs = brmPartner.getPayOffs();

				if (payoffs == 0) {

					/**
					 * The pay date
					 */
					Date payDate = refDate;

					balanceTransList.add(new Transaction(payDate, chartType, brmPartner.getPartnerId(),
							creditCard.getDebitor(), creditCard.getBICLent()));

					logger.debug("Found transaction at date " + payDate.toString() 
					+ " for creditor " + brmPartner.getPartnerId() 
					+ " with debitor " + creditCard.getDebitor() 
					+ " of BICs " + (creditCard.getBICLent() / payoffs));

				} else {

					for (double payoff = 1; payoff <= payoffs;) {

						/**
						 * The pay date
						 */
						Date payDate = new Date((long) (refDate.getTime() + (payoff * 86400 * 1000)));

						balanceTransList.add(new Transaction(payDate, chartType, brmPartner.getPartnerId(),
								creditCard.getDebitor(), creditCard.getBICLent() / payoffs));

						logger.debug("Found transaction at date " + payDate.toString() 
						+ " for creditor " + brmPartner.getPartnerId() 
						+ " with debitor " + creditCard.getDebitor() 
						+ " of BICs " + (creditCard.getBICLent() / payoffs));

						payoff += 1;
					}
				}
			}
		}

		// Partners debits
		for (DebitCard debitCard : brmPartner.getLoanList()) {

			if (!debitCard.isPrevious()) {
				/**
				 * The payoff
				 */
				int payoffs = droolsUtils.receivePartnerWithId(debitCard.getCreditor(), 
						pSId.toString(), brmInstanceMap.get(pSId)).getPayOffs();

				if (payoffs == 0) {

					/**
					 * The pay date
					 */
					Date payDate = refDate;

					balanceTransList.add(new Transaction(payDate, chartType, brmPartner.getPartnerId(),
							debitCard.getCreditor(), - debitCard.getBICBorrowed()));

					logger.debug("Found transaction at date " + payDate.toString() + " for creditor "
							+ debitCard.getCreditor() + " with debitor " + brmPartner.getPartnerId() + " of BICs "
							+ (debitCard.getBICBorrowed() / payoffs));

				} else {

					for (double payoff = 1; payoff <= payoffs;) {

						/**
						 * The pay date
						 */
						Date payDate = new Date((long) (refDate.getTime() + (payoff * 86400 * 1000)));

						balanceTransList.add(new Transaction(payDate, chartType, brmPartner.getPartnerId(),
								debitCard.getCreditor(), -debitCard.getBICBorrowed() / payoffs));

						logger.debug("Found transaction at date " + payDate.toString() + " for creditor "
								+ debitCard.getCreditor() + " with debitor " + brmPartner.getPartnerId() + " of BICs "
								+ (debitCard.getBICBorrowed() / payoffs));

						payoff += 1;
					}
				}
			}
		}

		logger.debug("A number of " + balanceTransList.size() + " applicable transactions "
				+ "found for owner " + ownerId);

		return balanceTransList;
	}
	
	/**
	 * Get associated visibility to acquisition station Id and contact counter
	
	 * @param pSId - the Planning Session Id
	 * @param acqStationId - the acquisition station Id
	 * @param contactCounter - the contact counter
	 * @param satid - the satelliteId
	 * @return the associated visibility
	 */
	public static Visibility getVisibility(Long pSId, String acqStationId, Long contactCounter, String satId) {
		
		for(Visibility vis : brmParamsMap.get(pSId).getAllVisibilities()) {
			
			if (vis.getAcqStatId().equals(acqStationId) 
					&& vis.getContactCounter() == contactCounter
					&& vis.getSatelliteId().equals(satId)) {
				
				return vis;
			}
		}
		
		logger.warn("Returned null visibility!");
		
		return null;
	}
	
	/**
	 * Clear BRM session
	 * 
	 * @param pSId - the Planning Session Id
	 */
	public static void clearBRMSession(Long pSId) {
		
		logger.info("Clear all BRM resources relevant to Planning Session: " + pSId);
		
		// Close instances
		if (brmOperMap.get(pSId) != null) {
			brmOperMap.get(pSId).closeAllInstancesForSession(pSId.toString());
		}
	}
}

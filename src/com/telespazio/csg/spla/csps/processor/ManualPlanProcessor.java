/**
 *
 * MODULE FILE NAME: ManualPlanProcessor.java
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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RankPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.splaif.protobuf.Common.PlanningPolicyType;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;

public class ManualPlanProcessor {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(ManualPlanProcessor.class);

	// TODO: instance maps!

	/**
	 * The manual AR iteration map
	 */
	public static Map<Long, Integer> manPlanIterMap;

	/**
	 * The manual schedAR map
	 */
	public static Map<Long, SchedAR> manPlanARMap;

	/**
	 * The next AR DTO List map
	 */
	public static Map<Long, ArrayList<SchedDTO>> manPlanDTOListMap;

	/**
	 * The map of the best ranked solution
	 */
	public static Map<Long, ArrayList<SchedDTO>> bestRankSolMap;

	/**
	 * The domain of previously scheduled AR
	 */
	public static ArrayList<ArrayList<SchedDTO>> workSchedDTODomain;

	/**
	 * The list of previously scheduled AR DTOs
	 */
	public static ArrayList<SchedDTO> workSchedDTOList;
	
	/**
	 * The list of scheduled working AR Ids
	 */
	public static ArrayList<String> workSchedARIdList;

	// /**
	// * The map of manualPlan submission date
	// */
	// public static HashMap<String, Long> manPlanSubDateMap;

	/**
	 * Process the the Delta Plan requests
	 *
	 * // TODO: implement S-Band condition check // TODO: implement inside delta
	 * plan logical check // TODO: set final task statuses // TODO: apply clone
	 * tasks for unchanged Task Ids
	 *
	 * @param pSId
	 * @param manPRList
	 * @param manDTOList
	 * @return
	 */
	public boolean processManualReplan(Long pSId, ArrayList<ProgrammingRequest> manPRList,
			ArrayList<SchedDTO> manDTOList) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();

		FilterDTOHandler filterDTOHandler = new FilterDTOHandler();
		
		/**
		 * The output boolean
		 */
		boolean accepted = true;

		try {
			logger.info("Compute the Manual Planning for Planning Session: " + pSId);

			// 1.0. Handle previously filtered unranked DTOs // TODO: check consistency!
			filterDTOHandler.handlePrevRejRequests(pSId);
			
			// 2.0. Import Manual ARs
			for (ProgrammingRequest manPR : manPRList) {

				if (manPR.getPitchExtraBIC() == null) {
					manPR.setPitchExtraBIC(Double.valueOf(0));
				}

				for (AcquisitionRequest manAR : manPR.getAcquisitionRequestList()) {

					// Process manual AR
					processManualAR(pSId, ObjectMapper.parseDMToSchedAR(pSId, manPR.getUserList().get(0).getUgsId(),
							manPR.getProgrammingRequestId(), manAR, 
							manPR.getUserList().get(0).getAcquisitionStationIdList(),
							manPR.getPitchExtraBIC(), false));

				}
			}

			// 2.1. Get accepted DTOs
			ArrayList<SchedDTO> manPlanSol = rulesPerformer.getAcceptedDTOs(pSId);

//			// 3.0. Update Planning Session statuses for the manual planning solution
			logger.debug("Update the Planning statuses.");
			sessionScheduler.setPlanStatuses(pSId, manPlanSol, manDTOList);


		} catch (Exception e) {

			logger.error("Error processing Manual PRList {} - {}", pSId, e.getMessage(), e);

			accepted = false;

		}

		return accepted;
	}

	/**
	 * Process the manual AR Id
	 * 
	 * @param pSId
	 * @param manPlanAR
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean processManualAR(Long pSId, SchedAR manPlanAR) {
		
		/**
		 * The output result
		 */
		boolean result = false;

		/**
		 * Instance handlers
		 */
		RankPerformer rankPerformer = new RankPerformer();

		logger.info("Process the incoming Manual AR: " + manPlanAR.getARId());

		try
		{

			logger.debug("Processing data: " + manPlanAR.getARId().toString());

			// 1.0 Initialize manualPlan domain
			if (initDomain(pSId, manPlanAR)) {

				// Initialize the best solution of DTO list
				bestRankSolMap.put(pSId, new ArrayList<SchedDTO>());

				// // Set manualAR submit date
				// manPlanSubDateMap.put(manPlanAR.getARId(), new Date().getTime());

				/**
				 * The map of the list of the new scheduled DTO
				 */
				ArrayList<SchedDTO> manDTOList = (ArrayList<SchedDTO>) manPlanARMap.get(pSId).getDtoList()
						.clone();

				// 1.1. Filter new AR DTOs
				for (int i = 0; i < manDTOList.size(); i++) 
				{

					if (manDTOList.get(i).getStatus().equals(DtoStatus.Rejected)) 
					{

						manDTOList.remove(i);

						i--;
					}
				}

				// 2.0. Compute the AR schedulability according to the planning
				// policy
				logger.info("Compute the schedulability status of the AR.");

				if (SessionActivator.planPolicyMap.get(pSId).equals(PlanningPolicyType.Ranked_Based)) {

					// 2.1 Set the working scheduled Tasks
					setWorkSchedTasks(pSId);

					/**
					 * The size of the AR to be newly scheduled
					 */
					double manARProcSize = (PRListProcessor.newARSizeMap.get(pSId) - manPlanIterMap.get(pSId)) + 1;
					
					if (manARProcSize <= 0) {
						
						manARProcSize = 1;
					}
										
					
					// Changed from 29/08/2022 to isolate timeout for Manual Replanning
					/**
					 * The available time for scheduling cutoff (s)
					 */
					double schedTime = Configuration.manSchedTime;					
					
					/**
					 * The expended time
					 */
					long expTime = new Date().getTime() - SessionActivator.planDateMap.get(pSId).getTime();
					
					/**
					 * The AR scheduling cutoff time (ms)
					 */
					double cutoffTime = (schedTime - expTime) / manARProcSize;

					// TODO: Tailor cutoff time with max manualPlan processing time ??


					logger.info("Perform the Ranked-Based scheduling algorithm.");

					// 2.2. Perform revisited Constraint Back-Jumping (CBJ) strategy
					// -> Ranked-Based scheduling algorithm
					bestRankSolMap.put(pSId, rankPerformer.performRevCBJ(pSId, manDTOList, workSchedDTODomain,
							workSchedDTOList, workSchedARIdList, cutoffTime));

					for (SchedDTO dto : bestRankSolMap.get(pSId)) {

						logger.trace(dto.getDTOId() + " is NEO: " + dto.isNEO());
					}

					logger.debug("Final DTO solution size is: " + bestRankSolMap.get(pSId).size());
				}
				
				
//				// 3.0. Update Planning Session statuses
//				sessionScheduler.setPlanStatuses(pSId, (ArrayList<SchedDTO>) bestRankSolMap.get(pSId).clone(),
//						manPlanARMap.get(pSId).getDtoList());

				// // TODO: check for test purposes
				// if (newSchedARSize == 1) {
				//
				// rulesPerformer.printSchedDTOList(pSId);
				// }

				// 3.1. Update ranked AR statuses
				updateRankStatuses(pSId);

//				// 3.2. Update Partners BIC
//				BICCalculator.updatePartnersBICs(pSId);

				// 3.2. Check AR scheduling
				result = RulesPerformer.checkARIdScheduling(pSId, manPlanARMap.get(pSId).getARId());

			} else {

				result = false;
			}

			if (!result) {

				PRListProcessor.schedARIdRankMap.get(pSId).remove(manPlanARMap.get(pSId).getARId());
			}

		} catch (Exception e) {

			logger.error("Error processing session {} - {}", pSId, e.getMessage(), e);

		} finally {

			logger.info("Manual Planning processing ended.");
			System.out.println("");
		}

		return result;
	}

	/**
	 * Initialize DTOs domain
	 *
	 * @param pSId
	 * @param manPlanAR
	 * @return the AR schedulability boolean
	 */
	@SuppressWarnings("unchecked")
	private boolean initDomain(Long pSId, SchedAR manPlanAR) throws Exception {

		/**
		 * The output boolean
		 */
		boolean initStatus = false;

		// Update iteration map
		manPlanIterMap.put(pSId, manPlanIterMap.get(pSId) + 1);
		
		try {

			logger.info("Get the data relevant to each AR.");

			logger.debug("Collect the PRList relevant to the Planning Session " + pSId);

			/**
			 * The PR iterator
			 */	
			Iterator<Entry<String, ProgrammingRequest>> it = PRListProcessor.pRSchedIdMap.get(pSId).entrySet()
					.iterator();

			while (it.hasNext()) {

				/**
				 * The PR entry
				 */	
				Map.Entry<String, ProgrammingRequest> entry = it.next();

				/**
				 * The manual PR
				 */	
				ProgrammingRequest manPR = entry.getValue();

				if (manPR.getPitchExtraBIC() == null) 
				{
					manPR.setPitchExtraBIC(Double.valueOf(0));
				}

				for (AcquisitionRequest manAR : (ArrayList<AcquisitionRequest>) ((ArrayList<AcquisitionRequest>) manPR
						.getAcquisitionRequestList()).clone()) {

					// Initialize manualPlan in the map
					if (manPR.getUserList().get(0).getUgsId().equals(ObjectMapper.getUgsId(manPlanAR.getARId()))
							&& manPR.getProgrammingRequestId().equals(ObjectMapper.getPRId(manPlanAR.getARId()))
							&& manAR.getAcquisititionRequestId().equals(ObjectMapper.getARId(manPlanAR.getARId()))) {

						logger.info("Initialize the incoming Manual AR: " + (ObjectMapper.getARId(manPlanAR.getARId()))
								+ " of PR " + ObjectMapper.getPRId(manPlanAR.getARId()) + " for UGS "
								+ ObjectMapper.getUgsId(manPlanAR.getARId()));
						
						// Initialize the incoming manual AR plan
						initManARPlan(pSId, manPR, manAR);
						
						for (int i = 0; i < manPlanDTOListMap.get(pSId).size(); i++) {

							// initial AR status by default
							manPlanDTOListMap.get(pSId).get(i).setStatus(DtoStatus.Unused);
						}
					}
				}
			}

			if (manPlanARMap.get(pSId).getARId() == null) {

				logger.warn("No AR Id related to the Manual Planning is found!");

			} else if (manPlanARMap.get(pSId).getDtoList().size() < 1) {

				logger.warn("No DTOs related to the Manual Planning are found!");

			} else {

				initStatus = true;

			}

		} catch (Exception ex) {

			logger.error("Exception raised!", ex.getStackTrace()[0].toString());

			ex.printStackTrace();
		}

		return initStatus;
	}
	
	/**
	 * Initialize manual Plan
	 * 
	 * @param pSId
	 * @param manPlanAR
	 * @param orderDTOList
	 * @param manPR
	 * @param manAR
	 * @return
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<SchedDTO> initManARPlan(Long pSId, 
			ProgrammingRequest manPR,  AcquisitionRequest manAR) throws Exception {
		
		/**
		 * Instance handlers
		 */
		ConflictDTOCalculator conflDTOCalculator = new ConflictDTOCalculator();

		
		logger.debug("Initialize Manual Plan data for Planning Session: " + pSId);
		
		// Set relative rank of the AR
		PRListProcessor.schedARIdRankMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
				manPR.getUserList().get(0).getUgsId(), manPR.getProgrammingRequestId(), 
				manAR.getAcquisititionRequestId()), RulesPerformer.getPlannedARIds(pSId).size() + 1);
		
		/**
		 * The scheduled AR
		 */
		SchedAR schedAR = ObjectMapper.parseDMToSchedAR(pSId, manPR.getUserList().get(0).getUgsId(),
				manPR.getProgrammingRequestId(), manAR, 
				manPR.getUserList().get(0).getAcquisitionStationIdList(), 
				manPR.getPitchExtraBIC(), false);

		ArrayList<SchedDTO> orderDTOList = (ArrayList<SchedDTO>) schedAR.getDtoList().clone();

		if (schedAR.getDtoList().size() > 0) {

			for (int i = 0; i < orderDTOList.size(); i++) {

				// Erase DTOs outside Mission Horizon
				if (orderDTOList.get(i).getStartTime()
						.after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime())
						|| orderDTOList.get(i).getStartTime().before(SessionActivator.planSessionMap
								.get(pSId).getMissionHorizonStartTime())) {

					logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " is rejected "
							+ "because outside the relevant Mission Horizon. ");

					orderDTOList.remove(i);

					i--;
				}

				// Erase DTOs filtered by SCM
				else if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId)
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

			// Sort worth DTOs
			conflDTOCalculator.orderWorthDTOs(pSId, orderDTOList);
			
		}

		// Update DTO list
		schedAR.setSchedDTOList((ArrayList<SchedDTO>) orderDTOList.clone());

		manPlanARMap.put(pSId, schedAR);

		manPlanDTOListMap.put(pSId, (ArrayList<SchedDTO>) schedAR.getDtoList().clone());

		return orderDTOList;
	}

	
	/**
	 * Get previously scheduled tasks // TODO: filter previously rejected DTOs
	 *
	 * @param pSId
	 * @throws Exception
	 */
	private void setWorkSchedTasks(Long pSId) throws Exception {

		// 1.0. Initialize working domain
		logger.debug("Initialize working domain.");
		
		workSchedDTODomain = new ArrayList<ArrayList<SchedDTO>>();

		workSchedDTOList = new ArrayList<SchedDTO>();

		workSchedARIdList = new ArrayList<String>(); 
		
		for (int i = 0; i < PersistPerformer.workTaskListMap.get(pSId).size(); i++) {

			/**
			 * The previously scheduled task
			 */
			Task workTask = PersistPerformer.workTaskListMap.get(pSId).get(i);

			if (workTask.getTaskType().equals(TaskType.ACQ)) 
			{				
				// Initialize working DTOs
				initWorkDTOs(pSId, (Acquisition) workTask);
			}
		}
	}
	
	/**
	 * Initialize DTOs in the Planning Session according to the working acquisitions
	 * 
	 * @param pSId
	 * @param workAcq
	 * @param workARIdList
	 * @throws Exception 
	 */
	private boolean initWorkDTOs(Long pSId, Acquisition workAcq) throws Exception {
		
		/**
		 * Instance handlers 
		 */	
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		logger.trace("Initialize DTOs of acquisition: " + workAcq.getTaskId() 
			+ " for the working Planning Session.");

		
		/**
		 * The map of the PRs
		 */
		Iterator<Entry<String, ProgrammingRequest>> it = PRListProcessor.pRSchedIdMap.get(pSId).entrySet()
				.iterator();
		
		while (it.hasNext()) {
			
			/**
			 * The map entry
			 */
			Map.Entry<String, ProgrammingRequest> entry = it.next();
	
			/**
			 * The initial PR
			 */
			ProgrammingRequest initPR = entry.getValue();

			// Add Standard DTOs
			if (initPR.getUserList().get(0).getUgsId().equals(workAcq.getUgsId())
					&& initPR.getProgrammingRequestId().equals(workAcq.getProgrammingRequestId())) {

				for (AcquisitionRequest aR : initPR.getAcquisitionRequestList()) {

					if (aR.getAcquisititionRequestId().equals(workAcq.getAcquisitionRequestId())) {
													
						// 1.0. Filter the rejected DTOs of working HP/PP sessions
						ArrayList<DTO> workDTOList = filterWorkRejDTOs(pSId, aR.getDtoList(), 
								initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
							aR.getAcquisititionRequestId(), initPR.getType(), ObjectMapper.parseDMToSchedDTOId(
									workAcq.getUgsId(), workAcq.getProgrammingRequestId(), 
									workAcq.getAcquisitionRequestId(), workAcq.getDtoId()));

						/**
						 * The scheduling AR Id
						 */
						String schedARId = ObjectMapper.parseDMToSchedARId(
								initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
								aR.getAcquisititionRequestId());
										
						for (DTO dto : workDTOList) {

							if (dto.getDtoId().equals(workAcq.getDtoId())) { 

								// 1.1. Add list of DTOs relevant to working domain
								workSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId,
									initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
									aR.getAcquisititionRequestId(), dto,
									initPR.getUserList().get(0).getAcquisitionStationIdList(), 
									true));
							}
							
							// --------
							// Added on 21/04/2022 for DI2S online management
							if (workAcq.getDi2s() != null) {
								
								// The Equivalent DTO Id
								String equivDTOId = aR.getEquivalentDTO().getEquivalentDtoId();
								
								if (aR.getEquivalentDTO().getEquivalentDtoId() != null) {
	
									// Set default Equivalent DTO Id
									equivDTOId = ObjectMapper.parseDMToEquivDTOId(
											initPR.getUserList().get(0).getUgsId(),
											initPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(),
											dto.getDtoId());
								}
	
								/**
								 * The Equivalent DTO
								 */
								EquivalentDTO equivDTO = new EquivalentDTO(equivDTOId);
								equivDTO.setStartTime(dto.getStartTime());
								equivDTO.setStopTime(dto.getStopTime());
								equivDTO.setTaskList(new ArrayList<>());
								
								// Set working Equivalent DTO (DI2S)
								equivDTOHandler.setWorkDI2SEquivDTO(pSId, equivDTO, PRMode.DI2S,
										initPR.getPitchExtraBIC());
								
								// Add Equivalent Scheduling AR Id
								PRListProcessor.equivIdSchedARIdMap.get(pSId).put(aR.getEquivalentDTO().getEquivalentDtoId(),
										ObjectMapper.parseDMToSchedARId(initPR.getUgsId(), initPR.getProgrammingRequestId(), 
												aR.getAcquisititionRequestId()));
							}
							// --------
							
						}
						
						if (!workSchedARIdList.contains(schedARId)) {
							
							// 1.2. Add DTOs relevant to the working domain
							workSchedDTODomain.add(ObjectMapper.parseDMToSchedDTOList(pSId,
								initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
								aR.getAcquisititionRequestId(), workDTOList,
								initPR.getUserList().get(0).getAcquisitionStationIdList(), 
								false));
							
							logger.trace("Added in domain Acquisition task of working AR: " + schedARId);
						
							// Add working AR Id
							workSchedARIdList.add(schedARId);
							
							// Add relative rank of the scheduling AR
							PRListProcessor.schedARIdRankMap.get(pSId).put(schedARId, 
									workAcq.getWeightedRank().intValue());
						}
					}
				}
			}						
			
//			// Removed on 21/04/2022 for DI2S online management
//			// Add DI2S DTOs	
//			if (workAcq.getDi2s() != null) {
//
//				if (initPR.getUserList().get(0).getUgsId().equals(workAcq.getDi2s().getUgsId())
//					&& initPR.getProgrammingRequestId().equals(workAcq.getDi2s().getProgrammingRequestId())) {
//
//					for (AcquisitionRequest aR : initPR.getAcquisitionRequestList()) {
//
//						/**
//						 * The scheduling AR Id
//						 */
//						String schedARId = ObjectMapper.parseDMToSchedARId(
//								initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
//								aR.getAcquisititionRequestId());
//					
//						if (aR.getAcquisititionRequestId().equals(workAcq.getDi2s().getAcquisitionRequestId())) {
//						
//							ArrayList<DTO> workDTOList = new ArrayList<DTO>();
//						
//							for (DTO dto : aR.getDtoList()) {
//
//								if (dto.getDtoId().equals(workAcq.getDi2s().getDtoId())) { 
//
//									// 1.1. Add list of DTOs relevant to working domain
//									workSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId,
//											initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
//										aR.getAcquisititionRequestId(), dto,
//										initPR.getUserList().get(0).getAcquisitionStationIdList(), 
//										true));
//
//									workDTOList.add(dto);
//									
//									logger.debug("Detected DI2S DTO for AR: " + schedARId);
//								}
//							}
//							
//							if (! workSchedARIdList.contains(schedARId)) {
//								
//								// 1.2. Add DTOs relevant to the working domain
//								workSchedDTODomain.add(ObjectMapper.parseDMToSchedDTOList(pSId,
//									initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
//									aR.getAcquisititionRequestId(), workDTOList,
//									initPR.getUserList().get(0).getAcquisitionStationIdList(), 
//									true));
//															
//								logger.debug("Added in domain DI2S Acquisition task of working AR: " + schedARId);
//								
//								// Add working schedARId for AR
//								workSchedARIdList.add(schedARId);
//							
//								// Add relative rank of the scheduling AR for DI2S 
//								// TODO: check if according to the master DTO
//								PRListProcessor.schedARIdRankMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
//										initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
//										aR.getAcquisititionRequestId()), workAcq.getWeightedRank().intValue());
//							}
//						}
//					}
//				}
//			}


		}
		
		return true;
	}
	
	/**
	 * Filter rejected DTOs in the working Planning Session in case of HP or PP PRType 
	 * for a Routine Planning Session
	 * 
	 * @param pSId
	 * @param workDTOList
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param pRType
	 * @param dtoId
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<DTO> filterWorkRejDTOs(Long pSId, List<DTO> workDTOList, 
			String ugsId, String pRId, String aRId, PRType pRType, String schedDTOId) {
		
		/**
		 * The list of new working DTOs
		 */	
		ArrayList<DTO> newWorkDTOList = (ArrayList<DTO>)((ArrayList<DTO>) workDTOList).clone();
		
		if (SessionChecker.isRoutine(pSId)) {

			for (int i = 0; i < newWorkDTOList.size(); i++) 
			{	
				// Check session type
				if ((pRType.equals(PRType.Hp) || pRType.equals(PRType.PP))) {
					
					if (! ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, newWorkDTOList.get(i).getDtoId())
							.equals(schedDTOId)) 
					{		
						// Remove DTO
						newWorkDTOList.remove(i);
						
						// Reset index
						i --;
					}
					
	//			} else if (RequestChecker.hasEquivDTO(pSId, schedARId)) { // TODO: && is NOT scheduled
	//				
	//				// Remove DTO
	//				newWorkDTOList.remove(i);
	//				
	//				// Reset index
	//				i --;
				}
			}
		}
		
		return newWorkDTOList;
	}	
	
//	/**
//	 * Initialize working Acquisition
//	 * @param pSId
//	 * @param prevTask
//	 * @return
//	 * @throws Exception
//	 */
//	private void initWorkAcquisition(Long pSId, Task prevTask) throws Exception {
//		
//		logger.debug("Initialize previous DTO of task: " + prevTask.getTaskId());
//
//		// 1.0. Check PR scheduling
//		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
//				ObjectMapper.parseDMToSchedPRId(prevTask.getUgsId(), prevTask.getProgrammingRequestId()))
//				&& PRListProcessor.aRSchedIdMap.get(pSId).containsKey(
//						ObjectMapper.parseDMToSchedARId(prevTask.getUgsId(), 
//								prevTask.getProgrammingRequestId(), prevTask.getAcquisitionRequestId()))) {
//		
//			/**
//			 * The Programming Request
//			 */
//			ProgrammingRequest pR = PRListProcessor.pRSchedIdMap.get(pSId)
//					.get(ObjectMapper.parseDMToSchedPRId(
//					prevTask.getUgsId(), prevTask.getProgrammingRequestId()));
//			
//			/**
//			 * The Acquisition Request
//			 */
//			AcquisitionRequest aR = PRListProcessor.aRSchedIdMap.get(pSId)
//					.get(ObjectMapper.parseDMToSchedARId(
//					prevTask.getUgsId(), prevTask.getProgrammingRequestId(), 
//					prevTask.getAcquisitionRequestId()));
//
//			// 1.1. Add list of DTOs relevant to previous domain
//			workSchedDTODomain.add(ObjectMapper.parseDMToSchedDTOList(pSId,
//					prevTask.getUgsId(), prevTask.getProgrammingRequestId(),
//					aR.getAcquisititionRequestId(), aR.getDtoList(),
//					pR.getUserList().get(0).getAcquisitionStationIdList(), 
//					false));
//
//			for (DTO dto : aR.getDtoList()) {
//
//				if (dto.getDtoId().equals(prevTask.getDtoId())) {
//					
//					// 1.2. Add previous scheduled DTO
//					workSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId,
//							prevTask.getUgsId(), prevTask.getProgrammingRequestId(),
//							aR.getAcquisititionRequestId(), dto, 
//							pR.getUserList().get(0).getAcquisitionStationIdList(), 
//							 true));
//
//					logger.debug("Previous acquisition task detected: " + prevTask.getTaskId());
//				}
//			}
//		}
//	}

	/**
	 * Update rank statuses
	 *
	 * @param pSId
	 */
	@SuppressWarnings("unchecked")
	private void updateRankStatuses(Long pSId) throws Exception {

		logger.debug("Update statuses of ranked DTOs.");
		
		for (int n = 0; n < manPlanDTOListMap.get(pSId).size(); n++) {

			/**
			 * The list of PR statuses
			 */
			List<PlanProgrammingRequestStatus> pRStatusList = SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList();

			for (int i = 0; i < pRStatusList.size(); i++) {

				/**
				 * The list of AR statuses
				 */
				List<PlanAcquisitionRequestStatus> aRStatusList = pRStatusList.get(i)
						.getAcquisitionRequestStatusList();

				for (int j = 0; j < aRStatusList.size(); j++) {

					/**
					 * The list of DTO statuses
					 */
					List<PlanDtoStatus> dtoStatusList = aRStatusList.get(j).getDtoStatusList();

					for (int k = 0; k < dtoStatusList.size(); k++) {

						if (dtoStatusList.get(k).getStatus().equals(DtoStatus.Scheduled)
								&& manPlanDTOListMap.get(pSId).get(n).getDTOId()
										.contains(dtoStatusList.get(k).getDtoId() + Configuration.splitChar)
								&& manPlanDTOListMap.get(pSId).get(n).getARId().contains(
										aRStatusList.get(j).getAcquisitionRequestId() + Configuration.splitChar)
								&& manPlanDTOListMap.get(pSId).get(n).getPRId().contains(
										pRStatusList.get(i).getProgrammingRequestId() + Configuration.splitChar)) {

							/**
							 * The new DTO
							 */
							SchedDTO newDTO = manPlanDTOListMap.get(pSId).get(n);
							newDTO.setStatus(DtoStatus.Scheduled);

							manPlanDTOListMap.get(pSId).remove(n);
							manPlanDTOListMap.get(pSId).add(0, newDTO);

							// Update new AR DTO List
							manPlanARMap.get(pSId).getDtoList().clear();
							manPlanARMap.get(pSId).getDtoList()
									.addAll((ArrayList<SchedDTO>) manPlanDTOListMap.get(pSId).clone());
							manPlanARMap.get(pSId).setARStatus(AcquisitionRequestStatus.Scheduled);

							SessionScheduler.schedARListMap.get(pSId).add(manPlanARMap.get(pSId).clone());
						}
					}
				}
			}
		}
	}
}

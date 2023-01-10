/**
 *
 * MODULE FILE NAME: UnrankARListProcessor.java
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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.OptPerformer;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.DTOWorthComparator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;

/**
 * Processor of the unranked ARList class.
 */
public class UnrankARListProcessor {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(UnrankARListProcessor.class);

	/**
	 * The list of unranked schedAR map
	 */
	public static Map<Long, HashMap<String, SchedAR>> unrankSchedARListMap;

	/**
	 * 2.4. Process Optimal Heuristic (OH) strategy
	 * 
	 * -> Perform the Optimization-Based scheduling algorithm
	 *
	 * @param pSId
	 * @param unrankARList
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void processUnrankARs(Long pSId, ArrayList<SchedAR> unrankARList) {

		/**
		 * Instance handlers
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();

		OptPerformer optPerformer = new OptPerformer();

		FilterDTOHandler filterDTOHandler = new FilterDTOHandler();

		ConflictDTOCalculator conflDTOCalculator = new ConflictDTOCalculator();
		
		RulesPerformer rulesPerformer = new RulesPerformer();

		try {

			logger.info("Process the scheduling of a number of " + unrankARList.size() 
			+ " unranked ARs");
			
			/**
			 * The available time for scheduling
			 */
			double availTime = (Configuration.optSchedTime
					- (new Date().getTime() - SessionActivator.planDateMap.get(pSId).getTime()));

			logger.info("Perform Optimization-based scheduling algorithm.");

			/**
			 * The optimal scheduling solution
			 */
			ArrayList<SchedDTO> optSchedSol = optPerformer.performOHScheduling(
					pSId, unrankARList, availTime);

			optSchedSol.addAll(getRankSol(pSId));

			if (!unrankARList.isEmpty()) {

				// Handle previously filtered unranked DTOs
				filterDTOHandler.handlePrevRejRequests(pSId);
				
				/**
				 * The new AR DTO List
				 */
				ArrayList<SchedDTO> unrankDTOList = new ArrayList<>();

				for (SchedAR unrankAR : unrankARList) {

					// Fill the unranked list map
					unrankSchedARListMap.get(pSId).put(unrankAR.getARId(), unrankAR);

					ArrayList<SchedDTO> orderDTOList = (ArrayList<SchedDTO>) unrankAR.getDtoList().clone();

					if (orderDTOList.size() > 0) {
					
						for (int i = 0; i < orderDTOList.size(); i ++) {
							
							// Erase DTOs outside Mission Horizon
							if (! RequestChecker.isInsideMH(pSId, orderDTOList.get(i))) {
	
								logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " is excluded "
										+ "because outside the relevant Mission Horizon. ");
	
								orderDTOList.remove(i);
	
								i--;
	
							// Erase DTOs filtered by SCM (in Ranked Routine Session)
							} else if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId)
									.contains(orderDTOList.get(i).getDTOId())) {
	
								logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " is excluded "
										+ "due to filtering.");
	
								orderDTOList.remove(i);
	
								i --;
							}
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

					unrankDTOList.addAll((ArrayList<SchedDTO>) orderDTOList.clone());

					// Sort unranked DTOs
					if (unrankDTOList.size() > 0) {

						Collections.sort(unrankDTOList, new DTOWorthComparator());
					}
				}

				// 2.5. Add previous ranked DTOs to the unranked list
				logger.info("Add previous ranked DTOs to the unranked list.");
				unrankDTOList.addAll(getRankSol(pSId));
				
				// 2.6. Update the optimal scheduling solution 
				optSchedSol = rulesPerformer.getAcceptedDTOs(pSId);

				// 3.0. Update the planning statuses for the ranked & unranked solutions
				sessionScheduler.setPlanStatuses(pSId, optSchedSol, unrankDTOList);

				// 3.1. Update lists of unranked ARs
				logger.debug("Update lists of unranked ARs.");
				for (SchedAR unrankAR : unrankARList) {

					PRListProcessor.schedARIdRankMap.get(pSId).put(unrankAR.getARId(),
							RulesPerformer.getPlannedARIds(pSId).size() + 1);

					// 3.2. Update scheduled AR statuses
					updateSchedARStatus(pSId, unrankAR);
				}
				
			} else {

				logger.info("No unranked requests to be scheduled for Planning Session: " + pSId);
			}

//			// 4.0. Finalize schedule
//			sessionScheduler.finalizeSchedule(pSId);

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

	}
	
	/**
	 * Update scheduled AR
	 *
	 * @param pSId
	 * @param schedAR
	 */
	private void updateSchedARStatus(Long pSId, SchedAR schedAR) {

		for (int n = 0; n < schedAR.getDtoList().size(); n++) {

			/**
			 * The list of PR status
			 */
			List<PlanProgrammingRequestStatus> pRStatusList = SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList();

			for (int i = 0; i < pRStatusList.size(); i++) {

				/**
				 * The list of AR status
				 */
				List<PlanAcquisitionRequestStatus> aRStatusList = pRStatusList.get(i)
						.getAcquisitionRequestStatusList();

				for (int j = 0; j < aRStatusList.size(); j++) {

					/**
					 * The list of DTO status
					 */
					List<PlanDtoStatus> dtoStatusList = aRStatusList.get(j).getDtoStatusList();

					for (int k = 0; k < dtoStatusList.size(); k++) {

						if (dtoStatusList.get(k).getStatus().equals(DtoStatus.Scheduled)
								&& schedAR.getDtoList().get(n).getDTOId()
										.contains(dtoStatusList.get(k).getDtoId() + Configuration.splitChar)
								&& schedAR.getDtoList().get(n).getARId().contains(
										aRStatusList.get(j).getAcquisitionRequestId() + Configuration.splitChar)
								&& schedAR.getDtoList().get(n).getPRId().contains(
										pRStatusList.get(i).getProgrammingRequestId() + Configuration.splitChar)) {

							SchedDTO newDTO = schedAR.getDtoList().get(n);
							newDTO.setStatus(DtoStatus.Scheduled);

							schedAR.setARStatus(AcquisitionRequestStatus.Scheduled);
							SessionScheduler.schedARListMap.get(pSId).add(schedAR);

						}
					}
				}
			}
		}
	}

	/**
	 * Get the ranked solution
	 *
	 * @param pSId
	 * @return
	 * @throws Exception
	 */
	private ArrayList<SchedDTO> getRankSol(Long pSId) throws Exception {

		logger.debug("Get the ranked solution for Planning Session: " + pSId);
		
		/**
		 * The previous scheduled list of DTOs
		 */
		ArrayList<SchedDTO> workSchedDTOList = new ArrayList<>();

		for (Task workTask : PersistPerformer.workTaskListMap.get(pSId)) {

			// Check ACQ Task
			if (workTask.getTaskType().equals(TaskType.ACQ)) {

				// Check ranked solution
				for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {

					for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

						if (workTask.getProgrammingRequestListId().equals(pR.getProgrammingRequestListId())
								&& workTask.getProgrammingRequestId().equals(pR.getProgrammingRequestId())
								&& workTask.getAcquisitionRequestId().equals(aR.getAcquisititionRequestId())) {

							for (DTO dto : aR.getDtoList()) {

								try {

									if (workTask.getDtoId().equals(dto.getDtoId())) {
										
										// Add working scheduling DTO
										workSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId,
												pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(),
												aR.getAcquisititionRequestId(), dto, 
												pR.getUserList().get(0).getAcquisitionStationIdList(), 
												false));
										
										break;
									}
								
								} catch (Exception ex) {
									
									logger.error("Exception raised: " + ex.getMessage());
								}
							}
						}
					}
				}
			}
		}

		return workSchedDTOList;
	}

}

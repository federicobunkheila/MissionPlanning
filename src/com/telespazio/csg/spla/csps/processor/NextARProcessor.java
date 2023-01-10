/**
*
* MODULE FILE NAME: NextARProcessor.java
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RankPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.utils.ConflictDTOCalculator;
import com.telespazio.csg.spla.csps.utils.IntMatrixCalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.splaif.protobuf.Common.PlanningPolicyType;
import com.telespazio.splaif.protobuf.NextARMessage.NextAR;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.UserInfo;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;

/**
 * The NextAR processor class
 */
public class NextARProcessor {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(NextARProcessor.class);

	/**
	 * The next AR iteration map
	 */
	public static Map<Long, Integer> nextARIterMap;

	/**
	 * The next schedAR map
	 */
	public static Map<Long, SchedAR> nextSchedARMap;

	/**
	 * The next AR DTO List map
	 */
	public static Map<Long, ArrayList<SchedDTO>> nextSchedDTOListMap;

	/**
	 * The map of the best ranked solution
	 */
	public static Map<Long, ArrayList<SchedDTO>> bestRankSolMap;

	/**
	 * The domain of AR DTO in the working Planning Session
	 */
	public static ArrayList<ArrayList<SchedDTO>> workSchedDTODomain;

	/**
	 * The working solution DTOs
	 */
	public static ArrayList<SchedDTO> workSchedSol;

	/**
	 * The list of scheduled working AR Ids
	 */
	public static ArrayList<String> workSchedARIdList;
	
	/**
	 * The map of NextAR submission date
	 */
	public static HashMap<String, Long> nextARSubDateMap;

	/**
	 * Process the NextAR
	 *
	 * @param nextAR
	 *            - the nextAR to be imported
	 * @param isInternal
	 * 			  - the internal flag
	 * @return the NextAR status
	 */
	@SuppressWarnings("unchecked")
	public boolean processNextAR(NextAR nextAR, boolean isInternal) {

		/**
		 * The output result
		 */
		boolean result = false;
		
		/**
		 * Instance handlers
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();

		RankPerformer rankPerformer = new RankPerformer();
		
		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The Planning Session Id
		 */
		Long pSId = nextAR.getPlanningSessionId();

		try {
				
//				logger.debug(PRListProcessor.pRSchedIdMap.get(pSId).keySet().toString());
			
			if (!PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
						nextAR.getUgsId(), nextAR.getProgRedId()))) {
				
				logger.warn("No plannable PR found with PRId " + ObjectMapper.parseDMToSchedPRId(
						nextAR.getUgsId(), nextAR.getProgRedId()) + " for Planning Session: " + pSId);
				
				return result;						
			}

			if (!isInternal && RequestChecker.isCrisis(
					PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.parseDMToSchedPRId(
					nextAR.getUgsId(), nextAR.getProgRedId())).getType())) {
			
				logger.warn("The PR: " + ObjectMapper.parseDMToSchedPRId(
						nextAR.getUgsId(), nextAR.getProgRedId())
					+ " for UGS: " + PRListProcessor.pRSchedIdMap.get(pSId).get(
							ObjectMapper.parseDMToSchedPRId(nextAR.getUgsId(), nextAR.getProgRedId()))
					.getUserList().get(0).getUgsId() + " has been just processed as a CRISIS PRType.");
				
				// Check Crisis AR scheduling result
				result = checkCrisisNextAR(nextAR);
				
				return result;
				
			} else {
			
				logger.info("Process the incoming NextAR: " + nextAR.toString());
			}

			// 1.0 Initialize NextAR domain
			if (initDomain(nextAR)) {

				// Initialize the best solution of DTO list
				bestRankSolMap.put(pSId, new ArrayList<SchedDTO>());

				// Set NextAR submit date
				nextARSubDateMap.put(ObjectMapper.parseDMToSchedARId(
						nextAR.getUgsId(), nextAR.getProgRedId(), nextAR.getArId()), new Date().getTime());
								
				/**
				 * The map of the list of the new scheduled DTO
				 */
				ArrayList<SchedDTO> nextARDTOList = (ArrayList<SchedDTO>) nextSchedARMap.get(pSId).getDtoList()
						.clone();

				logger.debug("The NextAR is composed of a number of DTOs: " + nextARDTOList.size() );
				
				// 1.1. Filter new AR DTOs
				for (int i = 0; i < nextARDTOList.size(); i++) {

					if (nextARDTOList.get(i).getStatus().equals(DtoStatus.Rejected)) {

						logger.debug("Remove REJECTED DTO: " + nextARDTOList.get(i).getDTOId());
						nextARDTOList.remove(i);

						i--;
					}
				}

				// 2.0. Compute the AR schedulability according to the planning policy
				logger.info("Compute the schedulability status of the AR: " + nextSchedARMap.get(pSId).getARId());
				
				/**
				 * The size of the ARs to be newly processed
				 */
				double newARProcSize = (PRListProcessor.newARSizeMap.get(pSId) - nextARIterMap.get(pSId)) + 1;

				if (newARProcSize <= 0) {
					
					logger.warn("Invalid AR processing size is found.");
					
					newARProcSize = 1;
				}
				
				logger.debug("A number of " + newARProcSize + " AR remaining to be processed.");
				
				if (SessionActivator.planPolicyMap.get(pSId).equals(PlanningPolicyType.Ranked_Based)) {

					// 2.1 Set the existing working scheduled Tasks
					setWorkSchedTasks(pSId);

					/**
					 * The expended time
					 */
					long expTime = new Date().getTime() - SessionActivator.planDateMap.get(pSId).getTime();
					
					/**
					 * The available time for scheduling cutoff (s)
					 */
					double schedTime = 0;
					
					if (SessionChecker.isRankedRoutine(pSId)) {
						
						schedTime = Configuration.routSchedTime;
					
					} else {
						
						schedTime = Configuration.premSchedTime;
					}
					
					/**
					 * The AR scheduling cutoff time (ms)
					 */
					double cutOffTime = (schedTime - expTime) / newARProcSize;

					// Tailor cutoff time with max NextAR processing time
					if (cutOffTime > (nextAR.getMaxProcessingTime() - Configuration.cutOffDelay)) {

						cutOffTime = nextAR.getMaxProcessingTime() - Configuration.cutOffDelay;
					
					// Hp: according to the SPLA cut-off, no available time remains
					} else if (cutOffTime < 0) {
						
						cutOffTime = 0.0;
					}

					// 2.2. Process Constraint Back-Jumping (CBJ) strategy
					// -> Perform the Ranked-Based scheduling algorithm
					logger.info("Perform the Ranked-Based scheduling algorithm.");

					if (! nextARDTOList.isEmpty()) {
					
						// Perform revisited CBJ version
						bestRankSolMap.put(pSId, rankPerformer.performRevCBJ(pSId, nextARDTOList, 
								workSchedDTODomain, workSchedSol, workSchedARIdList, cutOffTime));
					} else {
						
						bestRankSolMap.put(pSId, (ArrayList<SchedDTO>) rulesPerformer.getAcceptedDTOs(pSId).clone());
					}

					logger.info("Final DTO solution size is: " + bestRankSolMap.get(pSId).size());
				}

				// 3.0. Update the PR planning statuses
				sessionScheduler.setPlanStatuses(pSId, (ArrayList<SchedDTO>) bestRankSolMap.get(pSId).clone(),
						nextSchedARMap.get(pSId).getDtoList());

				// 3.1. Update ranked AR statuses
				updateRankStatuses(pSId);

				// 3.2. Check AR scheduling
				result = RulesPerformer.checkARIdScheduling(pSId, nextSchedARMap.get(pSId).getARId());
				
//				// 3.3. Update Partners BIC
//				BICCalculator.updatePartnersBICs(pSId);

			} else {

				result = false;
			}

			if (!result) {

				PRListProcessor.schedARIdRankMap.get(pSId).remove(nextSchedARMap.get(pSId).getARId());
			}

		} catch (Exception ex) {

			logger.error("Error processing session {} - {}", pSId, ex.getStackTrace()[0].toString());

		} finally {

			logger.info("NextAR processing ended.");
			System.out.println("");
		}

		return result;
	}

	/**
	 * Initialize DTOs domain
	 *
	 * @param nextAR
	 * @return the AR schedulability boolean
	 */
	@SuppressWarnings("unchecked")
	private boolean initDomain(NextAR nextAR) throws Exception {

		/**
		 * The output boolean
		 */
		boolean initStatus = false;

		/**
		 * The Planning Session Id
		 */
		Long pSId = nextAR.getPlanningSessionId();

		// Update iteration map
		nextARIterMap.put(pSId, nextARIterMap.get(pSId) + 1);
		
		try {
						
			logger.debug("Collect the data relevant to the NextAR from the PRLists "
					+ "of Planning Session: " + pSId);

			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(ObjectMapper.parseDMToSchedPRId(
					nextAR.getUgsId(), nextAR.getProgRedId()))) {
				
				/**
				 * The initial PR
				 */
				ProgrammingRequest initPR = PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.parseDMToSchedPRId(
						nextAR.getUgsId(), nextAR.getProgRedId()));
				
				for (AcquisitionRequest initAR : (ArrayList<AcquisitionRequest>) ((ArrayList<AcquisitionRequest>) initPR
						.getAcquisitionRequestList()).clone()) {

					// Initialize nextAR in the map
					if (initPR.getProgrammingRequestId().equals(nextAR.getProgRedId())
							&& initAR.getAcquisititionRequestId().equals(nextAR.getArId())) {

						logger.info("Initialize the incoming NextAR: " + nextAR.getArId()
								+ " of PR " + nextAR.getProgRedId() + " for UGS " + nextAR.getUgsId());
						
						// Initialize the incoming Next AR plan
						initNextARPlan(pSId, initPR, initAR);
						
						break;

					}
				}
			}

			if (nextSchedARMap.get(pSId).getARId() == null) {

				logger.warn("No AR Id related to the NextAR is found!");

			} else if (nextSchedARMap.get(pSId).getDtoList().isEmpty()) {

				logger.warn("No DTOs related to the NextAR are found!");

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
	 * Initialize Next AR plan
	 * @param pSId
	 * @param initPR
	 * @param initAR
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void initNextARPlan(Long pSId, ProgrammingRequest initPR, AcquisitionRequest initAR) throws Exception {
					
		/**
		 * Instance handlers
		 */
		ConflictDTOCalculator conflDTOCalculator = new ConflictDTOCalculator();
		
		IntMatrixCalculator intMatrixCalculator = new IntMatrixCalculator();
		
		logger.debug("Initialize Next AR Plan data for Planning Session: " + pSId);

		// Set relative rank of the AR
		PRListProcessor.schedARIdRankMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
				initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
				initAR.getAcquisititionRequestId()), RulesPerformer.getPlannedARIds(pSId).size() + 1);
		
		/**
		 * The scheduled AR
		 */
		SchedAR schedAR = ObjectMapper.parseDMToSchedAR(pSId, initPR.getUserList().get(0).getUgsId(),
				initPR.getProgrammingRequestId(), initAR, initPR.getUserList().get(0).getAcquisitionStationIdList(), 
				initPR.getPitchExtraBIC(), false);
		
		logger.debug("Internal scheduling AR found: " + schedAR.getARId());
		
		/**
		 * The ordered DTO list
		 */
		ArrayList<SchedDTO> orderDTOList = (ArrayList<SchedDTO>) schedAR.getDtoList().clone();

		// Build Intersection Matrix
		intMatrixCalculator.buildDTOIntMatrix(pSId, orderDTOList);
		
		if (orderDTOList.size() > 0) {
		
			logger.debug("Process relevant DTO list.");

			for (int i = 0; i < orderDTOList.size(); i++) {
	
				// Erase DTOs outside Mission Horizon
				if (! RequestChecker.isInsideMH(pSId, orderDTOList.get(i))) {
	
					logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " from: " +
						orderDTOList.get(i).getStartTime() + " to: " + orderDTOList.get(i).getStopTime()
						+ " is rejected because outside the relevant Mission Horizon.");
	
					orderDTOList.remove(i);
	
					i--;
					
				// Erase owners DTOs filtered by SCM
				} else if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId)
						.contains(orderDTOList.get(i).getDTOId())) {
	
					logger.info("The DTO " + orderDTOList.get(i).getDTOId() + " is excluded "
							+ "due to filtering.");
	
					orderDTOList.remove(i);
	
					i--;					
				}
			}
	
			// Erase subscribers DTOs filtered by SCM
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

		nextSchedARMap.put(pSId, schedAR);

		nextSchedDTOListMap.put(pSId, (ArrayList<SchedDTO>) orderDTOList.clone());

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

		workSchedSol = new ArrayList<SchedDTO>();

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
		IntMatrixCalculator intMatrixCalculator = new IntMatrixCalculator();
		
		logger.trace("Initialize DTOs of acquisition: " + workAcq.getTaskId() 
			+ " for the working Planning Session.");

		/**
		 * The list of user info list
		 */
		ArrayList<ArrayList<UserInfo>> userInfoListList = new ArrayList<ArrayList<UserInfo>>();
		
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

			// 1.0. Add Standard DTOs
			if (initPR.getUserList().get(0).getUgsId().equals(workAcq.getUgsId())
					&& initPR.getProgrammingRequestId().equals(workAcq.getProgrammingRequestId())) {

				for (AcquisitionRequest aR : initPR.getAcquisitionRequestList()) {

					if (aR.getAcquisititionRequestId().equals(workAcq.getAcquisitionRequestId())) {
													
						// 1.1. Filter the rejected DTOs of working HP/PP sessions
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
												
						if (!workSchedARIdList.contains(schedARId)) {								
							
							for (DTO dto : workDTOList) {
	
								if (dto.getDtoId().equals(workAcq.getDtoId())) { 
	
									// 1.1. Add list of DTOs relevant to working domain
									workSchedSol.add(ObjectMapper.parseDMToSchedDTO(pSId,
										initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
										aR.getAcquisititionRequestId(), dto,
										initPR.getUserList().get(0).getAcquisitionStationIdList(), 
										true));
								}
							}
							
							/**
							 * The list of working DTOs
							 */
							ArrayList<SchedDTO> workSchedDTOList = ObjectMapper.parseDMToSchedDTOList(pSId,
									initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
									aR.getAcquisititionRequestId(), workDTOList,
									initPR.getUserList().get(0).getAcquisitionStationIdList(), 
									false);
							
							// 1.2. Build Intersection Matrix
							intMatrixCalculator.buildDTOIntMatrix(pSId, workSchedDTOList);
							
							// 1.3. Filter unconsistent DTOs
							workSchedDTOList = filterUnconsistentDTOs(pSId, workSchedDTOList);
							
							// 1.4. Add working data
							workSchedDTODomain.add(workSchedDTOList);
							
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
				
			// 2.0. Add DI2S DTOs	
			if (workAcq.getDi2s() != null) {
								
				if (initPR.getUserList().get(0).getUgsId().equals(workAcq.getDi2s().getUgsId())
					&& initPR.getProgrammingRequestId().equals(workAcq.getDi2s().getProgrammingRequestId())) {

					for (AcquisitionRequest aR : initPR.getAcquisitionRequestList()) {

						/**
						 * The scheduling AR Id
						 */
						String schedARId = ObjectMapper.parseDMToSchedARId(
								initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
								aR.getAcquisititionRequestId());
					
						if (aR.getAcquisititionRequestId().equals(workAcq.getDi2s().getAcquisitionRequestId())) {
													
							if (! workSchedARIdList.contains(schedARId)) {
							
								ArrayList<DTO> workDTOList = new ArrayList<DTO>();
							
								for (DTO dto : aR.getDtoList()) {
	
									if (dto.getDtoId().equals(workAcq.getDi2s().getDtoId())) { 
	
										// 2.1. Add list of DTOs relevant to working domain
										workSchedSol.add(ObjectMapper.parseDMToSchedDTO(pSId,
												initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
											aR.getAcquisititionRequestId(), dto,
											initPR.getUserList().get(0).getAcquisitionStationIdList(), 
											true));
	
										workDTOList.add(dto);
										
										logger.debug("Detected DI2S DTO for AR: " + schedARId);
									}
								}
								
								/**
								 * The list of consistent DTOs
								 */
								ArrayList<SchedDTO> workSchedDTOList = ObjectMapper.parseDMToSchedDTOList(pSId,
										initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(),
										aR.getAcquisititionRequestId(), workDTOList,
										initPR.getUserList().get(0).getAcquisitionStationIdList(), 
										false);
								
								// 1.2. Build Intersection Matrix
								intMatrixCalculator.buildDTOIntMatrix(pSId, workSchedDTOList);
								
								// 1.3. Filter unconsistent DTOs
								workSchedDTOList = filterUnconsistentDTOs(pSId, workSchedDTOList);
								
								// 1.4. Add working data
								workSchedDTODomain.add(workSchedDTOList);
															
								logger.trace("Added in domain DI2S Acquisition task of working AR: " + schedARId);
								
								// Add working schedARId for AR
								workSchedARIdList.add(schedARId);
							
								// Add relative rank of the scheduling AR for DI2S 
								PRListProcessor.schedARIdRankMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
										initPR.getUserList().get(0).getUgsId(), initPR.getProgrammingRequestId(), 
										aR.getAcquisititionRequestId()), workAcq.getWeightedRank().intValue());
							}
						}
					}
				}
			}
			
			userInfoListList.clear();
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

	/**
	 * Filter not-scheduled DTOs of a given AR not arc-consistent with respect to previous
	 * scheduled DTOs
	 * 
	 * @param pSId - the Planning Session Id
	 * @param schedDTOList - the list of scheduling DTOs
	 */
	private ArrayList<SchedDTO> filterUnconsistentDTOs(Long pSId, ArrayList<SchedDTO> schedDTOList) {

		// Check arc consistency
		for (int i = 0; i < schedDTOList.size(); i ++) {
			
			/**
			 * The scheduling DTO
			 */
			SchedDTO schedDTO = schedDTOList.get(i);
			
			if (!schedDTO.getStatus().equals(DtoStatus.Scheduled)) {
							
				for (int j = i; j < schedDTOList.size(); j ++) {
			
					if (i != j) {
					
						/**
						 * The conflicting DTO
						 */
						SchedDTO conflDTO = schedDTOList.get(j);	
						
						if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).containsKey(schedDTO.getDTOId())
								&& IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(schedDTO.getDTOId())
									.containsKey(conflDTO.getDTOId())) {
			
							if (IntMatrixCalculator.intDTOMatrixMap.get(pSId).get(schedDTO.getDTOId())
									.get(conflDTO.getDTOId()) == 0) {
			
								// Remove scheduling DTO
								schedDTOList.remove(j);
								
								j --;
							}
						}
					}
				}
			}
		}
		
		return schedDTOList;
	}
	
	/**
	 * Update rank statuses
	 *
	 * @param pSId
	 */
	@SuppressWarnings("unchecked")
	private void updateRankStatuses(Long pSId) throws Exception {

		logger.debug("Update lists of statuses of the ranked ARs.");
		
		for (int n = 0; n < nextSchedDTOListMap.get(pSId).size(); n++) {

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
								&& nextSchedDTOListMap.get(pSId).get(n).getDTOId()
										.contains(dtoStatusList.get(k).getDtoId() + Configuration.splitChar)
								&& nextSchedDTOListMap.get(pSId).get(n).getARId().contains(
										aRStatusList.get(j).getAcquisitionRequestId() + Configuration.splitChar)
								&& nextSchedDTOListMap.get(pSId).get(n).getPRId().contains(
										pRStatusList.get(i).getProgrammingRequestId() + Configuration.splitChar)) {

							/**
							 * The new DTO
							 */
							SchedDTO newDTO = nextSchedDTOListMap.get(pSId).get(n);
							newDTO.setStatus(DtoStatus.Scheduled);

							nextSchedDTOListMap.get(pSId).remove(n);
							nextSchedDTOListMap.get(pSId).add(0, newDTO);

							// Update new AR DTO List
							nextSchedARMap.get(pSId).getDtoList().clear();
							nextSchedARMap.get(pSId).getDtoList()
									.addAll((ArrayList<SchedDTO>) nextSchedDTOListMap.get(pSId).clone());
							nextSchedARMap.get(pSId).setARStatus(AcquisitionRequestStatus.Scheduled);

							SessionScheduler.schedARListMap.get(pSId).add(nextSchedARMap.get(pSId).clone());
						}
					}
				}
			}
		}
	}
	
	/**
	 * Check the Crisis scheduling result
	 * @param nextAR
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean checkCrisisNextAR(NextAR nextAR) {
		
		/**
		 * The output result
		 */
		boolean result = false;
		
		/**
		 * The Planning Session Id
		 */
		Long pSId = nextAR.getPlanningSessionId();
		
		try {
			
			logger.debug("Retrieve the data relevant to the CRISIS NextAR from the PRLists "
					+ "of Planning Session: " + pSId);

			/**
			 * The scheduling AR
			 */
			SchedAR schedAR = null;
			
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
					
				if (RequestChecker.isCrisis(initPR.getType())) {

					logger.debug("The PR: " + initPR.getProgrammingRequestId()  
						+ " for UGS: " + initPR.getUserList().get(0).getUgsId() +
						" is of CRISIS Type.");
				}
				
				for (AcquisitionRequest initAR : (ArrayList<AcquisitionRequest>) ((ArrayList<AcquisitionRequest>) initPR
						.getAcquisitionRequestList()).clone()) {

					// Initialize nextAR in the map
					if (initPR.getProgrammingRequestId().equals(nextAR.getProgRedId())
							&& initAR.getAcquisititionRequestId().equals(nextAR.getArId())) {
						
						logger.trace("CRISIS request found.");
						
						// Get scheduling AR 
						schedAR = ObjectMapper.parseDMToSchedAR(pSId, initPR.getUserList().get(0).getUgsId(),
										initPR.getProgrammingRequestId(), initAR,
										initPR.getUserList().get(0).getAcquisitionStationIdList(), 
										initPR.getPitchExtraBIC(), false);
						
						break;
					}
				}
			}

			if (schedAR != null) {	
			
				// Check the AR scheduling 
				result = RulesPerformer.checkARIdScheduling(pSId, schedAR.getARId());
			
			}
	
		} catch (Exception ex) {
	
			logger.error("Error processing session {} - {}", pSId, ex.getStackTrace()[0].toString());
		}
		
		return result;
		
	}
}
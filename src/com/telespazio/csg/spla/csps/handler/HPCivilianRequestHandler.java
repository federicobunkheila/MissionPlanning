///**
// *
// * MODULE FILE NAME: CivilRequestHandler.java
// *
// * MODULE TYPE:      <Class definition>
// *
// * FUNCTION:         <Functional description of the DDC>
// *
// * PURPOSE:          <List of SR>
// *
// * CREATION DATE:    <01-Jan-2017>
// *
// * AUTHORS:          bunkheila Bunkheila
// *
// * DESIGN ISSUE:     1.0
// *
// * INTERFACES:       <prototype and list of input/output parameters>
// *
// * SUBORDINATES:     <list of functions called by this DDC>
// *
// * MODIFICATION HISTORY:
// *
// *             Date          |  Name      |   New ver.     | Description
// * --------------------------+------------+----------------+-------------------------------
// * <DD-MMM-YYYY>             | <name>     |<Ver>.<Rel>     | <reasons of changes>
// * --------------------------+------------+----------------+-------------------------------
// *
// * PROCESSING
// */
//package com.telespazio.csg.spla.csps.handler;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map.Entry;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
//import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
//import com.telespazio.csg.spla.csps.performer.RulesPerformer;
//import com.telespazio.csg.spla.csps.processor.PRListProcessor;
//import com.telespazio.csg.spla.csps.processor.SessionScheduler;
//import com.telespazio.csg.spla.csps.utils.ObjectMapper;
//
//import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
//import it.sistematica.spla.datamodel.core.model.DTO;
//import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
//
///**
// * The handler of the Civilian HP Requests
// * @author bunkheila
// *
// */
//public class HPCivilianRequestHandler {
//
//	/**
//	 * The proper logger
//	 */
//	protected static Logger logger = LoggerFactory.getLogger(HPCivilianRequestHandler.class);
//
//	/**
//	 * The HP-Civilian DTO Ids map
//	 */
//	public static HashMap<Long, ArrayList<String>> hpCivilDTOIdListMap;
//	
//	/**
//	 * The HP-Civilian Unique Ids map
//	 */
//	public static HashMap<Long, ArrayList<String>> hpCivilUniqueIdListMap;
//
//	
//	/**
//	 * Handle HP-Civilian requests
//	 * 
//	 * @param pSId
//	 */
//	public void handleHpCivilRequests(Long pSId) {
//
//		// Find replacing PRs of HP-Civilian/Crisis
//		logger.debug("Find HP-Civilian/Crisis requests to be replaced.");
//		findReplacedPRs(pSId);
//
//		// Schedule replacing PRs
//		logger.debug("Schedule replacing PRs.");
//		scheduleReplacingPRs(pSId);
//
//	}
//
//	/**
//	 * Find replacing PRs (Civilian / Crisis)
//	 * 
//	 * @param pSId
//	 * @return
//	 */
//	private void findReplacedPRs(Long pSId) {
//
//		/**
//		 * Instance handlers
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
//
//		SessionScheduler sessionScheduler = new SessionScheduler();
//		
//		try {
//			
//			/**
//			 * The cancelled list of DTOs
//			 */
//			ArrayList<SchedDTO> cancDTOList = new ArrayList<SchedDTO>();
//			
//			for (ProgrammingRequest replPR : PRListProcessor.replPRListMap.get(pSId)) {
//
//				logger.debug("Search replacing PR: " + replPR.getProgrammingRequestId() 
//				+ " with unique Id: " + replPR.getCivilianUniqueId());
//				
//				/**
//				 * The map of the PR scheduling Ids
//				 */
//				Iterator<Entry<String, ProgrammingRequest>> it = PRListProcessor.pRSchedIdMap.get(pSId).entrySet().iterator();
//
//				while (it.hasNext()) {
// 
//					/**
//					 * The Programming Request
//					 */
//					ProgrammingRequest pR = it.next().getValue();
//			
//					if (pR.getCivilianUniqueId() != null && pR.getCivilianUniqueId().equals(replPR.getCivilianUniqueId())
//							&& ! (ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId())
//							.equals(ObjectMapper.parseDMToSchedPRId(replPR.getUserList().get(0).getUgsId(), 
//									replPR.getProgrammingRequestId())))) {
//
//						logger.trace("Match HP-Civilian PR: " + pR.getProgrammingRequestId() 
//						+ " with unique Id: " + pR.getCivilianUniqueId());
//						
//						// Update HP-Civilian requests statuses
//						logger.info("Unique PR: " + ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(),
//									pR.getProgrammingRequestId()) + " has to be replaced by civilian PR.");
//
//						for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {
//		
//							// Retract DTO List
//							rulesPerformer.retractDTOList(pSId, ObjectMapper.parseDMToSchedDTOList(pSId,
//									pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
//									aR.getAcquisititionRequestId(), aR.getDtoList(), 
//									pR.getUserList().get(0).getAcquisitionStationIdList(), false), 
//									ReasonOfReject.systemConflict);
//							
//							for (DTO dto : aR.getDtoList()) {
//
//								/**
//								 * The scheduled DTO
//								 */
//								SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId, 
//										pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
//										aR.getAcquisititionRequestId(), dto,
//										pR.getUserList().get(0).getAcquisitionStationIdList(), false);
//
//								cancDTOList.add(schedDTO);
//						
//								if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTO.getDTOId())) {
//								
//									hpCivilDTOIdListMap.get(pSId).add(schedDTO.getDTOId());
//									hpCivilUniqueIdListMap.get(pSId).add(pR.getCivilianUniqueId());
//								}	
//							}
//						}						
//					}
//				}
//			}
//			
//			logger.info("A number of " + cancDTOList.size() + " PRs to be replaced is found.");
//			
//			// 2.0. Get the accepted DTOs
//			ArrayList<SchedDTO> schedSol = rulesPerformer.getAcceptedDTOs(pSId);
//
//			// 3.0. Update the Planning Session statuses
//			logger.info("Update the Planning Session statuses.");
//			sessionScheduler.setPlanStatuses(pSId, schedSol, cancDTOList);
//			
//		} catch (Exception ex) {
//
//			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
//		}
//	}
//	
//
//	/**
//	 * Schedule the replaced PRs (PP-Civilian)
//	 * 
//	 * // TODO: check break
//	 * 
//	 * @param pSId
//	 * @param replPRList
//	 */
//	private void scheduleReplacingPRs(Long pSId) {
//
//		/**
//		 * Instance handlers
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
//
//		SessionScheduler sessionScheduler = new SessionScheduler();
//
//		try {
//
//			/**
//			 * The replacing scheduled list of DTOs
//			 */
//			ArrayList<SchedDTO> replDTOList = new ArrayList<SchedDTO>();
//
//			// 2.0. Plan the replacing PRs
//			for (ProgrammingRequest replPR : PRListProcessor.replPRListMap.get(pSId)) {
//						
//				for (AcquisitionRequest aR : replPR.getAcquisitionRequestList()) {
//
//					for (DTO dto : aR.getDtoList()) {
//						
//						if (hpCivilUniqueIdListMap.get(pSId).contains(replPR.getCivilianUniqueId())) {
//						
//							/**
//							 * The scheduled DTO
//							 */
//							SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId, replPR.getUserList().get(0).getUgsId(),
//									replPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto, 
//									replPR.getUserList().get(0).getAcquisitionStationIdList(), false);
//	
//							logger.info("Add replacing internal DTO: " + schedDTO.getDTOId());
//							
//							replDTOList.add(schedDTO);
//							
//							break;
//						}
//					}
//				}
//			}
//			
//			// 2.1. Plan the replacing DTOList
//			if (rulesPerformer.planSchedDTOList(pSId, replDTOList, true)) {
//					
//				logger.info("A number of replacing PRs " + replDTOList.size() + " has been scheduled for Planning Session: " + pSId);
//
//			} else {
//				
//				logger.warn("Some replacing PRs have NOT been scheduled for Planning Session: " + pSId);
//			}
//
//			logger.info("A number of " + replDTOList.size() + 
//					" replacing PRs has been scheduled.");
//
//			
//			// 3.0. Get the accepted DTOs
//			ArrayList<SchedDTO> schedSol = rulesPerformer.getAcceptedDTOs(pSId);
//			
//			// 4.0. Update the Planning Session statuses
//			logger.info("Update the Planning Session statuses.");
//			sessionScheduler.setPlanStatuses(pSId, schedSol, replDTOList);
//
//		} catch (Exception ex) {
//
//			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
//		}
//
//	}
//
//}


/**
 *
 * MODULE FILE NAME: CivilRequestHandler.java
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
package com.telespazio.csg.spla.csps.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;

/**
 * The handler of the Civilian HP Requests
 * @author bunkheila
 *
 */
public class HPCivilianRequestHandler {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(HPCivilianRequestHandler.class);

	/**
	 * The HP-Civilian DTO Ids map
	 */
	public static HashMap<Long, ArrayList<String>> hpCivilDTOIdListMap;
	
	/**
	 * The HP-Civilian Unique Ids map
	 */
	public static HashMap<Long, ArrayList<String>> hpCivilUniqueIdListMap;

	
	/**
	 * Handle HP-Civilian requests
	 * 
	 * @param pSId
	 */
	public void handleHpCivilRequests(Long pSId) {

		// Find replacing PRs of HP-Civilian/Crisis
		logger.debug("Find HP-Civilian/Crisis requests to be replaced.");
		findReplacedPRs(pSId);

		// Schedule replacing PRs
		logger.debug("Schedule replacing requests.");
		scheduleReplacingPRs(pSId);
	}

	/**
	 * Find replacing PRs (Civilian / Crisis)
	 * 
	 * @param pSId
	 * @return
	 */
	private void findReplacedPRs(Long pSId) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();
		
		try {
			
			/**
			 * The cancelled list of DTOs
			 */
			ArrayList<SchedDTO> cancDTOList = new ArrayList<SchedDTO>();
			
			for (ProgrammingRequest replPR : PRListProcessor.replPRListMap.get(pSId)) {

				logger.debug("Search replacing PR: " + replPR.getProgrammingRequestId() 
				+ " with unique Id: " + replPR.getCivilianUniqueId());
				
				/**
				 * The map of the PR scheduling Ids
				 */
				Iterator<Entry<String, ProgrammingRequest>> it = PRListProcessor.pRSchedIdMap.get(pSId).entrySet().iterator();

				while (it.hasNext()) {
 
					/**
					 * The Programming Request
					 */
					ProgrammingRequest pR = it.next().getValue();
			
					if (pR.getCivilianUniqueId() != null && pR.getCivilianUniqueId().equals(replPR.getCivilianUniqueId())
							&& ! (ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId())
							.equals(ObjectMapper.parseDMToSchedPRId(replPR.getUserList().get(0).getUgsId(), 
									replPR.getProgrammingRequestId())))) {

						logger.trace("Match HP-Civilian PR: " + pR.getProgrammingRequestId() 
						+ " with unique Id: " + pR.getCivilianUniqueId());
						
						// Update HP-Civilian requests statuses
						logger.info("Unique PR: " + ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(),
									pR.getProgrammingRequestId()) + " has to be replaced by civilian PR.");

						for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {
		
							// Retract DTO List
							rulesPerformer.retractDTOList(pSId, ObjectMapper.parseDMToSchedDTOList(pSId,
									pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
									aR.getAcquisititionRequestId(), aR.getDtoList(), 
									pR.getUserList().get(0).getAcquisitionStationIdList(), false), 
									ReasonOfReject.systemConflict);
							
							for (DTO dto : aR.getDtoList()) {

								/**
								 * The scheduled DTO
								 */
								SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId, 
										pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
										aR.getAcquisititionRequestId(), dto,
										pR.getUserList().get(0).getAcquisitionStationIdList(), false);

								cancDTOList.add(schedDTO);
						
								if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTO.getDTOId())) {
								
									hpCivilDTOIdListMap.get(pSId).add(schedDTO.getDTOId());
									hpCivilUniqueIdListMap.get(pSId).add(pR.getCivilianUniqueId());
								}	
							}
						}						
					}
				}
			}
			
			logger.info("A number of " + cancDTOList.size() + " PRs to be replaced is found.");
			
			// 2.0. Get the accepted DTOs
			ArrayList<SchedDTO> schedSol = rulesPerformer.getAcceptedDTOs(pSId);

			// 3.0. Update the Planning Session statuses
			logger.info("Update the Planning Session statuses.");
			sessionScheduler.setPlanStatuses(pSId, schedSol, cancDTOList);
			
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}
	

	/**
	 * Schedule the replaced PRs (PP-Civilian)
	 * 
	 * // TODO: check break
	 * 
	 * @param pSId
	 * @param replPRList
	 */
	private void scheduleReplacingPRs(Long pSId) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		SessionScheduler sessionScheduler = new SessionScheduler();

		try {

			/**
			 * The replacing scheduled list of DTOs
			 */
			ArrayList<SchedDTO> replDTOList = new ArrayList<SchedDTO>();

			/**
			 * The replacing DTO size
			 */
			int replPRSize = 0;
			
			// 2.0. Plan the replacing PRs
			for (ProgrammingRequest replPR : PRListProcessor.replPRListMap.get(pSId)) {
					
				/**
				 * The scheduling PR Id
				 */
				String schedPRId = ObjectMapper.parseDMToSchedPRId(replPR.getUgsId(), replPR.getProgrammingRequestId());
				
				for (AcquisitionRequest aR : replPR.getAcquisitionRequestList()) {

					/**
					 * The scheduling AR Id
					 */
					String schedARId = ObjectMapper.parseDMToSchedARId(replPR.getUgsId(), replPR.getProgrammingRequestId(),
							aR.getAcquisititionRequestId());
					
					// Standard AR check
					if (RequestChecker.isStandardAR(pSId, schedARId)) {
												
						if (hpCivilUniqueIdListMap.get(pSId).contains(replPR.getCivilianUniqueId())) {

							for (DTO dto : aR.getDtoList()) {

								/**
								 * The scheduled DTO
								 */
								SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId, replPR.getUserList().get(0).getUgsId(),
										replPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto, 
										replPR.getUserList().get(0).getAcquisitionStationIdList(), false);
		
								logger.info("Add replacing internal DTO: " + schedDTO.getDTOId());
								
								replDTOList.add(schedDTO);
								
//								break;
							}
						}
						
						// 2.1. Plan the replacing DTOList
						if (rulesPerformer.planSchedDTOList(pSId, replDTOList, true)) {
				 			
							replPRSize ++;
						}

					// No Standard AR check	
					} else {
						
						if (hpCivilUniqueIdListMap.get(pSId).contains(replPR.getCivilianUniqueId())) {

							logger.debug("Build the internal Equivalent AR.");	
								
							/**
							 * The Equivalent AR
							 */
							SchedAR equivAR = new SchedAR(schedARId, AcquisitionRequestStatus.New, 
									ObjectMapper.parseDMToSchedDTOList(pSId, replPR.getUgsId(), aR.getDtoList(), 
											PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId)
											.getUserList().get(0).getAcquisitionStationIdList(), false), 
											replPR.getType(), replPR.getMode());
								
							equivAR.setEquivalentDTO(aR.getEquivalentDTO());

							if (equivAR.getDtoList().isEmpty()) {
								
								logger.warn("Equivalent DTO not properly set.");
							}
							
							/**
							 * The BRM Equivalent DTO
							 */
							com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = ObjectMapper
									.parseSchedToBRMEquivDTO(pSId, equivAR, equivAR.getEquivalentDTO());
						
							if (rulesPerformer.planEquivDTO(pSId, brmEquivDTO, false)) {
								
								replPRSize ++;
							}
						}
					}
				}
			}
			
			logger.info("A number of " + replPRSize 
					+ " replacing PRs has been scheduled for Planning Session: " + pSId);
		
			// 3.0. Get the accepted DTOs
			ArrayList<SchedDTO> schedSol = rulesPerformer.getAcceptedDTOs(pSId);
			
			// 4.0. Update the Planning Session statuses
			sessionScheduler.setPlanStatuses(pSId, schedSol, replDTOList);

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

	}

}


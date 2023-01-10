/**
 *
 * MODULE FILE NAME: EquivDTOHandler.java
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.ReasonOfReject;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.DTOTimeComparator;
//import com.telespazio.csg.spla.csps.utils.AoICalculator;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityResult.DI2SOnlineRequest;
import com.telespazio.splaif.protobuf.DI2SMessage.DI2SCompatibilityResult.DI2SProgrammingRequestSet;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.Category;
import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;

/**
 * The Equivalent DTO Handler class
 */
public class EquivDTOHandler {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(EquivDTOHandler.class);

	/**
	 * The map of master DI2S DTOs
	 */
	public static HashMap<Long, HashMap<String, SchedDTO>> di2sMasterSchedDTOMap;

	/**
	 * The map of slave DI2S DTOs
	 */
	public static HashMap<Long, HashMap<String, SchedDTO>> di2sSlaveSchedDTOMap;
	
	/**
	 * The map of slave DTO Id list
	 */
	public static HashMap<Long, ArrayList<String>> slaveDTOIdListMap;
	
	/**
	 * The map of slave DI2S DTO Ids
	 */
	public static HashMap<Long, HashMap<String, String>> di2sLinkedIdsMap;
	
	/**
	 * The DI2S request counter
	 */
	private int requestCount;

	/**
	 * Handle multiple DI2S (online/offline) requests 
	 * 
	 * @param pSId
	 * @param msl
	 * @return the list of DI2S-able PRs
	 */
	public ArrayList<ProgrammingRequest> handleMultiDI2SRequests(Long pSId) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The DI2S Programming Request list
		 */
		ArrayList<ProgrammingRequest> di2sProgReqList = new ArrayList<>();

		try {

			/**
			 * The list of DI2S DTO Ids
			 */
			ArrayList<String> di2sDTOIdList = new ArrayList<>();

			/**
			 * The list of DI2S AR Ids
			 */
			ArrayList<String> di2sARIdList = new ArrayList<>();

			// Init the DI2S counters
			requestCount = 0;
			// responseCount = 0;

			// 1.0. Search compatible DI2S DTOs
			logger.info("Search global compatibility of DI2S DTOs...");

			for (String rejDTOId : SessionScheduler.rejARDTOIdSetMap.get(pSId)) {

				// 1.1. Updated check about filtering (filtRejDTOIdListMap not contains rejDTOId)
				if (! RulesPerformer.getPlannedARIds(pSId).contains(ObjectMapper.getSchedARId(rejDTOId))
						&& ! di2sARIdList.contains(ObjectMapper.getSchedARId(rejDTOId))		
						&& !FilterDTOHandler.filtRejDTOIdListMap.get(pSId).contains(rejDTOId)) {

					logger.debug("Search DI2S compatibility for rejected DTO: " + rejDTOId);
			
					// Search DI2S compatibility for rejected DTO
					for (String planDTOId : SessionScheduler.planDTOIdListMap.get(pSId)) {

						if (!di2sDTOIdList.contains(planDTOId)) {

							logger.trace("Search compatibility with planned DTO: " + planDTOId);

							if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(rejDTOId)
									&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(planDTOId)) {

								logger.trace("Search DI2S flag and PRType compatibility.");

								// Search DI2S-ability and PR type compatibility
								if (areDI2Sable(pSId, planDTOId, rejDTOId) 
										&& areCompatiblePRTypes(pSId, planDTOId, rejDTOId)) {

									logger.trace("Search DTO Sensor Mode compatibility.");

									if (areCompatibleSensorModes(
											PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getSensorMode(),
											PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getSensorMode())) {

										logger.trace("Search User Category compatibility.");

										if (PRListProcessor.pRSchedIdMap.get(pSId)
												.get(ObjectMapper.getSchedPRId(rejDTOId)).getCategory() == null
												|| PRListProcessor.pRSchedIdMap.get(pSId)
														.get(ObjectMapper.getSchedPRId(planDTOId))
														.getCategory() == null) {

											logger.warn("No User category mode found!"); 
											// TODO: delete workaround

											PRListProcessor.pRSchedIdMap.get(pSId)
													.get(ObjectMapper.getSchedPRId(rejDTOId))
													.setCategory(Category.Civilian);
										}

										if (PRListProcessor.pRSchedIdMap.get(pSId)
												.get(ObjectMapper.getSchedPRId(rejDTOId)).getCategory()
												.equals(PRListProcessor.pRSchedIdMap.get(pSId)
														.get(ObjectMapper.getSchedPRId(planDTOId)).getCategory())) {

											logger.trace("Search DTO Look Side compatibility.");

											if (PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getLookSide()
													.equals(PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId)
														.getLookSide())) {

												logger.trace("Search Polarization compatibility.");
												
												if (arePolarCompatible(pSId,
												PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getPolarization(),
												PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getPolarization())) 											
												{

//												logger.trace("Search AoI compatibility.");
//
//												if (aoICalculator.isIntersectedAoI(
//														PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId)
//																.getAreaOfInterest(),
//														PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId)
//																.getAreaOfInterest())) {

													logger.trace("Search DTO interval compatibility.");

													if (isTimeCompatible(pSId, planDTOId, rejDTOId)) {

														// Check subscription conditions
														logger.trace("Search subscriptability compatibility.");
														if (areSubscrCompatible(pSId, planDTOId, rejDTOId)) {

															logger.debug("Detected DI2S compatible requests: "
																	+ "rejected DTO " + rejDTOId + " with planned DTO "
																	+ planDTOId);

															// Add plan request data
															di2sARIdList.add(ObjectMapper.getSchedARId(planDTOId));
															di2sDTOIdList.add(planDTOId);

															// Add rejected request data
															di2sARIdList.add(ObjectMapper.getSchedARId(rejDTOId));
															di2sDTOIdList.add(rejDTOId);

															requestCount ++;

															break;
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			logger.info("A number of " + requestCount + " requests "
					+ " are checked as compatible with DI2S operative mode for Planning Session: " + pSId);

			//Update rejected DTOs
			rulesPerformer.setRejectedDTOs(pSId);
			
			// 2.0. Get DI2S-able PRList 
			di2sProgReqList = getDI2SablePRList(pSId, di2sDTOIdList);
				
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return di2sProgReqList;
	}
		
	/**
	 * Handle single DI2S (online/offline) requests 
	 * 
	 * @param pSId
	 * @param schedARId
	 * @return the list of DI2S-able PRs
	 */
	public ArrayList<ProgrammingRequest> handleSingleDI2SRequests(Long pSId, String schedARId) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The DI2S Programming Request list
		 */
		ArrayList<ProgrammingRequest> di2sProgReqList = new ArrayList<>();

		try {

			/**
			 * The list of DI2S DTO Ids
			 */
			ArrayList<String> di2sDTOIdList = new ArrayList<>();

			/**
			 * The list of DI2S AR Ids
			 */
			ArrayList<String> di2sARIdList = new ArrayList<>();

			// Init the DI2S counters
			requestCount = 0;
			// responseCount = 0;

			// 1.0. Search compatible DI2S DTOs
			logger.info("Search dynamical compatibility of DI2S DTOs...");

			for (DTO rejDTO : PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getDtoList()) {

				/**
				 * The rejected DTO Id
				 */
				String rejDTOId = ObjectMapper.parseDMToSchedDTOId(ObjectMapper.getUgsId(schedARId), 
						ObjectMapper.getPRId(schedARId), ObjectMapper.getARId(schedARId), rejDTO.getDtoId());
				
				if (! RulesPerformer.getPlannedARIds(pSId).contains(ObjectMapper.getSchedARId(rejDTOId))
						&& ! di2sARIdList.contains(ObjectMapper.getSchedARId(rejDTOId))) {

					logger.debug("Search DI2S compatibility for rejected DTO: " + rejDTOId);
			
					// Search DI2S compatibility for rejected DTO
					for (String planDTOId : SessionScheduler.planDTOIdListMap.get(pSId)) {

						if (!di2sDTOIdList.contains(planDTOId)) {

							logger.trace("Search compatibility with planned DTO: " + planDTOId);

							if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(rejDTOId)
									&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(planDTOId)) {

								logger.trace("Search DI2S flag and PRType compatibility.");

								if (areDI2Sable(pSId, planDTOId, rejDTOId) 
										&& areCompatiblePRTypes(pSId, planDTOId, rejDTOId)) {

									logger.trace("Search DTO Sensor Mode compatibility.");

									if (areCompatibleSensorModes(
											PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getSensorMode(),
											PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getSensorMode())) {

										logger.trace("Search User Category compatibility.");

										if (PRListProcessor.pRSchedIdMap.get(pSId)
												.get(ObjectMapper.getSchedPRId(rejDTOId)).getCategory() == null
												|| PRListProcessor.pRSchedIdMap.get(pSId)
														.get(ObjectMapper.getSchedPRId(planDTOId))
														.getCategory() == null) {

											logger.warn("No User category mode found!"); 
											// TODO: delete workaround

											PRListProcessor.pRSchedIdMap.get(pSId)
													.get(ObjectMapper.getSchedPRId(rejDTOId))
													.setCategory(Category.Civilian);
										}

										if (PRListProcessor.pRSchedIdMap.get(pSId)
												.get(ObjectMapper.getSchedPRId(rejDTOId)).getCategory()
												.equals(PRListProcessor.pRSchedIdMap.get(pSId)
														.get(ObjectMapper.getSchedPRId(planDTOId)).getCategory())) {

											logger.trace("Search DTO Look Side compatibility.");

											if (PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getLookSide()
													.equals(PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId)
														.getLookSide())) {

												logger.trace("Search Polarization compatibility.");
												
												if (arePolarCompatible(pSId,
												PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getPolarization(),
												PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getPolarization())) 											
												{

//												logger.trace("Search AoI compatibility.");
//
//												if (aoICalculator.isIntersectedAoI(
//														PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId)
//																.getAreaOfInterest(),
//														PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId)
//																.getAreaOfInterest())) {

													logger.trace("Search DTO interval compatibility.");

													if (isTimeCompatible(pSId, planDTOId, rejDTOId)) {

														// Check subscription conditions
														logger.trace("Search subscriptability compatibility.");
														if (areSubscrCompatible(pSId, planDTOId, rejDTOId)) {

															logger.debug("Detected DI2S compatible requests: "
																	+ "rejected DTO " + rejDTOId + " with planned DTO "
																	+ planDTOId);

															// Add plan request data
															di2sARIdList.add(ObjectMapper.getSchedARId(planDTOId));
															di2sDTOIdList.add(planDTOId);

															// Add rejected request data
															di2sARIdList.add(ObjectMapper.getSchedARId(rejDTOId));
															di2sDTOIdList.add(rejDTOId);

															requestCount ++;

															break;
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			// Added on 29/03/2022 for info upgrade
			if (di2sDTOIdList.size() == 2) {
			
				logger.info("DTO requests " + di2sDTOIdList.get(0) + " and " + di2sDTOIdList.get(1)
						+ " are checked as compatible with DI2S operative mode for Planning Session: " + pSId);
			}
			
			// 2.0 Update rejected DTOs
			rulesPerformer.setRejectedDTOs(pSId);
			
			// 2.1. Get DI2S-able PRList 
			di2sProgReqList = getDI2SablePRList(pSId, di2sDTOIdList);
			
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return di2sProgReqList;
	}

	/**
	 * 
	 * @param pSId
	 * @param di2sDTOIdList
	 * @return
	 * @throws SPLAException 
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<ProgrammingRequest> getDI2SablePRList(Long pSId, 
			ArrayList<String> di2sDTOIdList) throws SPLAException {
		
		/**
		 * The DI2S Programming Request list
		 */
		ArrayList<ProgrammingRequest> di2sProgReqList = new ArrayList<>();
		
		for (String di2sDTOId : (ArrayList<String>) di2sDTOIdList.clone()) {

			for (int i = 0; i < PRListProcessor.pRListMap.get(pSId).size(); i ++) {

				ProgrammingRequest pR = (ProgrammingRequest) PRListProcessor.pRListMap.get(pSId).get(i).cloneModel();
				
				for (int j = 0; j < pR.getAcquisitionRequestList().size(); j ++) {

					AcquisitionRequest aR = (AcquisitionRequest) pR.getAcquisitionRequestList().get(j).cloneModel();
					
					for (int k = 0;  k < aR.getDtoList().size(); k ++) {

						/**
						 * The DI2S input DTO
						 */
						DTO dto = (DTO) aR.getDtoList().get(k).cloneModel();
						
						if (ObjectMapper.parseDMToSchedDTOId(pR.getUserList().get(0).getUgsId(),
								pR.getProgrammingRequestId(), aR.getAcquisititionRequestId(),
								dto.getDtoId()).equals(di2sDTOId)) {

							/**
							 * The scheduling DTO Id
							 */
							String schedDTOId = ObjectMapper.parseDMToSchedDTOId(
									pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(),
									aR.getAcquisititionRequestId(), dto.getDtoId());

							/**
							 * The DI2S scheduling DTO
							 */
							SchedDTO schedDTO = ObjectMapper.parseDMToSchedDTO(pSId,
									pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(),
									aR.getAcquisititionRequestId(), dto, 
									pR.getUserList().get(0).getAcquisitionStationIdList(), 
									false);

							logger.trace("Set DI2S DTO: " +  schedDTO.getDTOId());

							if (RulesPerformer.rejDTORuleListMap.get(pSId).containsKey(schedDTO.getDTOId())) {

								// Add DI2S slave DTO
								di2sSlaveSchedDTOMap.get(pSId).put(schedDTOId, schedDTO);

							} else {
									
								// Add DI2S master DTO
								di2sMasterSchedDTOMap.get(pSId).put(schedDTOId, schedDTO);
							}

							// Add DI2S-able PR, AR and DTO only
							aR.getDtoList().clear();
							aR.addDto(dto);
							pR.getAcquisitionRequestList().clear();
							pR.addAcquisitionRequest(aR);

							di2sProgReqList.add(pR);

							break;
						}
					}
				}
			}
		}
		
		return di2sProgReqList;
	}
	
	/**
	 * Handle (online/offline) DI2S result
	 *
	 * @param pSId
	 * @param di2sPRSet
	 * @throws Exception
	 */
	public boolean handleDI2SResult(Long pSId, DI2SProgrammingRequestSet di2sPRSet) {

		/**
		 * The output result
		 */
		boolean result = false;
		
		try {
			
			/**
			 * Instance handlers
			 */
			RulesPerformer rulesPerformer = new RulesPerformer();

			SessionScheduler sessionScheduler = new SessionScheduler();

			/**
			 * The DI2S online request
			 */
			DI2SOnlineRequest di2sReq = di2sPRSet.getDi2SOnlineRequest();

			/**
			 * The Master DTO Id
			 */
			String masterSchedDTOId = ObjectMapper.parseDMToSchedDTOId(di2sReq.getMasterUGSId(),
					di2sReq.getMasterPRId(), di2sReq.getMasterARId(), di2sReq.getMasterDTOId());
			
			/**
			 * The Slave DTO Id
			 */
			String slaveSchedDTOId = ObjectMapper.parseDMToSchedDTOId(di2sReq.getSlaveUGSId(), 
					di2sReq.getSlavePRId(), di2sReq.getSlaveARId(), di2sReq.getSlaveDTOId());
 
			/**
			 * The DI2S DTO list
			 */
			ArrayList<SchedDTO> di2sSchedDTOList = new ArrayList<>();
			
			// 1.0. check DI2S result data			
			if (di2sMasterSchedDTOMap.get(pSId).containsKey(masterSchedDTOId)
					&& di2sSlaveSchedDTOMap.get(pSId).containsKey(slaveSchedDTOId)
					&& 	! PRListProcessor.aRSchedIdMap.get(pSId).containsKey(
							ObjectMapper.getARId(slaveSchedDTOId))) {

				/**
				 * The Master DTO
				 */
				SchedDTO masterSchedDTO = di2sMasterSchedDTOMap.get(pSId).get(masterSchedDTOId);

				/**
				 * The Slave DTO
				 */
				SchedDTO slaveSchedDTO = di2sSlaveSchedDTOMap.get(pSId).get(slaveSchedDTOId);	
				
				di2sSchedDTOList.add(masterSchedDTO);
			
				if (di2sPRSet.hasDi2SOnlineDTO() && di2sPRSet.getDi2SOnlineDTO().getDtoMaster() != null) 
				{
	
					logger.info("DI2S compatibility found by SPARC for PR Set: " + di2sPRSet.getPrSetId());
	
					/**
					 * The Master DI2S info
					 */
					String[] masterIds = masterSchedDTOId.split(Configuration.splitChar);
	
					/**
					 * The Slave DI2S info
					 */
					String[] slaveIds = slaveSchedDTOId.split(Configuration.splitChar);
					
					logger.debug("Parse DI2S DTO result.");
	
					/**
					 * The DI2S Master scheduling DTO
					 */				
					SchedDTO di2sMasterDTO = ObjectMapper.parseDMToSchedDTO(pSId, masterIds[0], masterIds[1], masterIds[2],
						(DTO) MessageHandler.deserializeInputString(di2sPRSet.getDi2SOnlineDTO().getDtoMaster()),
						masterSchedDTO.getPrefStationIdList(), false);
								
					/**
					 * The DI2S Slave scheduling DTO
					 */
					SchedDTO di2sSlaveDTO = ObjectMapper.parseDMToSchedDTO(pSId, slaveIds[0], slaveIds[1], slaveIds[2],
							(DTO) MessageHandler.deserializeInputString(di2sPRSet.getDi2SOnlineDTO().getDtoSlave()),
							slaveSchedDTO.getPrefStationIdList(), false);
	
					logger.debug("Received Master DTO: " + ((DTO) MessageHandler.deserializeInputString(
							di2sPRSet.getDi2SOnlineDTO().getDtoMaster())).toString());
					logger.debug("Received Slave DTO: " + ((DTO) MessageHandler.deserializeInputString(
							di2sPRSet.getDi2SOnlineDTO().getDtoSlave())).toString());
										
					/**
					 * The Equivalent DTO 
					 */
					EquivalentDTO equivDTO = (EquivalentDTO) MessageHandler
							.deserializeByteArray(di2sPRSet.getDi2SOnlineRequest().getEquivalentDTO().toByteArray());
					equivDTO.setTaskList(new ArrayList<Task>());

					logger.debug("Built Equivalent DTO: " + equivDTO);
					
					/**
					 * The Equivalent DTO list
					 */
					ArrayList<SchedDTO> equivDTOList = new ArrayList<>();
	
					logger.debug("Add Master DI2S DTO: " + masterSchedDTOId + " in the Equivalent DTO list.");
					equivDTOList.add(di2sMasterDTO);
					
					logger.debug("Add Slave DI2S DTO: " + slaveSchedDTOId + " in the Equivalent DTO list.");
					equivDTOList.add(di2sSlaveDTO);
						
					/**
					 * The Equivalent AR
					 */
					SchedAR equivAR = new SchedAR(di2sMasterDTO.getARId(), AcquisitionRequestStatus.New, 
							equivDTOList, di2sMasterDTO.getPRType(), PRMode.DI2S);
					equivAR.setEquivalentDTO(equivDTO);
	
					if (equivAR.getDtoList().isEmpty()) {
						
						logger.warn("Equivalent DTO not properly set.");
					}
	
					// Added condition on 20/01/2022 to manage DI2S-able PRs
					// Removed on 15/2/2022 to wait the complete fix about the DI2S
//					for (SchedDTO schedDTO : equivDTOList) {
//						
//						PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.getSchedPRId(
//								schedDTO.getDTOId()), true);
//					}
					
					/**
					 * The BRM Equivalent DTO
					 */
					com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = ObjectMapper
							.parseSchedToBRMEquivDTO(pSId, equivAR, equivAR.getEquivalentDTO());
								
					if (brmEquivDTO.getAllDtoInEquivalentDto().isEmpty()) {
						
						logger.warn("Equivalent DTO not properly set.");
					}
						
					// 1.1. Plan Equivalent DTO
					if (rulesPerformer.planEquivDTO(pSId, brmEquivDTO, false)) {
	
						// Add slave DTO into the scheduled DTO list
						di2sSchedDTOList.add(di2sSlaveDTO);
						
						logger.info("DI2S Equivalent DTO successfully scheduled by BRM.");

						di2sLinkedIdsMap.get(pSId).put(masterSchedDTOId, slaveSchedDTOId);
						
						// 1.2. Substitute DI2S Enchryption Info
						// Added on 12/09/2022
						substituteDi2sEncrInfo(pSId, di2sPRSet);
						
						// 1.3. Substitute DI2S-compatible DTOs
						substituteDi2sDTOs(pSId, di2sPRSet, equivDTO);
								
						// 1.4. Update Rejected DTOs
						rulesPerformer.setRejectedDTOs(pSId);
						
						// 1.5. Update Planned Tasks
						rulesPerformer.updateSchedTasks(pSId, false);
						
						result = true;
						
						// Added on 01/04/2022 for DI2S requests management
						if (RequestChecker.isDefence(ObjectMapper.getUgsId(masterSchedDTOId))) {
								
							// Updated on 29/08/2022
							if (getDI2SVisibility(pSId, PRListProcessor.pRSchedIdMap.get(pSId)
									.get(ObjectMapper.getSchedPRId(masterSchedDTOId)),
									PRListProcessor.pRSchedIdMap.get(pSId)
									.get(ObjectMapper.getSchedPRId(slaveSchedDTOId))) == 0) {
							
								PRListProcessor.pRIntBoolMap.get(pSId).put(
										ObjectMapper.getSchedPRId(masterSchedDTOId), true);	
							}
						}
	
					} else {
							
						logger.info("DI2S DTO NOT scheduled by BRM.");
	
						logger.debug("Reschedule Master DTO: " + masterSchedDTOId);
		
						// 1.2. Reschedule initial master DTO
						rulesPerformer.retractDTO(pSId, masterSchedDTO, ReasonOfReject.deletedForDi2s);	
						
						if (rulesPerformer.planSchedDTO(pSId, masterSchedDTO)) {
								
							logger.info("Master DTO successfully rescheduled: " + masterSchedDTOId);
							
							di2sSchedDTOList.add(masterSchedDTO);

						} else {
	
							logger.warn("Master DTO NOT rescheduled. Failed repair...");
							
							// TODO: check BRM problem
						}
					}
										
				} else {
					
					logger.info("DI2S compatibility NOT found by SPARC for PR Set: " + di2sPRSet.getPrSetId());			
				}

			} else {
				
				logger.info("DI2S Response incompatible with the input PR Set: " + di2sPRSet.getPrSetId());	
			}
			
//			// 2.0 Update rejected DTOs
//			rulesPerformer.setRejectedDTOs(pSId);
			
			// 2.1. Update DI2S statuses
			logger.info("Update the statuses of the master and slave DI2S requests.");
			sessionScheduler.setPlanStatuses(pSId, rulesPerformer.getAcceptedDTOs(pSId), 
					di2sSchedDTOList);

		} catch (Exception e) {

			logger.error("Exception raised: " + e.getMessage());
		}
		
		return result;
	}

	/**
	 * Check DI2S-ability between DTOs
	 * @param pSId
	 * @param planDTOId
	 * @param rejDTOId
	 * @return
	 */
	private boolean areDI2Sable(Long pSId, String planDTOId, String rejDTOId) {
		
		/**
		 * The compatibility boolean
		 */
		boolean areDi2Sable = false;

		if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId))
				.getDi2sAvailabilityFlag() && PRListProcessor.pRSchedIdMap.get(pSId)
				.get(ObjectMapper.getSchedPRId(planDTOId))
				.getDi2sAvailabilityFlag()) {
			
			// Set compatibility
			areDi2Sable = true;
		}
		
		if ((PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).isDi2SFlag()
				|| PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).isDi2SFlag())) {
			
			// Unset compatibility
			areDi2Sable = false;
		}
					
		// Added on 28/04/2022 to handle condition compatibility between different ugsId
		if (Configuration.sameUgsForDI2SFlag 
				&& ! (ObjectMapper.getUgsId(planDTOId)
						.equals(ObjectMapper.getUgsId(rejDTOId)))) {
			
			// Unset compatibility
			areDi2Sable = false;
		}
		
		return areDi2Sable;
	}
	
	/**
	 * Check compatibility between planned and rejected sensor modes
	 * (S-1A/B with S-1A/B or S-2A/B with S-2A/B)
	 *
	 * @param planSensorMode
	 * @param rejSensorMode
	 * @return
	 */
	private boolean areCompatibleSensorModes(DTOSensorMode planSensorMode, DTOSensorMode rejSensorMode) {

		/**
		 * The compatibility boolean
		 */
		boolean areCompatible = false;

		if ((rejSensorMode.toString().contains("SPOTLIGHT_1A") || (rejSensorMode.toString().contains("SPOTLIGHT_1B"))
				&& planSensorMode.toString().contains("SPOTLIGHT_1A") || planSensorMode.toString().contains("SPOTLIGHT_1B"))
				|| (rejSensorMode.toString().contains("SPOTLIGHT_2A") || rejSensorMode.toString().contains("SPOTLIGHT_2B")
				&& planSensorMode.toString().contains("SPOTLIGHT_2A") || planSensorMode.toString().contains("SPOTLIGHT_2B"))) {

			// Set compatibility		   
			areCompatible = true;
		}
		
		if (rejSensorMode.toString().contains("MS") || 
				planSensorMode.toString().contains("MS")) {

			// Unset compatibility
			areCompatible = false;
		}

		return areCompatible;
	}

	/**
	 * Substitute DI2S EncryptionInfo
	 * // Implemented on 12/09/2022
	 * 
	 * @param pSId
	 * @param di2sPRSet
	 * @param equivDTO
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws SPLAException 
	 */
	private void substituteDi2sEncrInfo(Long pSId, DI2SProgrammingRequestSet di2sPRSet)
			throws ClassNotFoundException, IOException, SPLAException {
		
		logger.info("Substitute DI2S-compatible EnchryptionInfo.");

		/**
		 * Online request
		 */
		DI2SOnlineRequest onlineReq = di2sPRSet.getDi2SOnlineRequest();
		
		// The  Master Scheduling Ids
		String masterSchedPRId = ObjectMapper.parseDMToSchedPRId(onlineReq.getMasterUGSId(), 
				onlineReq.getMasterPRId());
		String masterSchedARId = ObjectMapper.parseDMToSchedARId(onlineReq.getMasterUGSId(), 
				onlineReq.getMasterPRId(), onlineReq.getMasterARId()); 
		
		// The  Slave Scheduling Ids
		String slaveSchedPRId = ObjectMapper.parseDMToSchedPRId(onlineReq.getSlaveUGSId(), 
				onlineReq.getSlavePRId());
		String slaveSchedARId = ObjectMapper.parseDMToSchedARId(onlineReq.getMasterUGSId(), 
				onlineReq.getMasterPRId(), onlineReq.getSlaveARId()); 
		
		// Check DI2S international appliability
		int checkDI2SInt = getDI2SVisibility(pSId, PRListProcessor.pRSchedIdMap.get(pSId)
				.get(masterSchedPRId), PRListProcessor.pRSchedIdMap.get(pSId)
				.get(slaveSchedPRId));
				
		int i  = 0;
		
		for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {
			
			int j = 0;

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {
			
				if (pR.getUserList().get(0).getUgsId().equals(onlineReq.getMasterUGSId())
						&& pR.getProgrammingRequestId().equals(onlineReq.getMasterPRId())) {	
					
					logger.debug("Substitute Enchryption Info for Master DI2S PR: " 
					+ ObjectMapper.parseDMToSchedPRId(pR.getUserList().get(0).getUgsId(), 
							pR.getProgrammingRequestId()));
					
					// TODO: updated
					if (checkDI2SInt == 3) {
						
						setPREnchryptionInfo(pSId, aR, slaveSchedARId);
					}
					
					pR.getAcquisitionRequestList().set(j, aR);
					PRListProcessor.pRSchedIdMap.get(pSId).put(masterSchedPRId, pR);			
					PRListProcessor.pRListMap.get(pSId).set(i, pR);
				
				} else if (pR.getUserList().get(0).getUgsId().equals(onlineReq.getSlaveUGSId())
						&& pR.getProgrammingRequestId().equals(onlineReq.getSlavePRId())) {
					
					logger.debug("Substitute Enchryption Info for Slave DI2S PR: " 
					+ ObjectMapper.parseDMToSchedPRId(
							pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId()));
					
					// TODO: updated
					if (checkDI2SInt != 3) {
						
						setPREnchryptionInfo(pSId, aR, masterSchedARId);
					}
					
					pR.getAcquisitionRequestList().set(j, aR);
					PRListProcessor.pRSchedIdMap.get(pSId).put(slaveSchedPRId, pR);
					PRListProcessor.pRListMap.get(pSId).set(i, pR);

				}
				
				j++;
			}
			
			i ++;
		}
		
	}

	
	/**
	 * Substitute DI2S online DTOs
	 * 
	 * @param pSId
	 * @param di2sPRSet
	 * @param equivDTO
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws SPLAException 
	 */
	@SuppressWarnings("unchecked")
	private void substituteDi2sDTOs(Long pSId, DI2SProgrammingRequestSet di2sPRSet, EquivalentDTO equivDTO)
			throws ClassNotFoundException, IOException, SPLAException {

		logger.info("Substitute DI2S-compatible DTOs.");

		/**
		 * Online request
		 */
		DI2SOnlineRequest onlineReq = di2sPRSet.getDi2SOnlineRequest();
				
		int i = 0;

		for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {
			
			int j = 0;

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

				int k = 0;

				for (DTO dto : (ArrayList<DTO>)((ArrayList<DTO>) aR.getDtoList()).clone()) {

					if (pR.getUserList().get(0).getUgsId().equals(onlineReq.getMasterUGSId())
							&& pR.getProgrammingRequestId().equals(onlineReq.getMasterPRId())
							&& aR.getAcquisititionRequestId().equals(onlineReq.getMasterARId())
							&& dto.getDtoId().equals(onlineReq.getMasterDTOId())) {

						logger.debug("Substitute DTO for Master DI2S DTO: " + ObjectMapper.parseDMToSchedDTOId(
								pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
								aR.getAcquisititionRequestId(), dto.getDtoId()));
						
						// Substitute master DTO
						DTO masterDTO = (DTO) MessageHandler
								.deserializeByteArray(di2sPRSet.getDi2SOnlineDTO().getDtoMaster().toByteArray().clone());						
						masterDTO.setDi2SFlag(true);
											
						// Removed on 24/03/2022 from SPLA-4.5.4 ---------------
//						// Reset polarization according to DHM features (Discussion started from the mail of G.S. of 29/9/2020)
//						masterDTO.setPolarization(dto.getPolarization());
											
						aR.getDtoList().set(k, masterDTO);
						aR.setEquivalentDTO(equivDTO);
						pR.getAcquisitionRequestList().set(j, aR);
						
						PRListProcessor.pRListMap.get(pSId).set(i, pR);
						
						/**
						 * The master DTO Id
						 */
						String masterDTOId = ObjectMapper.parseDMToSchedDTOId(onlineReq.getMasterUGSId(), 
								onlineReq.getMasterPRId(), onlineReq.getMasterARId(), onlineReq.getMasterDTOId());

						logger.debug("Matched polarization of the DI2S DTO: " 
								+ masterDTOId  + " is: " + masterDTO.getPolarization());					
						
						/**
						 * The master AR Id
						 */
						String masterARId = ObjectMapper.parseDMToSchedARId(onlineReq.getMasterUGSId(), 
								onlineReq.getMasterPRId(), onlineReq.getMasterARId());

						/**
						 * The master PR Id
						 */
						String masterPRId = ObjectMapper.parseDMToSchedPRId(onlineReq.getMasterUGSId(),
								onlineReq.getMasterPRId());						
											
						// Update Master DTO maps
						PRListProcessor.dtoSchedIdMap.get(pSId).put(masterDTOId, masterDTO);
						PRListProcessor.aRSchedIdMap.get(pSId).put(masterARId, aR);
						PRListProcessor.pRSchedIdMap.get(pSId).put(masterPRId, pR);
					
					} else if (pR.getUserList().get(0).getUgsId().equals(onlineReq.getSlaveUGSId())
							&& pR.getProgrammingRequestId().equals(onlineReq.getSlavePRId())
							&& aR.getAcquisititionRequestId().equals(onlineReq.getSlaveARId())
							&& dto.getDtoId().equals(onlineReq.getSlaveDTOId())) {

						logger.debug("Substitute DTO for Slave DI2S DTO: " + ObjectMapper.parseDMToSchedDTOId(
								pR.getUserList().get(0).getUgsId(), pR.getProgrammingRequestId(), 
								aR.getAcquisititionRequestId(), dto.getDtoId()));
						
						// Substitute slave DTO
						DTO slaveDTO = (DTO) MessageHandler
								.deserializeByteArray(di2sPRSet.getDi2SOnlineDTO().getDtoSlave().toByteArray().clone());
						slaveDTO.setDi2SFlag(true);

						// Removed on 24/03/2022 from SPLA-4.5.4 ---------------
//						// Reset polarization according to DHM features (Discussion started from the mail of G.S. of 29/9/2020)
//						slaveDTO.setPolarization(dto.getPolarization());
												
						aR.getDtoList().set(k, slaveDTO);
						pR.getAcquisitionRequestList().set(j, aR);
						PRListProcessor.pRListMap.get(pSId).set(i, pR);

						/**
						 * The slave DTO Id
						 */
						String slaveDTOId = ObjectMapper.parseDMToSchedDTOId(onlineReq.getSlaveUGSId(), 
								onlineReq.getSlavePRId(), onlineReq.getSlaveARId(), onlineReq.getSlaveDTOId());						

						/**
						 * The slave AR Id
						 */
						String slaveARId = ObjectMapper.parseDMToSchedARId(onlineReq.getSlaveUGSId(), 
								onlineReq.getSlavePRId(), onlineReq.getSlaveARId());

						/**
						 * The slave PR Id
						 */
						String slavePRId = ObjectMapper.parseDMToSchedPRId(onlineReq.getSlaveUGSId(),
								onlineReq.getSlavePRId());
						
						// Update slave DTO maps
						PRListProcessor.dtoSchedIdMap.get(pSId).put(slaveDTOId, slaveDTO);
						PRListProcessor.aRSchedIdMap.get(pSId).put(slaveARId, aR);
						PRListProcessor.pRSchedIdMap.get(pSId).put(slavePRId, pR);

					}

					k++;
				}

				j++;
			}

			i++;
		}
	}

	/**
	 * Check the PR type compatibility
	 *  
	 * @param pSId
	 * @param planDTOId
	 * @param rejDTOId
	 * @return
	 */
	private boolean areCompatiblePRTypes(Long pSId, String planDTOId, String rejDTOId) {
		
		/**
		 * The compatibility boolean
		 */
		boolean areCompatible = false;
		
		if (PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(planDTOId)).getType().equals(
				PRListProcessor.pRSchedIdMap.get(pSId).get(ObjectMapper.getSchedPRId(rejDTOId)).getType())) {

			// Set compatibility		   
			areCompatible = true;
		}
		
		return areCompatible;
	}
	
	/**
	 * Check the compatibility with respect to the DI2S subscription requirements
	 * // TODO: corrected on 31/05/2022- bug 1817 
	 * @param pSId
	 * @param masterDTOId
	 * @param slaveDTOId
	 * @return
	 */
	private boolean areSubscrCompatible(Long pSId, String masterDTOId, String slaveDTOId) {

		/**
		 * The compatibility boolean
		 */
		boolean areCompatible = false;

		/**
		 * The master PR
		 */
		ProgrammingRequest masterPR = PRListProcessor.pRSchedIdMap.get(pSId)
				.get(ObjectMapper.getSchedPRId(masterDTOId));

		/**
		 * The slave PR
		 */
		ProgrammingRequest slavePR = PRListProcessor.pRSchedIdMap.get(pSId)
				.get(ObjectMapper.getSchedPRId(slaveDTOId));

		// Compute subscription compatibility
		if (masterPR.getUserList().size() == 1 && slavePR.getUserList().size() == 1) {

			if (masterPR.getAvailableForSubscription() && ! RequestChecker.isSubscribed(masterPR)
					&& slavePR.getAvailableForSubscription() && ! RequestChecker.isSubscribed(slavePR)) {

				// Not-NEO condition
				if (RequestChecker.isNotNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility())) {
					
					areCompatible = true;

				// NEO condition
				} else if (RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNEO(slavePR.getVisibility())) {

					if (masterPR.getUserList().get(0).getOwnerId().equals(slavePR.getUserList().get(0).getOwnerId())) {

						areCompatible = true;
					}

			    // NEO/Not-NEO condition
				} else if ((RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility()))
						|| (RequestChecker.isNotNEO(masterPR.getVisibility())
								&& RequestChecker.isNEO(slavePR.getVisibility()))) {

					if (masterPR.getUserList().get(0).getOwnerId().equals(slavePR.getUserList().get(0).getOwnerId())) {

						areCompatible = true;
					}
					
				// International condition	
				} else if (RequestChecker.isInternational(masterPR.getVisibility())
						&& RequestChecker.isInternational(slavePR.getVisibility())) {

					areCompatible = true;

				// National condition
				} else if (RequestChecker.isNational(masterPR.getVisibility())
						&& RequestChecker.isNational(slavePR.getVisibility())) {

					if (masterPR.getUserList().get(0).getOwnerId().equals(slavePR.getUserList().get(0).getOwnerId())) {

						areCompatible = true;
					}

				// National/International condition
				} else if ((RequestChecker.isNational(masterPR.getVisibility())
						&& RequestChecker.isInternational(slavePR.getVisibility()))
						|| (RequestChecker.isInternational(masterPR.getVisibility())
								&& RequestChecker.isNational(slavePR.getVisibility()))) {
					
					if (masterPR.getUserList().get(0).getOwnerId().equals(slavePR.getUserList().get(0).getOwnerId())) {

						areCompatible = true;
					}
				}
			} else {
				
				logger.trace("Requests are not available for subscription!");
			}
		}

		return areCompatible;
	}
	
//	/**
//	 * Check the compatibility with respect to the DI2S polarizations
//	 * Removed on 17/3/2022 for DI2S compatibility change
//	 *
//	 * @param pSId
//	 * @param masterPolar
//	 * @param slavePolar
//	 * @return
//	 */
//	private boolean arePolarCompatible(Long pSId, String masterPolar, String slavePolar) {
//		
//		/**
//		 * The compatibility boolean
//		 */
//		boolean areCompatible = false;
//		
//		if (masterPolar.equals(slavePolar)) {
//			
//			// Set compatibility
//			areCompatible = true;
//				
//		} else if (masterPolar.contains(slavePolar) || slavePolar.contains(masterPolar)) {
//			
//			// Set compatibility
//			areCompatible = true;
//
//		}
//	
//		return areCompatible;
//
//	}
	
	/**
	 * Check the compatibility with respect to the DI2S polarizations
	 * Added on 17/3/2022 for DI2S compatibility change
	 * 
	 * @param pSId
	 * @param masterPolar
	 * @param slavePolar
	 * @return
	 */
	private boolean arePolarCompatible(Long pSId, String masterPolar, String slavePolar) {
		
		/**
		 * The compatibility boolean
		 */
		boolean areCompatible = false;
		
		/**
		 * The list of DI2S polarizations
		 */
		ArrayList<String> di2sPolarList = new ArrayList<String>();
		
		di2sPolarList.add("HH");
		di2sPolarList.add("VV");
		di2sPolarList.add("HH+HV");
		di2sPolarList.add("VV+VH");
		
		if ((masterPolar.equals(di2sPolarList.get(0)) 
				&& slavePolar.contains(di2sPolarList.get(0))
				|| (masterPolar.equals(di2sPolarList.get(0)) 
						&& slavePolar.contains(di2sPolarList.get(2))))) {
			
			// set compatibility
			areCompatible = true;
			
		} else if ((masterPolar.equals(di2sPolarList.get(1)) 
				&& slavePolar.contains(di2sPolarList.get(1))
				|| (masterPolar.equals(di2sPolarList.get(1)) 
						&& slavePolar.contains(di2sPolarList.get(3))))) {
			
			// set compatibility
			areCompatible = true;
		
		} else if ((masterPolar.equals(di2sPolarList.get(2)) 
				&& slavePolar.contains(di2sPolarList.get(0))
				|| (masterPolar.equals(di2sPolarList.get(2))
						&& slavePolar.contains(di2sPolarList.get(2))))) {
			
			// set compatibility
			areCompatible = true;
			
		} else if ((masterPolar.equals(di2sPolarList.get(3)) 
				&& slavePolar.contains(di2sPolarList.get(1))
				|| (masterPolar.equals(di2sPolarList.get(3))
						&& slavePolar.contains(di2sPolarList.get(3))))) {
			
			// set compatibility
			areCompatible = true;
		}
	
		return areCompatible;
	}

	/**
	 * Check the compatibility with respect to the Di2s time requirements
	 * 1. maximum time 2. gap time
	 * 
	 * // TODO: check compatibility with respect maximum time
	 * 
	 * @param pSId
	 * @param planDTOId
	 * @param rejDTOId
	 * @return
	 * @throws Exception 
	 */
	private boolean isTimeCompatible(Long pSId, String planDTOId, String rejDTOId) throws Exception {

		/**
		 * The compatibility boolean
		 */
		boolean isCompatible = false;

		if ((Math.abs(PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getStopTime().getTime()
				- PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getStartTime()
						.getTime()) <= (Configuration.di2sCompTime * 1000.0))
				|| (Math.abs(PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId).getStopTime().getTime()
						- PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId).getStartTime()
								.getTime()) <= (Configuration.di2sCompTime * 1000.0))) {
	
			/**
			 * The coupling DTO list
			 */
			ArrayList<DTO> di2sDTOList = new ArrayList<DTO>(); 
			
			di2sDTOList.add(PRListProcessor.dtoSchedIdMap.get(pSId).get(planDTOId));
			di2sDTOList.add(PRListProcessor.dtoSchedIdMap.get(pSId).get(rejDTOId));
			
			// Sort DI2S DTOs
			Collections.sort(di2sDTOList, new DTOTimeComparator());
			
			/**
			 * The DTOs gap
			 */
			long gap = di2sDTOList.get(1).getStartTime().getTime() 
					- di2sDTOList.get(0).getStopTime().getTime();
			
			if (RulesPerformer.brmOperMap.get(pSId).checkIfDi2sAreConsistentInDistance(
					ObjectMapper.parseDMToBRMSensorMode(di2sDTOList.get(0).getSensorMode()), 
					ObjectMapper.parseDMToBRMSensorMode(di2sDTOList.get(1).getSensorMode()), 
					gap, pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), 
					RulesPerformer.brmParamsMap.get(pSId))) 
			{				
				isCompatible = true;
			}
		}

		return isCompatible;
	}
	
//	/**
//	 * Plan Di2S Downloads
//	 * @param pSId
//	 */
//	public void planDI2SDwl(Long pSId) {
//		
//		logger.info("Plan DI2S downloads for Planning Session: " + pSId);
//		
//		/**
//		 * Instance handlers
//		 */
//		RulesPerformer rulesPerformer = new RulesPerformer();
//		
//		/**
//		 * The DI2S list of DTOs
//		 */	
//		Collection<SchedDTO> di2SDTOList = di2sMasterSchedDTOMap.get(pSId).values();
//		
//		di2SDTOList.addAll(
//				di2sSlaveSchedDTOMap.get(pSId).values());
//		
//		Collection<Date> schedDTOStartList = new ArrayList<Date>(); 
//		
//		for (SchedDTO schedDTO : di2SDTOList) {
//			
//			schedDTOStartList.add(schedDTO.getStartTime());
//		}
//		
//		/**
//		 * The minumum DI2S start time
//		 */
//		Date minStartTime = Collections.min(schedDTOStartList);
//		
//		// Plan Download Tasks
//		if (! rulesPerformer.planDwlTasks(pSId, minStartTime)) {
//			
//			// TODO!
//		}
//
//	}
	
	/**
	 * Set working equivalent DTO for Theatre and Experimental requests
	 * @param pSId
	 * @param equivDTO
	 * @param pRMode
	 * @param extraPitchBIC
	 * @param workPR
	 * @throws Exception 
	 */
	public void setWorkTheatreExpEquivDTO(Long pSId, EquivalentDTO equivDTO, PRMode pRMode, 
			double extraPitchBIC, ProgrammingRequest workPR) throws Exception {
			
		/**
		 * The scheduling DTO list
		 */
		ArrayList<SchedDTO> equivSchedDTOList = new ArrayList<SchedDTO>();
		
		for (AcquisitionRequest workAR : workPR.getAcquisitionRequestList()) {
			
			// Theatre/Experimental case
			if (workAR.getEquivalentDTO().getEquivalentDtoId().contains(
					equivDTO.getEquivalentDtoId())) {

				/**
				 * The scheduling AR Id
				 */
				String schedARId = ObjectMapper.parseDMToSchedARId(
						workPR.getUserList().get(0).getUgsId(), workPR.getProgrammingRequestId(),
						workAR.getAcquisititionRequestId());
				
				if (! SessionActivator.initARIdEquivDTOMap.get(pSId).containsKey(schedARId)) {
									
					logger.debug("Following Equivalent DTO: " + workAR.getEquivalentDTO().getEquivalentDtoId()
							+ " is imported into the working plan.");
					
					// Add theatre/exp DTO
					equivSchedDTOList.addAll(ObjectMapper.parseDMToSchedDTOList(pSId, schedARId,
							workAR.getDtoList(), workPR.getUserList().get(0).getAcquisitionStationIdList(), 
									true));
					/**
					 * The scheduling AR
					 */
					SchedAR schedAR = ObjectMapper.parseDMToSchedAR(pSId,
							workPR.getUserList().get(0).getUgsId(), workPR.getProgrammingRequestId(),
							workAR, workPR.getUserList().get(0).getAcquisitionStationIdList(), 
							workPR.getPitchExtraBIC(), true);
							
					ObjectMapper.parseSchedToBRMEquivDTO(pSId, schedAR, equivDTO);
					
					/**
					 * The BRM Equivalent DTO
					 */
					com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = 
							ObjectMapper.parseSchedToBRMEquivDTO(pSId, equivSchedDTOList, equivDTO, pRMode, extraPitchBIC);
		
					if (SessionChecker.isDelta(pSId)) {
						
						brmEquivDTO.setDecrementBic(false);
					}
					
					// Update equivalent DTO maps
					SessionActivator.initEquivDTOMap.get(pSId).put(equivDTO.getEquivalentDtoId(), brmEquivDTO);	
					SessionActivator.initARIdEquivDTOMap.get(pSId).put(ObjectMapper.parseDMToSchedARId(
							workPR.getUserList().get(0).getUgsId(), workPR.getProgrammingRequestId(),
							workAR.getAcquisititionRequestId()),  brmEquivDTO);
				}
			}
		}
	}
	
	/**
	 * Set working equivalent DTO for DI2S requests
	 * Updated on 18/3/2022 for DI2S polarization management
	 * @param pSId
	 * @param equivDTO
	 * @param pRMode
	 * @param extraPitchBIC
	 * @throws Exception 
	 */
	public void setWorkDI2SEquivDTO(Long pSId, EquivalentDTO equivDTO, PRMode pRMode, double extraPitchBIC) 
			throws Exception {

		/**
		 * The scheduling DTO list
		 */
		ArrayList<SchedDTO> equivSchedDTOList = new ArrayList<SchedDTO>();

		for (Task workTask : PersistPerformer.workTaskListMap.get(pSId)) {

			/**
			 * The scheduling DTO Id
			 */
			String schedARId = ObjectMapper.parseDMToSchedARId(
					workTask.getUgsId(), 
					workTask.getProgrammingRequestId(), 
					workTask.getAcquisitionRequestId());
						
			if (workTask.getTaskType().equals(TaskType.ACQ) && workTask.getDi2s() != null) {

				// online DI2S case
				if (equivDTO.getEquivalentDtoId().contains(schedARId)) {
	
					if (! SessionActivator.initARIdEquivDTOMap.get(pSId).containsKey(schedARId)) {
					
						logger.debug("Following Equivalent DTO: " + equivDTO.getEquivalentDtoId()
								+ " is imported into the working plan.");			
						
						// The master DTO Id
						String masterDTOId = ObjectMapper.parseDMToSchedDTOId(
								workTask.getUgsId(), 
								workTask.getProgrammingRequestId(), 
								workTask.getAcquisitionRequestId(), 
								workTask.getDtoId());
						
						// The slave DTO Id
						String slaveDTOId = ObjectMapper.parseDMToSchedDTOId(
								workTask.getDi2s().getUgsId(), 
								workTask.getDi2s().getProgrammingRequestId(), 
								workTask.getDi2s().getAcquisitionRequestId(), 
								workTask.getDi2s().getDtoId());
											
						if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(masterDTOId)
								&& PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(slaveDTOId)) {
													
							// Add master DTO
							equivSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId, masterDTOId, 
									PRListProcessor.dtoSchedIdMap.get(pSId).get(masterDTOId), true));
		
							// Add slave DTO
							equivSchedDTOList.add(ObjectMapper.parseDMToSchedDTO(pSId, slaveDTOId,
									PRListProcessor.dtoSchedIdMap.get(pSId).get(slaveDTOId), true));
						}
						
						/** 
						 * The BRM Equivalent DTO
						 */
						com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = 
								ObjectMapper.parseSchedToBRMEquivDTO(pSId, equivSchedDTOList, equivDTO, pRMode, extraPitchBIC);
	
						if (SessionChecker.isDelta(pSId)) {
							
							brmEquivDTO.setDecrementBic(false);
						}
						
						// Update equivalent DTO maps									
						SessionActivator.initEquivDTOMap.get(pSId).put(equivDTO.getEquivalentDtoId(), brmEquivDTO);
						SessionActivator.initARIdEquivDTOMap.get(pSId).put(ObjectMapper.getSchedARId(masterDTOId), brmEquivDTO);
					}
				}
			}			
		}	
	}
	
	/**
	 * Get the international boolean of the PR according to the table:
	 *  AR Master Vis			 AR Slave Vis  			 Owner    			Index			Output
	 * --------------------------------------------------------------------------------------------
	 *	NEO						 NEO					 Same   			National		1
	 *	NEO						 NotNEO					 Same   			National		2
	 *	NotNEO					 NEO					 Same   			National		3
	 *	NotNEO					 NotNEO					 Same or Diff 		International	0
	 *
	 * If check is available it returns:
	 * - 1, 2, 3 for the National index cases 
	 * - 0 for the International index case
	 * // Added on 29/08/2022 for compatibility with S-IM output
	 * 
	 * @param pSId
	 * @param masterPR
	 * @param slavePR
	 */
	public int getDI2SVisibility(Long pSId, ProgrammingRequest masterPR, ProgrammingRequest slavePR) {
		
		
		if (Configuration.checkDI2SVis) {
			// NEO/NEO
			if ((RequestChecker.isNEO(masterPR.getVisibility())
					&& RequestChecker.isNEO(slavePR.getVisibility()))) {
				
				PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
						masterPR.getUgsId(), masterPR.getProgrammingRequestId()),
						false);
				
				return 1;
			// NEO/NotNEO
			} else if ((RequestChecker.isNEO(masterPR.getVisibility())
					&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
				
				PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
						masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
						false);
				
				return 2;
			// NotNEO/NEO	
			} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
					&& RequestChecker.isNEO(slavePR.getVisibility()))) {
				
				PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
						masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
						false);
				
				return 3;
			// NotNEO/NotNEO
			} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
					&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
				
				PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
						masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
						true);
				
				return 0;
			}
		}
		
		return 0;
	}
	
	/**
	 * Get the international boolean of the PR according to the table:
	 *  AR Master Vis			 AR Slave Vis  			 Owner    			Index
	 * -----------------------------------------------------------------------------------
	 *	NEO						 NEO					 Same   			National
	 *	NEO						 NotNEO					 Same   			National
	 *	NotNEO					 NEO					 Same   			National
	 *	NotNEO					 NotNEO					 Same or Diff 		International
	 *
	 * If check is available it returns:
	 * - 1, 2, 3 for the National index cases 
	 * - 0 for the International index case
	 * // Added on 29/08/2022 for compatibility with S-IM output
	 * 
	 * @param pSId
	 * @param task
	 */
	public int getDI2SVisibility(Long pSId, Task task) {
		
		
		if (Configuration.checkDI2SVis && task.getDi2s() != null) {
			
			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
					ObjectMapper.parseDMToSchedPRId(task.getUgsId(),
					task.getProgrammingRequestId()))
					&& PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
						ObjectMapper.parseDMToSchedPRId(task.getDi2s().getUgsId(),
						task.getDi2s().getProgrammingRequestId()))) {
			
				// The master PR Id
				ProgrammingRequest masterPR = PRListProcessor.pRSchedIdMap.get(pSId).get(
						ObjectMapper.parseDMToSchedPRId(task.getUgsId(),
						task.getProgrammingRequestId()));
				
				// The slave PR Id
				ProgrammingRequest slavePR = PRListProcessor.pRSchedIdMap.get(pSId).get(
						ObjectMapper.parseDMToSchedPRId(task.getDi2s().getUgsId(),
						task.getDi2s().getProgrammingRequestId()));
			
				if ((RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()),
							false);
					
					return 1;
				
				} else if ((RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							false);
					
					return 2;
					
				} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
						&& RequestChecker.isNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							false);
					
					return 3;
					
				} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							true);
					
					return 0;
				}
			}
		}
		
		return 0;
	}
	
	/**
	 * Get the international boolean of the PR according to the table:
	 *  AR Master Vis			 AR Slave Vis  			 Owner    			Index
	 * -----------------------------------------------------------------------------------
	 *	NEO						 NEO					 Same   			National
	 *	NEO						 NotNEO					 Same   			National
	 *	NotNEO					 NEO					 Same   			National
	 *	NotNEO					 NotNEO					 Same or Diff 		International
	 *
	 * If check is available it returns:
	 * - 1, 2, 3 for the National index cases 
	 * - 0 for the International index case
	 * // Added on 29/08/2022 for compatibility with S-IM output
	 * 
	 * @param pSId
	 * @param task
	 */
	public int getDI2SVisibility(Long pSId, com.nais.spla.brm.library.main.ontology.tasks.Task task) {
		
		
		if (Configuration.checkDI2SVis && task.getDi2sInfo() != null) {
			
			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
					ObjectMapper.getSchedPRId(task.getIdTask()))
					&& PRListProcessor.pRSchedIdMap.get(pSId).containsKey(
						ObjectMapper.getSchedPRId(task.getDi2sInfo().getRelativeSlaveId()))) {
			
				// The master PR Id
				ProgrammingRequest masterPR = PRListProcessor.pRSchedIdMap.get(pSId).get(
						ObjectMapper.getSchedPRId(task.getIdTask()));
				
				// The slave PR Id
				ProgrammingRequest slavePR = PRListProcessor.pRSchedIdMap.get(pSId).get(
						ObjectMapper.getSchedPRId(task.getDi2sInfo().getRelativeSlaveId()));
			
				if ((RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()),
							false);
					
					return 1;
				
				} else if ((RequestChecker.isNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							false);
					
					return 2;
					
				} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
						&& RequestChecker.isNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							false);
					
					return 3;
				
				} else if ((RequestChecker.isNotNEO(masterPR.getVisibility())
						&& RequestChecker.isNotNEO(slavePR.getVisibility()))) {
					
					PRListProcessor.pRIntBoolMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							masterPR.getUgsId(), masterPR.getProgrammingRequestId()), 
							true);
					
					return 0;
				}
			}
		}
		
		return 0;
	}
	
	/**	
	 * Set PR Enchryption Info 
	 * // TODO: change AR Enchryption Info for DI2Sable AR
	 * 
	 * @param pSId
	 * @param dwl
	 * @param schedARId
	 */
	public static void setPREnchryptionInfo(Long pSId, AcquisitionRequest aR, String replSchedARId) {
			
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionEKSelection() != null) {
			aR.setEncryptionEKSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionEKSelection());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionEKIndex() != null) {
			aR.setEncryptionEKIndex(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId)
					.getEncryptionEKIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionEKOwnerId() != null) {
			aR.setEncryptionEKOwnerId(
					PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionEKOwnerId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionStrategy() != null) {
			aR.setEncryptionStrategy(
					PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionStrategy());
		} else {
			aR.setEncryptionStrategy("CLEAR");
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionIVIndex() != null) {
			aR.setEncryptionIVIndex(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionIVIndex());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionPageId() != null) {
			aR.setEncryptionPageId(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionPageId());
		}
		if (PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionIVSelection() != null) {
			aR.setEncryptionIVSelection(
					PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getEncryptionIVSelection());
		}					

		aR.setAdditionalPar1(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getAdditionalPar1());
		aR.setAdditionalPar2(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getAdditionalPar2());
		aR.setAdditionalPar3(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getAdditionalPar3());
		aR.setAdditionalPar4(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getAdditionalPar4());
		aR.setAdditionalPar5(PRListProcessor.aRSchedIdMap.get(pSId).get(replSchedARId).getAdditionalPar5());	   
	}
	
}

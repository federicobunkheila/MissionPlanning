/**
 *
 * MODULE FILE NAME: PersistPerformer.java
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

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat.ParseException;
import com.nais.spla.brm.library.main.drools.utils.TaskPlanned;
import com.nais.spla.brm.library.main.ontology.enums.ManeuverType;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.Partner;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;
import com.telespazio.csg.spla.csps.utils.BICCalculator;
import com.telespazio.csg.spla.csps.utils.BRMTaskTimeComparator;
import com.telespazio.csg.spla.csps.utils.DefaultDebugger;
import com.telespazio.csg.spla.csps.utils.ObjectMapper;
import com.telespazio.csg.spla.csps.utils.RequestChecker;
import com.telespazio.csg.spla.csps.utils.SessionChecker;
import com.telespazio.csg.spla.csps.utils.TUPCalculator;														
import com.telespazio.csg.spla.csps.utils.TaskStartTimeComparator;
import com.telespazio.csg.spla.csps.utils.VisTimeComparator;
import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.PRKind;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRStatus;
import it.sistematica.spla.datamodel.core.enums.ResourceStatusType;
import it.sistematica.spla.datamodel.core.enums.TaskMarkType;
import it.sistematica.spla.datamodel.core.enums.TaskStatus;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanningSession;
import it.sistematica.spla.datamodel.core.model.ProgrammingRequest;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.bean.PitchIntervals;
import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.Pdht;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogAcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogOwner;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogSatellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogUgs;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.AccountChart;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceStatus;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceValue;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Transaction;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;
import it.sistematica.spla.datamodel.core.model.task.Download;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.datamodel.core.model.task.Store;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;
import it.sistematica.spla.datamodel.core.model.task.BITE;
import it.sistematica.spla.dcm.core.service.CatalogService;
import it.sistematica.spla.dcm.core.service.ConfigService;
import it.sistematica.spla.dpl.core.service.Factory;
import it.sistematica.spla.dpl.core.service.HarmonizationService;
import it.sistematica.spla.dpl.core.service.PlanningSessionService;
import it.sistematica.spla.dpl.core.service.ResourceService;
import it.sistematica.spla.ekmlib.EKMLIB;

/**
 * The persistence performer class
 */
public class PersistPerformer {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(PersistPerformer.class);

	/**
	 * The map of the list of the working tasks
	 */
	public static HashMap<Long, ArrayList<Task>> workTaskListMap;

	/**
	 * The map of the list of the reference tasks
	 */
	public static HashMap<Long, ArrayList<Task>> refTaskListMap;

	/**
	 * The map of the of all the reference tasks by Planning Session
	 */
	public static HashMap<Long, HashMap<Long, ArrayList<Task>>> refPSTaskListMap;

	/**
	 * The map of the list of the reference acquisition Ids
	 */
	public static HashMap<Long, HashMap<String, Task>> refAcqIdMap;

	/**
	 * The map of the partner residual Premium BICs
	 */
	public static HashMap<Long, HashMap<String, ArrayList<Double>>> partnerResBICMap;

	/**
	 * The pitch interval map
	 */
	public static HashMap<String, PitchIntervals> pitchIntervalMap;


// --------------------------------------------

	/**
	 * Save Planning Session Info
	 *
	 * @param pSId
	 * @throws Exception
	 */
	private boolean saveSessionInfo(Long pSId) throws Exception {

		/**
		 * The output boolean
		 */
		boolean isSaved = false;

		try {

			// Set reference planning session statuses
			if (SessionActivator.firstSessionMap.get(pSId)) {

				// Add reference PR Statuses
				addRefPlanPRStatuses(pSId);

			} else {

				// Update reference PR Statuses
				updateRefPlanPRStatuses(pSId);
			}

			if (!Configuration.debugDPLFlag) {

				/**
				 * Harmonization service factory
				 */
				HarmonizationService hS = new Factory().getHarmonizationService();

				// Fill default PRList data
				fillDefaultPRListData(PRListProcessor.pRSchedIdMap.get(pSId).values());

				logger.info("Outcoming PRs to be saved for Planning Session: " + pSId);

				// Save Programming Requests on DPL
				for (ProgrammingRequest pR : PRListProcessor.pRListMap.get(pSId)) {

					logger.info("Saving PR " + pR.getProgrammingRequestId() + " of PRList "
							+ pR.getProgrammingRequestListId() + " for UGS " + pR.getUserList().get(0).getUgsId());

					hS.saveProgrammingRequest(pR);
				}

				logger.info("Outcoming Planning Session to be saved: " + pSId);

				// Save session Info on DPL
				logger.debug("Save Session Info for Planning Session: "
						+ SessionActivator.planSessionMap.get(pSId).toString());

				hS.saveSessionInfo(SessionActivator.planSessionMap.get(pSId));

				logger.info("Session Info successfully saved on DPL for Planning Session: {}", pSId);

				isSaved = true;

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");

				logger.warn("Data relevant to Planning Session: " + pSId + " NOT saved into DPL.");
			}

		} catch (SPLAException e) {

			logger.error("Error saving session {} - {}", pSId, e.getMessage(), e);

			return false;

		} catch (Throwable th) {

			logger.error("Exception raised: " + th.getStackTrace()[0].toString() + th.getMessage());

			logger.warn("Some session info NOT saved into DPL.");

			return false;
		}

		return isSaved;
	}

	/**
	 * Save Not persisted Planning Session Info
	 *
	 * @param pSId
	 * @param workPSId
	 * @throws Exception
	 */
	public boolean saveNotPersistingInfo(Long pSId, Long workPSId) throws Exception {

		/**
		 * The output boolean
		 */
		boolean isSaved = false;
		
		try {

			logger.info("Save not persisting Planning Session Info: " + pSId);

			if (!Configuration.debugDPLFlag) {

				 /**
				 * PlanningSession service factory
				 */
				 PlanningSessionService pS = new Factory().getPlanningSessionService();
				 
				 /**
				  * Harmonization service factory
				  */
				 HarmonizationService hS = new Factory().getHarmonizationService();
				 
				// Fill default PRList data
				fillDefaultPRListData(PRListProcessor.pRSchedIdMap.get(pSId).values());

				logger.info("Outcoming PRs to be saved for Planning Session: " + pSId);

				/**
				 * The list of rejected PRs
				 */
				ArrayList<ProgrammingRequest> rejPRList = getRejPlanSessionPRList(pSId);

				// Save Programming Requests on DPL
				for (ProgrammingRequest rejPR : rejPRList) {

					logger.debug("Adding rejected PR " + rejPR.getProgrammingRequestId() + " of PRList "
							+ rejPR.getProgrammingRequestListId() + " for UGS "
							+ rejPR.getUserList().get(0).getUgsId());
				}

				/**
				 * The working Planning Session
				 */
				PlanningSession workPS = pS.getPlanningSession(workPSId);

				// Add the rejected PR statuses
				addRejPlanPRStatuses(pSId, workPS, rejPRList);
				
				// Fill default PRList data
				fillDefaultPRListData(rejPRList);

				logger.info("Outcoming PRs to be saved for Planning Session: " + pSId);

				// Save Programming Requests on DPL
				for (ProgrammingRequest pR : rejPRList) {

					logger.info("Saving PR " + pR.getProgrammingRequestId() + " of PRList "
							+ pR.getProgrammingRequestListId() + " for UGS " + pR.getUserList().get(0).getUgsId());
				
					hS.saveProgrammingRequest(pR);
				}
	
				logger.info("Working Planning Session to be updated: " + workPSId);
				
				// Save session Info on DPL
				logger.debug("Save Session for working Planning Session: " + workPS.toString());
				 pS.savePlanningSession(workPS);

				logger.info("Session Info successfully added on DPL for Planning Session: {}", pSId);
				
				isSaved = true;

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");

				logger.warn("Data relevant to Planning Session: " + pSId + " NOT saved into DPL.");
			}

		} catch (SPLAException e) {
			
			 logger.error("Error saving session {} - {}", pSId, e.getMessage(), e);
				
			 logger.error("Exception raised: " + e.getStackTrace()[0].toString() + e.getMessage());

			 logger.warn("Session info NOT saved into DPL.");
			 
			 return false;

		} catch (Throwable th) {

			logger.error("Error saving session {} - {}", pSId, th.getMessage(), th);
			
			logger.error("Exception raised: " + th.getStackTrace()[0].toString() + th.getMessage());

			logger.warn("Session info NOT saved into DPL.");

			return false;
		}

		return isSaved;
	}

	/**
	 * Add Plan PR statuses relevant to downloads of reference sessions
	 * TODO: Extended for DI2S on 12/11/2020 and updated on 26/07/2022
	 * @param pSId
	 */
	private void addRefPlanPRStatuses(Long pSId) {

		logger.info("Add statuses relevant to downloads of reference sessions for Planning Session: " + pSId);

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		/**
		 * The list of scheduled DTO Id in download
		 */
		 ArrayList<String> dwlSchedDTOIdList = new ArrayList<String>();

		/**
		 * The DI2S boolean
		 */
		boolean isDI2SFound = false;
		 
		for (com.nais.spla.brm.library.main.ontology.tasks.Download brmDwl : rulesPerformer
				.getAcceptedDownloads(pSId)) {		
			
			if (brmDwl.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.DOWNLOAD)) {
				
				if (refAcqIdMap.get(pSId).containsKey(brmDwl.getRelatedTaskId())
						&& brmDwl.getStartTime()
								.after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& brmDwl.getStartTime()
								.before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime())) {

					logger.debug("Check download of DTO: " + brmDwl.getIdTask());
					
					/**
					 * The Plan Programming Request status
					 */
					PlanProgrammingRequestStatus pRStatus = new PlanProgrammingRequestStatus(
							refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getUgsId(),
							refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getProgrammingRequestListId(),
							refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getProgrammingRequestId(),
							PRStatus.Scheduled, false, // TODO: check replacing flag
							SessionActivator.ugsOwnerIdMap.get(pSId)
									.get(refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getUgsId()));

					/**
					 * The Plan Acquisition Request status
					 */
					PlanAcquisitionRequestStatus aRStatus = new PlanAcquisitionRequestStatus(
							refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getAcquisitionRequestId(),
							AcquisitionRequestStatus.Scheduled);

					/**
					 * The Plan DTO status
					 */
					PlanDtoStatus dtoStatus = new PlanDtoStatus(
							refAcqIdMap.get(pSId).get(brmDwl.getRelatedTaskId()).getDtoId(), DtoStatus.Scheduled);
					
					aRStatus.addDtoStatus(dtoStatus);
					
					// Extended on 26/07/2022 for DI2S offline ---------
					
					// Update reference DI2S offline data
					if (brmDwl.getDi2sInfo() != null 
							&& ObjectMapper.getSchedPRId(brmDwl.getDi2sInfo().getRelativeMasterId()).equals
									(ObjectMapper.getSchedPRId((brmDwl.getDi2sInfo().getRelativeSlaveId())))) {
								
						/**
						 * The Plan DTO status
						 */
						PlanDtoStatus di2sDtoStatus = new PlanDtoStatus(
								ObjectMapper.getDTOId(brmDwl.getDi2sInfo().getRelativeSlaveId()), 
								DtoStatus.Scheduled);
						
						aRStatus.addDtoStatus(di2sDtoStatus);
		
						// Update the list of dwl scheduling DTO Id
						if (! dwlSchedDTOIdList.contains(brmDwl.getDi2sInfo().getRelativeSlaveId())) {
						
							dwlSchedDTOIdList.add(brmDwl.getDi2sInfo().getRelativeSlaveId());
						}
						
						isDI2SFound = true;
					}
					
					pRStatus.addAcquisitionRequestStatus(aRStatus);
					
					// ---------
					
					// Update reference data
					if (! dwlSchedDTOIdList.contains(brmDwl.getRelatedTaskId())) {
						
						// Update the list of dwl scheduling DTO Id
						dwlSchedDTOIdList.add(brmDwl.getRelatedTaskId());
						
						// The previous counter
						int prevCount = 0;
						
						// The found flag
						boolean found = false;
						
						// Update PR Status
						for (PlanProgrammingRequestStatus prevPRStatus : SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()) {
						
							if (pRStatus.getUgsId().equals(prevPRStatus.getUgsId())
									&& pRStatus.getProgrammingRequestId().equals(prevPRStatus.getProgrammingRequestId())) {

								// Update the Planning Session map
								SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().set(prevCount, pRStatus);
							
								found = true;
								
								logger.info("Added reference Plan PR status for Download: " + brmDwl.getIdTask());

								
								break;
							}
							
							prevCount ++;
						}
						
						// Add PRStatus in case not updated
						if (!found) {
							
							logger.info("Added reference Plan PR status for Download: " + brmDwl.getIdTask());
						
							// Extend the Planning Session map
							SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().add(pRStatus);
						}
					}
				}
				
				// Extended  on 12/11/2020 for DI2S online ------------
				
				// Update reference DI2S data
				if (! isDI2SFound && brmDwl.getDi2sInfo() != null 
						&& brmDwl.getStartTime()
						.after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& brmDwl.getStartTime()
						.before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime())) {
				
					String pRListId = null;
					
					if (refAcqIdMap.get(pSId).containsKey(brmDwl.getRelatedTaskId())
						&& PRListProcessor.pRToPRListIdMap.get(pSId).containsKey(
						   ObjectMapper.getSchedPRId(brmDwl.getDi2sInfo().getRelativeSlaveId()))) {
					
						// Updated on 26/07/2022 ------
						
						// The PRList Id
						pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
								.get(ObjectMapper.getSchedPRId(brmDwl.getDi2sInfo().getRelativeSlaveId())).get(0);	
					
						// ---------
						
						if (pRListId != null) {
						
							/**
							 * The Plan Programming Request status
							 */
							PlanProgrammingRequestStatus pRStatus = new PlanProgrammingRequestStatus(
									ObjectMapper.getUgsId(brmDwl.getDi2sInfo().getRelativeSlaveId()),
									pRListId,
									ObjectMapper.getPRId(brmDwl.getDi2sInfo().getRelativeSlaveId()),
									PRStatus.Scheduled, false, // TODO: check replacing flag
									SessionActivator.ugsOwnerIdMap.get(pSId)
											.get(ObjectMapper.getUgsId(brmDwl.getDi2sInfo().getRelativeSlaveId())));
							/**
							 * The Plan Acquisition Request status
							 */
							PlanAcquisitionRequestStatus aRStatus = new PlanAcquisitionRequestStatus(
									ObjectMapper.getARId(brmDwl.getDi2sInfo().getRelativeSlaveId()),
									AcquisitionRequestStatus.Scheduled);
			
							/**
							 * The Plan DTO status
							 */
							PlanDtoStatus dtoStatus = new PlanDtoStatus(
									ObjectMapper.getDTOId(brmDwl.getDi2sInfo().getRelativeSlaveId()), 
									DtoStatus.Scheduled);
							
							aRStatus.addDtoStatus(dtoStatus);
							pRStatus.addAcquisitionRequestStatus(aRStatus);
			
							isDI2SFound = true;
							
							// Update the list of dwl scheduling DTO Id
							if (! dwlSchedDTOIdList.contains(brmDwl.getDi2sInfo().getRelativeSlaveId())) {
								
								dwlSchedDTOIdList.add(brmDwl.getDi2sInfo().getRelativeSlaveId());
							}
							
							// The previous counter
							int prevCount = 0;
							
							// The found flag
							boolean found = false;
							
							// Update PR Status
							for (PlanProgrammingRequestStatus prevPRStatus : SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()) {
							
								if (pRStatus.getUgsId().equals(prevPRStatus.getUgsId())
										&& pRStatus.getProgrammingRequestId().equals(prevPRStatus.getProgrammingRequestId())) {
		
									// Update the Planning Session map
									SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().set(prevCount, pRStatus);
								
									found = true;
									
									break;
								}
								
								prevCount ++;
							}
	
							// Add PRStatus in case not updated
							if (!found) {
								
								logger.info("Added Plan PR status for slave reference Download: "  + brmDwl.getDi2sInfo().getRelativeSlaveId());
							
								// Extend the Planning Session map
								SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().add(pRStatus);
							}
						
						} else {
							
							logger.warn("No PRList associated to found to: " + brmDwl.getRelatedTaskId());
						}
					}
				}
			}
		}
	}
	
	/**
	 * Update Plan PR statuses relevant to downloads of reference sessions
	 * TODO: Updated on 26/07/2022 for DI2S offline management
	 * @param pSId
	 */
	private void updateRefPlanPRStatuses(Long pSId) {

		logger.info("Update statuses relevant to downloads of reference sessions for Planning Session: " + pSId);

		/**
		 * The Rules Performer
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();

		for (com.nais.spla.brm.library.main.ontology.tasks.Download brmDwl : rulesPerformer
				.getAcceptedDownloads(pSId)) {
			
			if (brmDwl.getTaskType().equals(com.nais.spla.brm.library.main.ontology.enums.TaskType.DOWNLOAD)) {

				if (refAcqIdMap.get(pSId).containsKey(brmDwl.getRelatedTaskId())
						&& brmDwl.getStartTime()
								.after(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime())
						&& brmDwl.getStartTime()
								.before(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime())) {

					logger.debug("Check download of DTO: " + brmDwl.getIdTask());
					
					/**
					 * The found flag
					 */
					boolean isFound = false;

					// Update the Planning Session map
					for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId)
							.getProgrammingRequestStatusList()) {
						
						for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {

							for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {
								
								if (brmDwl.getRelatedTaskId()
										.equals(ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
												pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
												dtoStatus.getDtoId()))) {

									isFound = true;

									pRStatus.setStatus(PRStatus.Scheduled);
									aRStatus.setStatus(AcquisitionRequestStatus.Scheduled);
									dtoStatus.setStatus(DtoStatus.Scheduled);

									logger.info("Updated Plan PR status for reference DTO: " + brmDwl.getIdTask());
								
								// Added on 26/07/2022 -----	
									
								} else if (brmDwl.getDi2sInfo() != null &&
										brmDwl.getDi2sInfo().getRelativeSlaveId()
										.equals(ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
												pRStatus.getProgrammingRequestId(), aRStatus.getAcquisitionRequestId(),
												dtoStatus.getDtoId()))) { 

									isFound = true;
									
									pRStatus.setStatus(PRStatus.Scheduled);
									aRStatus.setStatus(AcquisitionRequestStatus.Scheduled);
									dtoStatus.setStatus(DtoStatus.Scheduled);
									
									logger.info("Updated Plan PR status for reference DTO: " 
											+ brmDwl.getDi2sInfo().getRelativeSlaveId());
								}
								
								//  ------
							}

							if (isFound) {

								break;
							}
						}

						if (isFound) {

							break;
						}
					}
				}
			}
		}
	}

	/**
	 * Add Plan PR statuses relevant to downloads or passthroughs of across visibilities
	 * 
	 * @param pSId
	 * @param acrossMHTaskList
	 */
	public void addAcrossPlanPRStatuses(Long pSId, ArrayList<Task> acrossMHTaskList) {

		logger.info("Add statuses relevant to downloads of reference sessions for Planning Session: " + pSId);

		 /**
		 * The list of task Ids across MH
		 */
		 ArrayList<BigDecimal> taskIdList = new ArrayList<BigDecimal>();
		 
		for (Task task : acrossMHTaskList) {		
	
			if (task.getProgrammingRequestId() != null 
					&& PRListProcessor.pRToPRListIdMap.get(pSId).containsKey(
							ObjectMapper.parseDMToSchedPRId(task.getUgsId(), task.getProgrammingRequestId()))) {
			
				/**
				 * The Plan Programming Request status
				 */
				PlanProgrammingRequestStatus pRStatus = new PlanProgrammingRequestStatus(
						task.getUgsId(),
						PRListProcessor.pRToPRListIdMap.get(pSId).get(
								ObjectMapper.parseDMToSchedPRId(task.getUgsId(), task.getProgrammingRequestId())).get(0),
						task.getProgrammingRequestId(),
						PRStatus.Scheduled, false, 
						SessionActivator.ugsOwnerIdMap.get(pSId)
								.get(task.getUgsId()));

				/**
				 * The Plan Acquisition Request status
				 */
				PlanAcquisitionRequestStatus aRStatus = new PlanAcquisitionRequestStatus(
						task.getAcquisitionRequestId(), AcquisitionRequestStatus.Scheduled);

				/**
				 * The Plan DTO status
				 */
				PlanDtoStatus dtoStatus = new PlanDtoStatus(task.getDtoId(), DtoStatus.Scheduled);
				
				aRStatus.addDtoStatus(dtoStatus);
				pRStatus.addAcquisitionRequestStatus(aRStatus);

				// Update reference data
				if (! taskIdList.contains(task.getTaskId())) {
					
					// Update the list of dwl scheduling DTO Id
					taskIdList.add(task.getTaskId());
					
					// The previous counter
					int prevCount = 0;
					
					// The found flag
					boolean found = false;
					
					// Update PR Status
					for (PlanProgrammingRequestStatus prevPRStatus : SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList()) {
					
						if (pRStatus.getUgsId().equals(prevPRStatus.getUgsId())
								&& pRStatus.getProgrammingRequestId().equals(prevPRStatus.getProgrammingRequestId())) {

							// Update the Planning Session map
							SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().set(prevCount, pRStatus);
						
							found = true;
						
							logger.info("Updated Plan PR status for across-MH Download: " + task.getTaskId());
							
							break;
						}
						
						prevCount ++;
					}
					
					// Add PR Status in case not updated
					if (!found) {
						
						logger.info("Added Plan PR status for across-MH Download: " + task.getTaskId());
					
						// Extend the Planning Session map
						SessionActivator.planSessionMap.get(pSId).getProgrammingRequestStatusList().add(pRStatus);
					}
				}
			}
		}
	}
	
	/**
	 * Add Plan PR statuses relevant to rejected PRs
	 *  
	 * @param pSId
	 * @param workPS
	 * @param rejPRList
	 */
	private void addRejPlanPRStatuses(Long pSId, PlanningSession workPS, 
			ArrayList<ProgrammingRequest> rejPRList) {

		/**
		 * Instance handlers
		 */
		RulesPerformer rulesPerformer = new RulesPerformer();
		
		try {

			logger.info("Add the rejected PR statuses for Planning Session: " + pSId);

			if (!Configuration.debugDPLFlag) {
				
				// Set rejected DTOs
				rulesPerformer.setRejectedDTOs(pSId);
				
				// Add PR Statuses
				workPS.getProgrammingRequestStatusList().addAll(
						getRejPlanSessionPRStatuses(pSId, rejPRList));
			}
			
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}
	}

	/**
	 * Save owners data
	 *
	 * @param pSId
	 * @return
	 * @throws Exception
	 */
	public boolean saveOwnersData(Long pSId) throws Exception {

		logger.info("Save owners data for Planning Session " + pSId + " on DPL.");
		
		try {

			/**
			 * Instance handler
			 */
			RulesPerformer rulesPerformer = new RulesPerformer();

			/**
			 * The resource service factory
			 */
			ResourceService rS = new Factory().getResourceService();

			/**
			 * The list of transactions
			 */
			List<Transaction> transList = new ArrayList<>();

			/**
			 * The total partner BICs
			 */
			Double[][] totalPartnerBICs = new Double[SessionActivator.ownerListMap.get(pSId).size()][];

			/**
			 * The reference date of saving
			 */
			Date refDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime();

			// // Update Partners BICs
			// BICCalculator.updatePartnersBICs(pSId);

			// Handle owners BICs
			for (int i = 0; i < SessionActivator.ownerListMap.get(pSId).size(); i++) {

				/**
				 * The owner
				 */
				Owner owner = SessionActivator.ownerListMap.get(pSId).get(i);


				Double[] ownerBICs = BICCalculator.computeOwnerBICs(pSId,
						SessionActivator.partnerListMap.get(pSId).get(i), owner);

				totalPartnerBICs[i] = ownerBICs;

				// Premium BICs
				if (ownerBICs[0] > 0) {

					// Get transactions relevant to premium BICs
					transList.addAll(rulesPerformer.getPartnerLoanTransList(pSId, owner.getCatalogOwner().getOwnerId(),
							"premiumAccountChart"));
				}

				if (owner.getAvailablePremiumBIC() != null) {

					if (!transList.isEmpty()) {

						logger.debug("Save premium transactions for owner: " + owner.getCatalogOwner().getOwnerId());
						rS.saveOwnerTransaction(owner.getCatalogOwner().getOwnerId(), "premiumAccountChart", transList);
					}

					// Add premium transactions
					for (Transaction trans : transList) {

						AccountChart accChart = new AccountChart();

						if (accChart.getTransactions().isEmpty()) {

							accChart.addTransaction(trans);
						}

						owner.setPremiumAccountChart(accChart);
					}

					transList.clear();

					// Save premium BICs
					savePremiumBICs(pSId, rS, owner, refDate, ownerBICs[3]);
				}

				// Routine BICs
				if ((ownerBICs[1] + ownerBICs[2]) > 0) {

					// Get transactions relevant to routine BICs
					transList.addAll(rulesPerformer.getPartnerLoanTransList(pSId, owner.getCatalogOwner().getOwnerId(),
							"routineAccountChart"));
				}

				if (owner.getAvailableRoutineBIC() != null) {

					if (!transList.isEmpty()) {

						logger.debug("Save routine transactions for owner: " + owner.getCatalogOwner().getOwnerId());

						rS.saveOwnerTransaction(owner.getCatalogOwner().getOwnerId(), "routineAccountChart", transList);
					}

					// Add routine transactions
					for (Transaction trans : transList) {

						AccountChart accChart = new AccountChart();

						if (accChart.getTransactions().isEmpty()) {

							accChart.addTransaction(trans);
						}

						owner.setRoutineAccountChart(accChart);
					}

					transList.clear();

					// Save routine BICs
					saveRoutineBICs(pSId, rS, owner, refDate, ownerBICs[4]);
				}

				if (owner.getAvailableNEOBIC() != null) {

					// Save NEO BICs
					saveNEOBICs(pSId, rS, owner, refDate, ownerBICs[6]);
				}
			}

			BICCalculator.writeBICReportFile(pSId, totalPartnerBICs);

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());

			logger.warn("Some owners resources NOT saved into DPL.");

			return false;

		} catch (Throwable th) {

			logger.error("Exception raised: " + th.getStackTrace()[0].toString() + th.getMessage());

			logger.warn("Some owners resources NOT saved into DPL.");

			return false;
		}

		return true;
	}

	/**
	 * Save Premium BICs resource
	 * 
	 * @param pSId
	 * @param rS
	 * @param owner
	 * @param refDate
	 * @param bicValue
	 * @throws SPLAException
	 */
	private void savePremiumBICs(Long pSId, ResourceService rS, Owner owner, Date refDate, double bicValue)
			throws SPLAException {

		/**
		 * The resource value
		 */
		ResourceValue resValue = new ResourceValue(refDate, BigDecimal.valueOf(bicValue), true, pSId);

		// Add Premium BIC
		if (owner.getAvailablePremiumBIC().isEmpty()) {

			owner.getAvailablePremiumBIC().add(resValue);

		} else {

			owner.getAvailablePremiumBIC().set(0, resValue);
		}

		logger.debug("Owner " + owner.getCatalogOwner().getOwnerId() + " has remaining premium BICs: "
				+ owner.getAvailablePremiumBIC().get(0).getValue());

		// Save BICs
		logger.debug("Save premium BICs for owner: " + owner.getCatalogOwner().getOwnerId());

		rS.saveResourceValue(owner.getCatalogOwner().getOwnerId(), "availablePremiumBIC",
				owner.getAvailablePremiumBIC().get(0));
	}

	/**
	 * Save Routine BICs resource
	 * 
	 * @param pSId
	 * @param rS
	 * @param owner
	 * @param refDate
	 * @param bicValue
	 * @throws SPLAException
	 */
	private void saveRoutineBICs(Long pSId, ResourceService rS, Owner owner, Date refDate, double bicValue)
			throws SPLAException {

		/**
		 * The resource value
		 */
		ResourceValue resValue = new ResourceValue(refDate, BigDecimal.valueOf(bicValue), true, pSId);

		// Add Routine BIC
		if (owner.getAvailableRoutineBIC().isEmpty()) {

			owner.getAvailableRoutineBIC().add(resValue);

		} else {

			owner.getAvailableRoutineBIC().set(0, resValue);
		}

		logger.debug("Owner " + owner.getCatalogOwner().getOwnerId() + " has remaining routine BICs: "
				+ owner.getAvailableRoutineBIC().get(0).getValue());

		// Save BICs
		logger.debug("Save routine BICs for owner: " + owner.getCatalogOwner().getOwnerId());

		rS.saveResourceValue(owner.getCatalogOwner().getOwnerId(), "availableRoutineBIC",
				owner.getAvailableRoutineBIC().get(0));
	}

	/**
	 * Save NEO BICs resource
	 * 
	 * @param pSId
	 * @param rS
	 * @param owner
	 * @param refDate
	 * @param bicValue
	 * @throws SPLAException
	 */
	private void saveNEOBICs(Long pSId, ResourceService rS, Owner owner, Date refDate, double bicValue)
			throws SPLAException {

		/**
		 * The resource value
		 */
		ResourceValue resValue = new ResourceValue(refDate, BigDecimal.valueOf(bicValue), true, pSId);

		// Add NEO BIC resource
		if (owner.getAvailableNEOBIC().isEmpty()) {

			owner.getAvailableNEOBIC().add(resValue);

		} else {

			owner.getAvailableNEOBIC().set(0, resValue);
		}

		logger.debug("Owner " + owner.getCatalogOwner().getOwnerId() + " has remaining NEO BICs: "
				+ owner.getAvailableNEOBIC().get(0).getValue());

		// Save NEO BICs status
		logger.debug("Save NEO BICs for owner: " + owner.getCatalogOwner().getOwnerId());

		rS.saveResourceValue(owner.getCatalogOwner().getOwnerId(), "availableNEOBIC",
				owner.getAvailableNEOBIC().get(0));
		// }
	}

	/**
	 * Save the satellite resources in DPL
	 *
	 * @param pSId
	 * @throws Exception
	 */
	public boolean saveSatResources(Long pSId) throws Exception {

		logger.info("Save satellite resources for Planning Session: " + pSId + " on DPL.");
		
		try {
			
			/**
			 * The resource service factory
			 */
			ResourceService rS = new Factory().getResourceService();

			for (int i = 0; i < SessionScheduler.satListMap.get(pSId).size(); i++) {

				/**
				 * The satellite Id
				 */
				String satId = SessionScheduler.satListMap.get(pSId).get(i).getCatalogSatellite().getSatelliteId();

				/**
				 * The PDHT Id
				 */
				String pdhtId = satId + "::PDHT";

				if (RulesPerformer.brmParamsMap.get(pSId) == null) {

					logger.error("BRM data for Planning Session:" + pSId + " NOT available!");
				}

				/**
				 * The reference date of saving
				 */
				Date refDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime();

				if (SessionChecker.isFinal(pSId)) {

					// Get final reference date
					refDate = SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime();
				}

				/**
				 * The satellite PDHT at the MH stop time
				 */
				Pdht satPdht = ObjectMapper.parseBRMToDMPDHT(RulesPerformer.brmOperMap.get(pSId).getPDHTStatus(
						refDate, satId, pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId)), refDate);

				// Save PDHT resources
				savePDHTResources(pSId, rS, refDate, satId, satPdht, pdhtId);

				// Save attitude resources
				saveAttitude(pSId, rS, refDate, satId, SessionScheduler.satListMap.get(pSId).get(i).getAttitude());

				logger.info("Resource values successfully saved for Planning Session: {}", pSId);
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());

			logger.warn("Some resources are NOT saved into DPL.");

			return false;

		} catch (Throwable th) {

			logger.error("Exception raised: " + th.getStackTrace()[0].toString() + th.getMessage());

			logger.warn("Some resources are NOT saved into DPL.");

			return false;
		}

		return true;
	}

	/**
	 * Save PDHT resource
	 * 
	 * @param pSId
	 * @param rS
	 * @param refDate
	 * @param satId
	 * @param satPdht
	 * @param pdhtId
	 * @throws SPLAException
	 */
	private void savePDHTResources(Long pSId, ResourceService rS, Date refDate, String satId, Pdht satPdht,
			String pdhtId) throws SPLAException {

		// Save PDHT MMs resources
		logger.debug("Save PDHT resources at the reference date for satellite: " + satId);

		for (int j = 0; j < satPdht.getMm1().size(); j++) {

			if (satPdht.getMm1().get(j) != null) {

				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm1().get(j).getValue().doubleValue()), true, pSId);
				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm1", resValue);
			}
		}
		for (int j = 0; j < satPdht.getMm2().size(); j++) {

			if (satPdht.getMm2().get(j) != null) {

				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm2().get(j).getValue().doubleValue()), true, pSId);

				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm2", resValue);
			}
		}
		for (int j = 0; j < satPdht.getMm3().size(); j++) {

			if (satPdht.getMm3().get(j) != null) {
				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm3().get(j).getValue().doubleValue()), true, pSId);

				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm3", resValue);
			}
		}
		for (int j = 0; j < satPdht.getMm4().size(); j++) {

			if (satPdht.getMm4().get(j) != null) {
				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm4().get(j).getValue().doubleValue()), true, pSId);

				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm4", resValue);

			}
		}
		for (int j = 0; j < satPdht.getMm5().size(); j++) {

			if (satPdht.getMm5().get(j) != null) {
				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm5().get(j).getValue().doubleValue()), true, pSId);
				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm5", resValue);

			}
		}

		for (int j = 0; j < satPdht.getMm6().size(); j++) {

			if (satPdht.getMm6().get(j) != null) {
				/**
				 * The resource value
				 */
				ResourceValue resValue = new ResourceValue(refDate,
						BigDecimal.valueOf(satPdht.getMm6().get(j).getValue().doubleValue()), true, pSId);
				resValue.setEstimated(true);

				// Save MM
				rS.saveResourceValue(pdhtId, "mm6", resValue);

			}
		}
	}

	/**
	 * Save Attitude resource
	 * 
	 * @param pSId
	 * @param rS
	 * @param refDate
	 * @param satId
	 * @param attList
	 * @throws Exception
	 */
	private void saveAttitude(Long pSId, ResourceService rS, Date refDate, String satId, List<ResourceValue> attList)
			throws Exception {

		logger.debug("Save attitude resource for satellite: " + satId);

		TaskPlanned taskPlan = new TaskPlanned();

		/**
		 * The satellite attitude
		 */
		BigDecimal satAtt = BigDecimal.valueOf(0.0);

		if (attList != null && !attList.isEmpty() && attList.get(0) != null) {

			satAtt = attList.get(0).getValue();
		}

		if (SessionChecker.isFinal(pSId)) {

			// Save final satellite attitude
			List<com.nais.spla.brm.library.main.ontology.tasks.Maneuver> manList = taskPlan.receiveAllManeuvers(
					pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), RulesPerformer.brmParamsMap.get(pSId),
					satId);

			// Filter for Roll maneuvers
			for (int i = 0; i < manList.size(); i ++) {
				
				if (manList.get(i).getType().equals(ManeuverType.PitchCPS)
						|| manList.get(i).getType().equals(ManeuverType.PitchSlew)) {
					
					manList.remove(i);
					
					i --;
				}
			}
			
			// Sort maneuvers by time
			Collections.sort(manList, new BRMTaskTimeComparator());

			if (manList.size() > 0) {

				satAtt = ObjectMapper.parseBRMToDMAttitude(manList.get(manList.size() - 1).isRightToLeftFlag());
			}
		}

		if (satAtt != null) {

			/**
			 * The resource value
			 */
			ResourceValue resValue = new ResourceValue(refDate, satAtt, true, pSId);

			// Save attitude
			rS.saveResourceValue(satId, "attitude", resValue);
		}
	}

	/**
	 * Get the full set of Programming Request relevant to the Mission Horizon
	 * 
	 * @param pSId
	 * @return
	 */
	public List<PlanProgrammingRequestStatus> getPlanPRStatusList(Long pSId) {

		logger.debug("Get the total list of PR statuses.");

		/**
		 * The total list of PRStatuses
		 */
		List<PlanProgrammingRequestStatus> inPRStatusList = new ArrayList<PlanProgrammingRequestStatus>();

		try {

			if (!Configuration.debugDPLFlag) {

				/**
				 * New planning session service factory
				 */
				PlanningSessionService pSS = new Factory().getPlanningSessionService();

				/**
				 * The PRList of the working Planning Session
				 */
				List<PlanProgrammingRequestStatus> totPRStatusList = pSS.getPlanningSession(pSId)
						.getProgrammingRequestStatusList();

				for (PlanProgrammingRequestStatus totPRStatus : totPRStatusList) {

					inPRStatusList.add((PlanProgrammingRequestStatus) totPRStatus.cloneModel());
				}
			}

		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn("Data relevant to working Planning Session NOT retrieved from DPL.");
		}

		return inPRStatusList;
	}

	/**
	 * Get the data relevant to the working Planning Session from DPL
	 *
	 * @param pSId
	 * @param workPS
	 */
	@SuppressWarnings("unchecked")
	public void getWorkPSData(Long pSId,
			com.telespazio.splaif.protobuf.ActivateMessage.Activate.PlanningSession workPS) {

		logger.info("Get the data from the working Planning Session: " + workPS.getPlanningSessionId());

		/**
		 * The equivalent DTO handler
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();

		try {

			if (!Configuration.debugDPLFlag) {

				/**
				 * New planning session service factory
				 */
				PlanningSessionService pSS = new Factory().getPlanningSessionService();

				// Get the relevant PRList for the working Planning Session
				logger.info("Get the relevant PRList from DPL for the working Planning Session: "
						+ workPS.getPlanningSessionId());

				/**
				 * The PRList of the working Planning Session
				 */
				List<ProgrammingRequest> workPRList = pSS.getProgrammingRequest(workPS.getPlanningSessionId());

				// Get working PRs, ARs and DTOs
				for (ProgrammingRequest workPR : workPRList) {

					logger.info("Imported working PR " + workPR.getProgrammingRequestId() + " for UGS "
							+ workPR.getUserList().get(0).getUgsId());

					/**
					 * The list of PR Ids
					 */
					ArrayList<String> pRIdList = new ArrayList<String>();

					pRIdList.add(workPR.getProgrammingRequestListId());

					PRListProcessor.pRToPRListIdMap.get(pSId).put(ObjectMapper.parseDMToSchedPRId(
							workPR.getUserList().get(0).getUgsId(), workPR.getProgrammingRequestId()), pRIdList);

					PRListProcessor.pRSchedIdMap.get(pSId).put(
									ObjectMapper.parseDMToSchedPRId(workPR.getUserList().get(0).getUgsId(),
											workPR.getProgrammingRequestId()),
									(ProgrammingRequest) workPR.cloneModel());

					// Add the working PR in the scheduling Id map
					PRListProcessor.workPRSchedIdMap.get(pSId).put(
									ObjectMapper.parseDMToSchedPRId(workPR.getUserList().get(0).getUgsId(),
											workPR.getProgrammingRequestId()),
									(ProgrammingRequest) workPR.cloneModel());

					/**
					 * The scheduled PR Id
					 */
					String schedPRId = ObjectMapper.parseDMToSchedPRId(workPR.getUserList().get(0).getUgsId(),
							workPR.getProgrammingRequestId());

					// Add the new PR international boolean to the scheduling map
					PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, false);
					
					// Manage international PR
					if ((workPR.getUserList().size() > 1)
							&& RequestChecker.isDefence(workPR.getUgsId())) {						
						
						logger.info("Assigned international flag to PR: " + schedPRId);						
						PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, true);
				
					} else if (workPR.getDi2sAvailabilityFlag()
							&& RequestChecker.isDefence(workPR.getUgsId())) {
						
						logger.info("Assigned international flag to PR: " + schedPRId);
						PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, true);
					}
					
					for (AcquisitionRequest aR : workPR.getAcquisitionRequestList()) {

						logger.debug("Imported working AR: " + aR.getAcquisititionRequestId());

						/**
						 * The scheduled AR Id
						 */
						String schedARId = ObjectMapper.parseDMToSchedARId(workPR.getUserList().get(0).getUgsId(),
								workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId());

						PRListProcessor.aRSchedIdMap.get(pSId).put(schedARId, (AcquisitionRequest) aR.cloneModel());

						for (DTO dto : aR.getDtoList()) {

							/**
							 * The scheduled DTO Id
							 */
							String schedDTOId = ObjectMapper.parseDMToSchedDTOId(workPR.getUserList().get(0).getUgsId(),
									workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto.getDtoId());

							PRListProcessor.dtoSchedIdMap.get(pSId).put(schedDTOId, (DTO) dto.cloneModel());
						}
					}
				}

				// Initialize the working DTO statuses
				initWorkStatuses(pSId, workPS);

				// Get working DTOs
				for (ProgrammingRequest workPR : (ArrayList<ProgrammingRequest>) ((ArrayList<ProgrammingRequest>) workPRList)
						.clone()) {

					for (AcquisitionRequest aR : workPR.getAcquisitionRequestList()) {

						/**
						 * The scheduled AR Id
						 */
						String schedARId = ObjectMapper.parseDMToSchedARId(workPR.getUserList().get(0).getUgsId(),
								workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId());

						for (DTO dto : aR.getDtoList()) {

							/**
							 * The scheduled DTO Id
							 */
							String schedDTOId = ObjectMapper.parseDMToSchedDTOId(workPR.getUserList().get(0).getUgsId(),
									workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto.getDtoId());

							if (SessionActivator.schedDTOIdStatusMap.get(pSId).containsKey(schedDTOId)
									&& SessionActivator.schedDTOIdStatusMap.get(pSId).get(schedDTOId)
											.equals(DtoStatus.Scheduled)
									&& RequestChecker.isInsideMH(pSId, dto)) {

								if (aR.getEquivalentDTO() != null) {
									
									// Theatre/Experimental case
									if (workPR.getMode().equals(PRMode.Theatre)
											|| (workPR.getMode().equals(PRMode.Experimental))) {

										// Set working Equivalent DTO (Theatre/Exp)
										equivDTOHandler.setWorkTheatreExpEquivDTO(pSId, aR.getEquivalentDTO(),
												workPR.getMode(), workPR.getPitchExtraBIC(), workPR);

									// DI2S cases
									} else if (dto.getSensorMode().equals(DTOSensorMode.SPOTLIGHT_1_MSOR)
											|| dto.getSensorMode().equals(DTOSensorMode.SPOTLIGHT_2_MSOS)
											|| dto.getSensorMode().equals(DTOSensorMode.SPOTLIGHT_2_MSJN)) {

										// The Equivalent DTO Id
										String equivDTOId = aR.getEquivalentDTO().getEquivalentDtoId();
										
										if (aR.getEquivalentDTO().getEquivalentDtoId() != null) {

											// Set default Equivalent DTO Id
											equivDTOId = ObjectMapper.parseDMToEquivDTOId(
													workPR.getUserList().get(0).getUgsId(),
													workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(),
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
												workPR.getPitchExtraBIC());
									}

								} else {

									logger.debug("Following scheduling DTO: " + schedDTOId
											+ " is imported into the working plan.");

									SessionActivator.initSchedDTOListMap.get(pSId).add(ObjectMapper.parseDMToSchedDTO(
											pSId, workPR.getUserList().get(0).getUgsId(),
											workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), dto,
											workPR.getUserList().get(0).getAcquisitionStationIdList(), true));

									SessionActivator.initSchedARIdDTOMap.get(pSId).put(schedARId,
											ObjectMapper.parseDMToSchedDTO(pSId, workPR.getUserList().get(0).getUgsId(),
													workPR.getProgrammingRequestId(), aR.getAcquisititionRequestId(),
													dto, workPR.getUserList().get(0).getAcquisitionStationIdList(),
													true));
								}
							}
						}
					}

					PRListProcessor.pRListMap.get(pSId).add((ProgrammingRequest) workPR.cloneModel());
				}

				logger.info("Added working PRList from Planning Session: " + workPS.getPlanningSessionId());

				if (PRListProcessor.pRListMap.get(pSId).isEmpty()) {

					logger.warn("Empty working PRList from Planning Session: " + workPS.getPlanningSessionId());
				}

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");
			}

		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn("Data relevant to working Planning Session NOT retrieved from DPL.");
		}
	}
	
	/**
	 * Set the data relevant to the reference Planning Sessions from DPL
	 *
	 * @param pSId
	 * @param workPS
	 */
	public boolean getRefPSData(Long pSId,
			com.telespazio.splaif.protobuf.ActivateMessage.Activate.PlanningSession refPS) {

		logger.info("Get data from the reference Planning Session: " + refPS.getPlanningSessionId());

		try {

			if (!Configuration.debugDPLFlag) {

				/**
				 * New planning session service factory
				 */
				PlanningSessionService pSS = new Factory().getPlanningSessionService();

				// Get the relevant PRList for the reference Planning Session
				logger.info("Get the relevant PRList from DPL for the reference Planning Session: "
						+ refPS.getPlanningSessionId());

				/**
				 * The programming requests list of the reference Planning Sessions
				 */
				List<ProgrammingRequest> refPRList = pSS.getProgrammingRequest(refPS.getPlanningSessionId());

				for (ProgrammingRequest refPR : refPRList) {

					// TODO: changed on 08/11/2022
					logger.debug("Imported reference PR: " + refPR.getProgrammingRequestId() + " for UGS Id: "
							+ refPR.getUgsId());

					/**
					 * The scheduling PR Id
					 */
					String schedPRId = ObjectMapper.parseDMToSchedPRId(
							refPR.getUgsId(), refPR.getProgrammingRequestId());
					
					/**
					 * The list of PR Ids
					 */
					ArrayList<String> pRIdList = new ArrayList<String>();

					pRIdList.add(refPR.getProgrammingRequestListId());

					// Update pR Id to PRList Id map
					PRListProcessor.pRToPRListIdMap.get(pSId).put(schedPRId, pRIdList);

					// Add reference PR
					PRListProcessor.pRSchedIdMap.get(pSId).put(schedPRId,
							(ProgrammingRequest) refPR.cloneModel());

					// Add the new PR international boolean to the scheduling map
					PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, false);
					
					// Manage international PR
					if ((refPR.getUserList().size() > 1)
							&& RequestChecker.isDefence(refPR.getUgsId())) {															 
						
						logger.info("Assigned international flag to PR: " + schedPRId);
						PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, true);
					
					} else if (refPR.getDi2sAvailabilityFlag()
							&& RequestChecker.isDefence(refPR.getUgsId())) {
					
						logger.info("Assigned international flag to PR: " + schedPRId);
						PRListProcessor.pRIntBoolMap.get(pSId).put(schedPRId, true);
					}
					
					for (AcquisitionRequest refAR : refPR.getAcquisitionRequestList()) {

						logger.debug("Imported reference AR: " + refAR.getAcquisititionRequestId());

						// Add reference AR
						PRListProcessor.aRSchedIdMap.get(pSId)
								.put(ObjectMapper.parseDMToSchedARId(refPR.getUserList().get(0).getUgsId(),
										refPR.getProgrammingRequestId(), refAR.getAcquisititionRequestId()),
										(AcquisitionRequest) refAR.cloneModel());

						for (DTO refDTO : refAR.getDtoList()) {

							logger.debug("Imported reference DTO: " + refDTO.getDtoId());

							// Add reference DTO
							PRListProcessor.dtoSchedIdMap.get(pSId)
									.put(ObjectMapper.parseDMToSchedDTOId(refPR.getUserList().get(0).getUgsId(),
											refPR.getProgrammingRequestId(), refAR.getAcquisititionRequestId(),
											refDTO.getDtoId()), (DTO) refDTO.cloneModel());
						}
					}
				}

				if (!refPRList.isEmpty()) {

					logger.info("Add reference PRList for Planning Session: " + refPS.getPlanningSessionId());
					PRListProcessor.refPRListMap.get(pSId).addAll(refPRList);
				
				} else {
		
					logger.warn("Empty reference PRList from Planning Session: " + refPS.getPlanningSessionId());
				}

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");
			}

		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn("Data relevant to the reference Planning Session NOT retrieved from DPL.");
		}

		return true;
	}

	/**
	 * Get the data relevant to all the Programming Requests for the Planning Session
	 *
	 * @param pSId
	 */
	public List<ProgrammingRequest> getAllProgrammingRequest(Long pSId) {
	
		// Get the relevant PRList for the working Planning Session
		logger.info("Get complete PRList from DPL for the Planning Session: " + pSId);

		/**
		 * The complete PRList
		 */
		List<ProgrammingRequest> allPRList = new ArrayList<ProgrammingRequest>();
		
		try {

			if (!Configuration.debugDPLFlag) {
		
				/**
				 * New planning session service factory
				 */
				PlanningSessionService pSS = new Factory().getPlanningSessionService();
			
				// Get All Programming Requests
				allPRList = pSS.getAllProgrammingRequest(pSId);
			}
			
		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn("Data relevant to the reference Planning Session NOT retrieved from DPL.");
		}
		return allPRList;
	}
	
	/**
	 * Set initial statuses of the working Planning Session
	 * 
	 * @param pSId
	 * @param workPS
	 */
	public void initWorkStatuses(Long pSId,
			com.telespazio.splaif.protobuf.ActivateMessage.Activate.PlanningSession workPS) {

		try {

			/**
			 * New planning session service factory
			 */
			PlanningSessionService pSS = new Factory().getPlanningSessionService();

			logger.debug("Initialize the statuses of the working Planning Session: " + workPS.getPlanningSessionId());

			if (workPS.hasPlanningSessionId()) {

				if (!Configuration.debugDPLFlag) {

					/**
					 * The working Planning Session
					 */
					PlanningSession workPlanSession = pSS.getPlanningSession(workPS.getPlanningSessionId());

					// Init requests statuses
					for (PlanProgrammingRequestStatus pRStatus : workPlanSession.getProgrammingRequestStatusList()) {

						if (PRListProcessor.workPRSchedIdMap.get(pSId).containsKey(ObjectMapper
								.parseDMToSchedPRId(pRStatus.getUgsId(), pRStatus.getProgrammingRequestId()))) {

							for (PlanAcquisitionRequestStatus aRStatus : pRStatus.getAcquisitionRequestStatusList()) {
								
								for (PlanDtoStatus dtoStatus : aRStatus.getDtoStatusList()) {

									// Add DTO status
									SessionActivator.schedDTOIdStatusMap.get(pSId)
											.put(ObjectMapper.parseDMToSchedDTOId(pRStatus.getUgsId(),
													pRStatus.getProgrammingRequestId(),
													aRStatus.getAcquisitionRequestId(), dtoStatus.getDtoId()),
													dtoStatus.getStatus());
								}
							}
						}
					}
				}
			}

		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.toString(), ex);

			logger.warn("Data relevant to given Planning Session NOT initialized from DPL.");
		}
	}

	/**
	 * Get the satellite state relevant to the Planning Session from DPL // TODO:
	 * check resource start date vs real PDHT data frequency
	 * 
	 * @param pS
	 * @param refTime
	 * @param workPSId
	 * @param refPSId
	 * @return
	 * @throws SPLAException
	 * @throws InvalidProtocolBufferException
	 * @throws IOException
	 * @throws Exception
	 */
	public boolean getSatState(PlanningSession pS, long refTime, long workPSId, long refPSId)
			throws SPLAException, InvalidProtocolBufferException, IOException, Exception {

		logger.info("Initialize satellite resources for Planning Session: " + pS.getPlanningSessionId());

		/**
		 * The initial boolean
		 */
		boolean isInit = false;

		/**
		 * The Planning Session Id
		 */
		Long pSId = pS.getPlanningSessionId();

		/**
		 * The satellite list
		 */
		List<Satellite> satList = new ArrayList<>();

		/**
		 * The acquisition stations list
		 */
		List<AcquisitionStation> acqStationList = new ArrayList<>();

		try {

			if (!Configuration.debugDPLFlag) {

				/**
				 * New resource service factory
				 */
				ResourceService rS = new Factory().getResourceService();

				/**
				 * New catalog service factory
				 */
				CatalogService cS = new it.sistematica.spla.dcm.core.service.Factory().getCatalogService();

				// Get satellite data
				logger.info("Get the satellite data from DPL for MH between: " + pS.getMissionHorizonStartTime()
						+ " and " + pS.getMissionHorizonStopTime());

				// Get satellite data according to: {PAW, visibility, eclipse, resources} time
				// ranges
				satList = rS.getSatelliteData(new Date((pS.getMissionHorizonStartTime().getTime() - 86400000L)),
						pS.getMissionHorizonStopTime(), // PAWs
						new Date(pS.getMissionHorizonStartTime().getTime() - 86400000L),
						new Date(pS.getMissionHorizonStopTime().getTime() + 86400000L), // visibilities
						new Date(pS.getMissionHorizonStartTime().getTime() - 86400000L), pS.getMissionHorizonStopTime(), // eclipses
						new Date(pS.getMissionHorizonStartTime().getTime() - 86400000L),
						pS.getMissionHorizonStopTime()); // resources

				// Get satellite PDHT
				logger.info("Get reference PDHT resources from DPL for MH between: " + pS.getMissionHorizonStartTime()
						+ " and " + pS.getMissionHorizonStopTime());

				if (refPSId > 0) {

					for (Satellite sat : satList) {

						// Get satellite PDHT resources
						sat = getSatPDHTResources(pSId, rS, sat, refPSId);
					}

				} else if (SessionActivator.mhPSIdListMap.get(pSId).size() > 1) {

					for (Satellite sat : satList) {

						// Get satellite PDHT resources
						sat = getSatPDHTResources(pSId, rS, sat, SessionActivator.mhPSIdListMap.get(pSId).get(0));
					}
				}

				if (satList.isEmpty()) {

					logger.warn("Empty satellite data found for Planning Session: " + pSId);

					logger.info("Get Catalog satellites data.");

					// Get catalog satellite instead
					for (CatalogSatellite catSat : cS.getCatalogSatelliteList()) {

						satList.add(new Satellite(catSat));
					}
				}

				/**
				 * The satellite counter
				 */
				int satCount = 0;

				for (Satellite sat : satList) {

					if (sat.getPdht() == null || sat.getPdht().getMm1() == null || sat.getPdht().getMm1().isEmpty()
							|| sat.getPdht().getMm1().get(0) == null) {

						logger.debug("Empty PDHT found for satellite: " + sat.getCatalogSatellite().getSatelliteId()
								+ " for Planning Session: " + pSId);

						// Get catalog PDHT instead
						getCatalogPDHT(pS, cS, sat, satCount);

					} else {

						logger.debug("PDHT resources correctly retrieved.");

						// Compute the PDHT state
						computePDHTStatus(pSId, sat);
					}

					logger.debug("PDHT resources finally updated.");

					if (sat.getAttitude().isEmpty()) { // TODO: check for isEstimated = false

						logger.info("Get the reference attitude resources from DPL between: "
								+ pS.getMissionHorizonStartTime() + " and " + pS.getMissionHorizonStopTime());

						if (refPSId > 0) {

							// Get satellite attitude
							sat.setAttitude(rS.getSatelliteResources(refPSId).get(0).getAttitude());

						} else if (SessionActivator.mhPSIdListMap.get(pSId).size() > 1) {

							// Get satellite attitude
							sat.setAttitude(rS.getSatelliteResources(SessionActivator.mhPSIdListMap.get(pSId).get(0))
									.get(satCount).getAttitude());
						}

						if (sat.getAttitude().isEmpty()) {

							logger.warn("Empty Attitude data found for satellite "
									+ sat.getCatalogSatellite().getSatelliteId() + " for Planning Session: " + pSId);

							// Get catalog attitude
							getCatalogAttitude(pS, sat);
						}

					} else {

						logger.debug("Attitude resources correctly retrieved from satellites data.");
					}

					satCount++;
				}

				logger.info("Get the acquisition stations from DPL between: " + pS.getMissionHorizonStartTime()
						+ " and " + pS.getMissionHorizonStopTime());

				// Get acquisition stations data
				acqStationList = rS.getAcquisitionStation(pS.getMissionHorizonStartTime(),
						pS.getMissionHorizonStopTime());

				for (AcquisitionStation acqStation : acqStationList) {

					for (ResourceStatus unavWin : acqStation.getUnavailabilityWindows()) {

						/**
						 * The reference dates
						 */
						Date unavStartTime = unavWin.getReferenceStartTime();
						Date unavStopTime = unavWin.getReferenceStopTime();

						if (unavStopTime == null) {

							unavStopTime = new Date((long) Double.POSITIVE_INFINITY);
						}

						logger.debug("Following Unavailability Window detected from: " + unavStartTime + " to: "
								+ unavStopTime + " for Acquisition Station: "
								+ acqStation.getCatalogAcquisitionStation().getAcquisitionStationId());
					}
				}

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");

				logger.debug("A default satellite is built to support the debug mode.");
				satList.add(DefaultDebugger.getDefaultSatellite(pS, refTime, "SSAR1"));
				satList.add(DefaultDebugger.getDefaultSatellite(pS, refTime, "SSAR2"));

				logger.debug("A default acquisition station list is built to support the debug mode.");
				acqStationList = DefaultDebugger.getDefaultAcqStationList(pSId);
			}

			// Add satellite list
			SessionScheduler.satListMap.get(pSId).addAll(satList);

			int i = 0;

			for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {

				logger.debug("Satellite Id: " + sat.getCatalogSatellite().getSatelliteId());
				logger.debug("Satellite Pdht: " + sat.getPdht());
				logger.debug("Satellite Attitude: " + sat.getAttitude());

				// Filter and get satellite visibilities
				SessionScheduler.satListMap.get(pSId).set(i, filterSatVisList(pSId, sat, acqStationList));



				// Sort visibilities by start time
				Collections.sort(sat.getVisibilityList(), new VisTimeComparator());
							
				if (sat.getVisibilityList() != null) {

					logger.debug("Available satellite Visibilities size: " + sat.getVisibilityList().size());

					for (int j = 0; j < sat.getVisibilityList().size(); j++) {

						logger.trace("Visibility: " + sat.getVisibilityList().get(j));
					}
				}

				// Get available PAWs
				if (sat.getPlatformActivityWindowList() != null) {

					logger.debug("Available satellite PAW size: " + sat.getPlatformActivityWindowList().size());

					for (int j = 0; j < sat.getPlatformActivityWindowList().size(); j++) {

						logger.debug("PAW: " + sat.getPlatformActivityWindowList().get(j));
					}
				}

				// Get eclipses
				if (sat.getEclipseList() != null) {

					logger.debug("Available satellite Eclipse size: " + sat.getEclipseList().size());

					for (int j = 0; j < sat.getEclipseList().size(); j++) {

						logger.debug("Eclipse: " + sat.getEclipseList().get(j));
					}
				}

				i++;
			}

			isInit = true;

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());

			logger.warn("Data relevant to satellite resources NOT got from DPL.");
		}

		return isInit;
	}

	/**
	 * Get the Catalog PDHT
	 * 
	 * @param pS
	 * @param sat
	 * @param
	 * @throws SPLAException
	 */
	private void getCatalogPDHT(PlanningSession pS, CatalogService cS, Satellite sat, int satCount)
			throws SPLAException {

		logger.info("Get Catalog PDHT data for satellite: " + sat.getCatalogSatellite().getSatelliteId());

		// Update Memory Modules

		// Set Memory Module
		sat.getPdht().getMm1().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm1()));
		// Set Memory Module
		sat.getPdht().getMm2().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm2()));
		// Set Memory Module
		sat.getPdht().getMm3().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm3()));
		// Set Memory Module
		sat.getPdht().getMm4().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm4()));
		// Set Memory Module
		sat.getPdht().getMm5().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm5()));
		// Set Memory Module
		sat.getPdht().getMm6().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				cS.getCatalogSatelliteList().get(satCount).getPdht().getMm6()));
	}

	/**
	 * Get satellite PDHT resources
	 * 
	 * @param pSId
	 * @param rS
	 * @param sat
	 * @param relPSId
	 * @throws SPLAException
	 */
	private Satellite getSatPDHTResources(Long pSId, ResourceService rS, Satellite sat, long relPSId)
			throws SPLAException {

		logger.info("Get PDHT from satellite resources relevant to Planning Session: " + relPSId);

		// Clear Memory Modules
		sat.getPdht().getMm1().clear();
		sat.getPdht().getMm2().clear();
		sat.getPdht().getMm3().clear();
		sat.getPdht().getMm4().clear();
		sat.getPdht().getMm5().clear();
		sat.getPdht().getMm6().clear();

		/**
		 * The resource PDHT
		 */
		Pdht pdht = new Pdht();

		if (sat.getCatalogSatellite().getSatelliteId().contains("SAR1")) {

			// Get PDHT
			pdht = rS.getSatelliteResources(relPSId).get(0).getPdht();

		} else {

			// Get PDHT
			pdht = rS.getSatelliteResources(relPSId).get(1).getPdht();
		}

		if (!pdht.getMm1().isEmpty()) {

			// Update Memory Module
			sat.getPdht().getMm1().add(new ResourceValue(pdht.getMm1().get(0).getReferenceTime(),
					pdht.getMm1().get(0).getValue(), true, pSId));
			// Update Memory Module
			sat.getPdht().getMm2().add(new ResourceValue(pdht.getMm2().get(0).getReferenceTime(),
					pdht.getMm2().get(0).getValue(), true, pSId));
			// Update Memory Module
			sat.getPdht().getMm3().add(new ResourceValue(pdht.getMm3().get(0).getReferenceTime(),
					pdht.getMm3().get(0).getValue(), true, pSId));
			// Update Memory Module
			sat.getPdht().getMm4().add(new ResourceValue(pdht.getMm4().get(0).getReferenceTime(),
					pdht.getMm4().get(0).getValue(), true, pSId));
			// Update Memory Module
			sat.getPdht().getMm5().add(new ResourceValue(pdht.getMm5().get(0).getReferenceTime(),
					pdht.getMm5().get(0).getValue(), true, pSId));
			// Update Memory Module
			sat.getPdht().getMm6().add(new ResourceValue(pdht.getMm6().get(0).getReferenceTime(),
					pdht.getMm6().get(0).getValue(), true, pSId));
		} else {

			logger.warn("Empty PDHT resources for satellite: " + sat.getCatalogSatellite().getSatelliteId());
		}

		return sat;
	}

	/**
	 * Get carrier resources as: 
	 * getCarrierPrepTimeOn and getCarrierPrepTimeOff times (ms)
	 * 
	 * @param pSId
	 * @throws SPLAException
	 */
	public static Long[] getCarrierResources(Long pSId) throws SPLAException {

		/**
		 * New catalog service factory
		 */
		ConfigService cS = new it.sistematica.spla.dcm.core.service.Factory().getConfigService();

		logger.info("Get Carrier preparation times.");

		/**
		 * The carrier preparation times
		 */		
		 Long[] prepTimes = {cS.getCarrierPrepTimeOn(), cS.getCarrierPrepTimeOff()};

		if (prepTimes[0] == null || prepTimes[1] == null) {

			logger.warn("No Carrier preparation times are retrieved for Planning Session: " + pSId);

			prepTimes[0] = 0L;
			prepTimes[1] = 0L;
		}

		return prepTimes;
	}
	
	/**
	 * Get maneuver resources
	 * 
	 * @param pSId
	 * @throws SPLAException
	 */
	public static Long[] getManResources(Long pSId) throws SPLAException {

		/**
		 * New catalog service factory
		 */
		ConfigService cS = new it.sistematica.spla.dcm.core.service.Factory().getConfigService();

		logger.info("Get Maneuver execution times.");

		/**
		 * The maneuver times
		 */
		Long[] manTimes = { cS.getTimeForManeuverRw(), cS.getTimeForManeuverCmga() };

		if (manTimes[0] == null || manTimes[1] == null) {

			logger.warn("No Maneuver execution times are retrieved for Planning Session: " + pSId);

			manTimes[0] = 0L;
			manTimes[1] = 0L;
		}

		return manTimes;
	}

	/**
	 * Compute the MM statuses according to the real PDHT data // TODO: check if
	 * working tasks are impacted // TODO: call refined BRM function once ready
	 * 
	 * @param sat
	 * @param pdht
	 * @throws InputException
	 */
	public Satellite computePDHTStatus(Long pSId, Satellite sat) throws InputException {

		logger.info("Compute satellite Memory Module statuses according to the estimated flag");

		/**
		 * The PDHT
		 */
		Pdht pdht = sat.getPdht();

		if (pdht.getMm1().get(0).isEstimated()) { // Hp: all the MM have same flag and date

			logger.info("Satellite " + sat.getCatalogSatellite().getSatelliteId() + " has estimated PDHT resources: "
					+ pdht.toString());

		} else if (!pdht.getMm1().isEmpty()) {

			logger.info("Satellite " + sat.getCatalogSatellite().getSatelliteId() + " has real PDHT resources: "
					+ pdht.toString());

			/**
			 * The reference date
			 */
			Date refDate = pdht.getMm1().get(0).getReferenceTime();

			logger.info("Compute the PDHT update according to the real PDHT status at reference date: " + refDate);

			/**
			 * The reference MM list
			 */
			Double[] refMMs = { pdht.getMm1().get(0).getValue().doubleValue(),
					pdht.getMm2().get(0).getValue().doubleValue(), pdht.getMm3().get(0).getValue().doubleValue(),
					pdht.getMm4().get(0).getValue().doubleValue(), pdht.getMm5().get(0).getValue().doubleValue(),
					pdht.getMm6().get(0).getValue().doubleValue() };

			logger.debug("Get the reference tasks for the current Mission Horizon.");

			/**
			 * The PDHT task list
			 */
			ArrayList<Task> pdhtTaskList = new ArrayList<Task>();

			for (Task refTask : refTaskListMap.get(pSId)) {

				// Update PDHT
				if (refTask.getStartTime().after(refDate)
						&& refTask.getSatelliteId().equals(sat.getCatalogSatellite().getSatelliteId())
						&& !refTask.getTaskStatus().equals(TaskStatus.Error)) {

					pdhtTaskList.add(refTask);

					if (refTask.getTaskType().equals(TaskType.STORE)) {

						if (!((Store) refTask).getMemoryModules().isEmpty()) {

							// Update Memory Modules
							refMMs[0] -= ((Store) refTask).getMemoryModules().get(0) * 4 * Math.pow(1024, 2);
							refMMs[1] -= ((Store) refTask).getMemoryModules().get(1) * 4 * Math.pow(1024, 2);
							refMMs[2] -= ((Store) refTask).getMemoryModules().get(2) * 4 * Math.pow(1024, 2);
							refMMs[3] -= ((Store) refTask).getMemoryModules().get(3) * 4 * Math.pow(1024, 2);
							refMMs[4] -= ((Store) refTask).getMemoryModules().get(4) * 4 * Math.pow(1024, 2);
							refMMs[5] -= ((Store) refTask).getMemoryModules().get(5) * 4 * Math.pow(1024, 2);
						}

					} else if (refTask.getTaskType().equals(TaskType.DWL)) {

						if (!((Download) refTask).getMemoryModules().isEmpty()) {

							// Update Memory Modules
							refMMs[0] += ((Download) refTask).getMemoryModules().get(0) * 4 * Math.pow(1024, 2);
							refMMs[1] += ((Download) refTask).getMemoryModules().get(1) * 4 * Math.pow(1024, 2);
							refMMs[2] += ((Download) refTask).getMemoryModules().get(2) * 4 * Math.pow(1024, 2);
							refMMs[3] += ((Download) refTask).getMemoryModules().get(3) * 4 * Math.pow(1024, 2);
							refMMs[4] += ((Download) refTask).getMemoryModules().get(4) * 4 * Math.pow(1024, 2);
							refMMs[5] += ((Download) refTask).getMemoryModules().get(5) * 4 * Math.pow(1024, 2);

						}

					} else if (refTask.getTaskType().equals(TaskType.BITE)) {

						// TODO: refine values!
						try {

							// Update Memory Modules
							if (Integer.parseInt(((BITE) refTask).getModuleId()) < 6) {

								refMMs[Integer.parseInt(((BITE) refTask).getModuleId())] = 8191 * 4 * Math.pow(1024, 2);

							} else {

								refMMs[0] = 8191 * 4 * Math.pow(1024, 2);
								refMMs[1] = 8191 * 4 * Math.pow(1024, 2);
								refMMs[2] = 8191 * 4 * Math.pow(1024, 2);
								refMMs[3] = 8191 * 4 * Math.pow(1024, 2);
								refMMs[4] = 8191 * 4 * Math.pow(1024, 2);
								refMMs[5] = 8191 * 4 * Math.pow(1024, 2);
							}

						} catch (Exception ex) {

							logger.warn("Not parsable BITE memory modules for Task: " + refTask.getTaskId());
						}
					}
				}
			}

			// Clear Memory Modules
			sat.getPdht().getMm1().clear();
			sat.getPdht().getMm2().clear();
			sat.getPdht().getMm3().clear();
			sat.getPdht().getMm4().clear();
			sat.getPdht().getMm5().clear();
			sat.getPdht().getMm6().clear();

			// Update Memory Modules
			sat.getPdht().getMm1().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[0]), true, pSId));
			sat.getPdht().getMm2().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[1]), true, pSId));
			sat.getPdht().getMm3().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[2]), true, pSId));
			sat.getPdht().getMm4().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[3]), true, pSId));
			sat.getPdht().getMm5().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[4]), true, pSId));
			sat.getPdht().getMm6().add(new ResourceValue(refDate, BigDecimal.valueOf(refMMs[5]), true, pSId));

		} else {

			logger.warn("Empty PDHT for satellite: " + sat.getCatalogSatellite().getSatelliteId());
		}

		return sat;
	}

	/**
	 * Get the Catalog Attitude
	 * 
	 * @param pS
	 * @param sat
	 */
	private void getCatalogAttitude(PlanningSession pS, Satellite sat) {

		logger.info("Get Catalog Attitude for satellite: " + sat.getCatalogSatellite().getSatelliteId());

		// Add satellite attitude
		sat.getAttitude().add(new ResourceValue(pS.getMissionHorizonStartTime(),
				sat.getCatalogSatellite().getAttitude(), true, pS.getPlanningSessionId()));
	}

	/**
	 * Filter the satellite visibilities according to the unavailability windows
	 * 
	 * @param pSId
	 * @param sat
	 * @param acqStationList
	 * @throws SPLAException 
	 */						   
	private Satellite filterSatVisList(Long pSId, Satellite sat, List<AcquisitionStation> acqStationList) throws SPLAException {

		/***
		 * Instance handlers
		 */
		TUPCalculator tupCalculator = new TUPCalculator();

		if (sat.getVisibilityList() != null) {

			// Filter satellite visibilities wrt acquisition stations unavailability windows
			logger.debug("Filter visibilities wrt relevant unavailability windows for satellite: "
					+ sat.getCatalogSatellite().getSatelliteId());

			for (int k = 0; k  < sat.getVisibilityList().size(); k++) {
				
				/**
				 * The visibility
				 */
				Visibility vis = sat.getVisibilityList().get(k);
				
				// The AcquisitionStation relevant to the visibility
				AcquisitionStation visAcqStat = null;
				
				for (AcquisitionStation acqStat : acqStationList) {
					
					if (acqStat.getCatalogAcquisitionStation().getAcquisitionStationId().equals(
							vis.getAcquisitionStationId())) {

						visAcqStat = acqStat;
					}
				}
				
				// Check visibility availability
				if (!isVisAvailable(vis, acqStationList) 						
						// Added on 20/01/2021 for S-TUP unavailabilities
						|| !tupCalculator.isAvailableTUPVis(pSId, vis, visAcqStat)) {

					logger.debug("The following visibility is NOT available for satellite "
							+ sat.getCatalogSatellite().getSatelliteId() + " between: " + vis.getVisibilityStartTime()
							+ " and " + vis.getVisibilityStopTime() + ", for the X-Band status " + vis.isXbandFlag()
							+ ", for the station " + vis.getAcquisitionStationId() + " with look side: " + vis.getLookSide());

					sat.getVisibilityList().remove(k);
					
					k --;
				
				} else {

					logger.debug("The following visibility is available for satellite "
							+ sat.getCatalogSatellite().getSatelliteId() + " between: " + vis.getVisibilityStartTime()
							+ " and " + vis.getVisibilityStopTime() + ", for the X-Band status " + vis.isXbandFlag()
							+ ", for the station " + vis.getAcquisitionStationId() + " for contact counter " + vis.getContactCounter()
							+ " and look side " + vis.getLookSide());
				}
			}
		}

		return sat;
	}

	/**
	 * Check the visibility wrt NotOperative Unavailabilities
	 * 
	 * @param vis
	 * @param acqStationList
	 * @param stationId
	 * @return
	 */
	private boolean isVisAvailable(Visibility vis, List<AcquisitionStation> acqStationList) {

		/**
		 * The visibility availability
		 */
		boolean visAvail = true;

		if (vis.isAllocated()) {

			for (AcquisitionStation acqStation : acqStationList) {

				// The acquisition station Id
				String acqStationId = acqStation.getCatalogAcquisitionStation().getAcquisitionStationId();
				
				// Check availability
				if (acqStation.getCatalogAcquisitionStation().getAcquisitionStationId()
						.equals(vis.getAcquisitionStationId()) && !acqStation.getUnavailabilityWindows().isEmpty()) {

					for (ResourceStatus unavWin : acqStation.getUnavailabilityWindows()) {

						if (unavWin.getStatus().equals(ResourceStatusType.NotOperative)) {

							if (!checkVisWithUnavTime(vis, unavWin, acqStationId)) {

								logger.debug("Visibility NOT available for unavailability check.");
								
								// Unset availability
								visAvail = false;

								break;
							}
						}
					}
				}
			}

		} else {

			// Unset availability
			visAvail = false;
		}

		return visAvail;
	}
		
	/** 
	 * Check the visibility wrt a given unavailability time window
	 * 
	 * @param vis
	 * @param unavWin
	 * @param acqStationId
	 * @return
	 */
	private boolean checkVisWithUnavTime(Visibility vis, ResourceStatus unavWin, String acqStationId) {

		/**
		 * The availability boolean
		 */
		boolean isAvail = true;

		/**
		 * The reference dates
		 */
		Date unavStartTime = unavWin.getReferenceStartTime();
		Date unavStopTime = unavWin.getReferenceStopTime();

		if (unavStopTime == null) {

			unavStopTime = new Date((long) Double.POSITIVE_INFINITY);
		}

		if (vis.getAcquisitionStationId().equals(acqStationId)
				&& ((vis.getVisibilityStartTime().getTime() >= unavStartTime.getTime()
				&& vis.getVisibilityStartTime().getTime() <= unavStopTime.getTime())
				|| (vis.getVisibilityStopTime().getTime() >= unavStartTime.getTime()
						&& vis.getVisibilityStopTime().getTime() <= unavStopTime.getTime()))) {

			isAvail = false;
		}

		return isAvail;
	}
	
	/**
	 * Set Tasks relevant to the reference (previous MH) and working (current MH)
	 * Planning Session from DPL
	 * // Updated on 27/05/2022 for the management of Partially Inside MH Tasks
	 * @param pSId
	 *            - the Planning Session Id
	 * @param workPS
	 *            - the working Planning Session
	 * @param refPSList
	 *            - the reference Planning Session list
	 * @param refTime
	 *            - the reference time
	 * @throws SPLAException
	 */
	@SuppressWarnings("unchecked")
	public boolean setPlanSessionTasks(Long pSId,
			com.telespazio.splaif.protobuf.ActivateMessage.Activate.PlanningSession workPS,
			List<com.telespazio.splaif.protobuf.ActivateMessage.Activate.PlanningSession> refPSList, double refTime)
			throws SPLAException {

		/**
		 * The initial boolean
		 */
		boolean isInit = false;

		try {

			if (!Configuration.debugDPLFlag) {

				logger.info("Get the initial list of Tasks relevant to the Planning Session: " + pSId);

				/**
				 * The new planning session service
				 */
				PlanningSessionService pSS = new Factory().getPlanningSessionService();

				/**
				 * The list of working tasks
				 */
				ArrayList<Task> workTaskList = new ArrayList<>();

				// Set weighted rank
				if (workPS.hasPlanningSessionId()) {

					logger.info("Get applicable tasks from DPL for the working Planning Session: "
							+ workPS.getPlanningSessionId());
					workTaskList.addAll(pSS.getTask(workPS.getPlanningSessionId()));

					for (int i = 0; i < workTaskList.size(); i++) {

						if (workTaskList.get(i).getTaskType().equals(TaskType.ACQ)) {

							logger.trace("Set rank for working task: " + workTaskList.get(i).getTaskId());
							setAcqWeightedRank(pSId, (Acquisition) workTaskList.get(i));
						}

						workTaskList.get(i).setRemovableFlag(true);

						if (!RequestChecker.isPartialInsideMH(pSId, workTaskList.get(i))) {

							workTaskList.remove(i);

							i--;
						}
					}

					// Sort reference tasks
					Collections.sort(workTaskList, new TaskStartTimeComparator());
					
					// TODO: check link owner Ids to the AR Ids in the
					// ownerARIdMap for the incoming tasks

					logger.info("A number of " + workTaskList.size() + " applicable tasks from "
							+ "working Planning Session " + workPS.getPlanningSessionId() + " is imported.");

					// Add working tasks list
					workTaskListMap.get(pSId).addAll((ArrayList<Task>) workTaskList.clone());
				}

				// TODO: changed on 21/12/2019
				for (int i = 0; i < refPSList.size(); i++) {

					logger.info("Get applicable tasks from DPL for the reference Planning Session: "
							+ refPSList.get(i).getPlanningSessionId());

					/**
					 * The list of reference tasks
					 */
					ArrayList<Task> refTaskList = (ArrayList<Task>) pSS
							.getTask(refPSList.get(i).getPlanningSessionId());

					// Set weighted rank
					for (int j = 0; j < refTaskList.size(); j ++) {

						if (refTaskList.get(j).getTaskType().equals(TaskType.ACQ)) {

							setAcqWeightedRank(pSId, (Acquisition) refTaskList.get(j));
						}

						refTaskList.get(j).setRemovableFlag(false);

						refTaskList.get(j).getMacroActivityList().clear();
						
						if (refTaskList.get(j).getTaskMark().equals(TaskMarkType.DELETED)) {
							
							refTaskList.remove(j);
							
							j --;
						}
					}

					// Sort reference tasks
					Collections.sort(refTaskList, new TaskStartTimeComparator());

					logger.info("A number of " + refTaskList.size() + " applicable tasks from "
							+ "reference Planning Session " + refPSList.get(i).getPlanningSessionId()
							+ " is imported.");

					if (!refTaskList.isEmpty()) {

						// Add reference task list
						refTaskListMap.get(pSId).addAll((ArrayList<Task>) refTaskList.clone());

						refPSTaskListMap.get(pSId).put(refPSList.get(i).getPlanningSessionId(),
								(ArrayList<Task>) refTaskList.clone());
					}
				}

				if (!refTaskListMap.get(pSId).isEmpty()) {

					// Sort all reference tasks by time
					Collections.sort(refTaskListMap.get(pSId), new TaskStartTimeComparator());

					// Adjust MH start time
					adjustMHStartTime(pSId, refTaskListMap.get(pSId));
				}

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");
			}

			isInit = true;

		} catch (Exception ex) {

			logger.warn("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn(
					"Some Tasks relevant to working or reference Planning Sessions " + "are NOT retrieved from DPL.");
		}

		return isInit;
	}

	/**
	 * Set acquisition weighted rank
	 *
	 * @param pSId
	 * @param acq
	 *
	 */
	private void setAcqWeightedRank(Long pSId, Acquisition acq) {

		logger.trace("Set the weighted rank for reference acquisition: " + acq.getTaskId());

		/**
		 * The scheduling AR Id
		 */
		String schedARId = ObjectMapper.parseDMToSchedARId(acq.getUgsId(), acq.getProgrammingRequestId(),
				acq.getAcquisitionRequestId());

		PRListProcessor.schedARIdRankMap.get(pSId).put(schedARId, acq.getWeightedRank().intValue());
	}

	/**
	 * Adjust mission Horizon start time according to the tasks of the previous MH
	 * 
	 * @param pSId
	 * @param refTaskList
	 * @throws InputException
	 */
	@SuppressWarnings("unchecked")
	private void adjustMHStartTime(Long pSId, ArrayList<Task> refTaskList) throws InputException {

		logger.info("Adjust MH start time according to the previous one.");

		/**
		 * The list of Tasks of the previous MH
		 */
		ArrayList<Task> prevMHTaskList = (ArrayList<Task>) refTaskList.clone();

		for (int j = 0; j < prevMHTaskList.size(); j++) {

			prevMHTaskList.get(j).setRemovableFlag(false);

			if (prevMHTaskList.get(j).getStartTime()
					.compareTo(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) > 0
					|| prevMHTaskList.get(j).getTaskType().equals(TaskType.DLO)) {

				prevMHTaskList.remove(j);

				j--;

			}
		}

		/**
		 * The stop time of the last task of the reference Planning Sessions
		 */
		Date lastRefTaskTime = refTaskList.get(refTaskList.size() - 1).getStopTime();

		// Adjust MH start time in case the @lastRefTaskTime is higher than MH start
		// time
		if (lastRefTaskTime.compareTo(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) > 0
			&& lastRefTaskTime.getTime() 
				- SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime() < Configuration.deltaTime) {
			
			// Update the updated Planning Session data
			logger.debug("Initialize the updated Planning Session data.");
			PlanningSession updPS = new PlanningSession(pSId,
					SessionActivator.planSessionMap.get(pSId).getPlanningSessionType());

			// Set planning session data
			updPS.setStatus(SessionActivator.planSessionMap.get(pSId).getStatus());
			updPS.setMissionHorizonStartTime(lastRefTaskTime);
			updPS.setMissionHorizonStopTime(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStopTime());

			// Update Planning Session
			SessionActivator.planSessionMap.put(pSId, updPS);
		}
	}

	/**
	 * Get the list of equivalentDTOs for the reference Planning Session
	 * 
	 * @param pSId
	 * @return
	 */
	public static ArrayList<EquivalentDTO> getRefEquivDTOList(Long pSId) {

		logger.info("Get Equivalent DTOs from reference Planning Sessions.");

		/**
		 * The list of equivalent DTOs
		 */
		ArrayList<EquivalentDTO> equivDTOList = new ArrayList<EquivalentDTO>();

		for (ProgrammingRequest refPR : PRListProcessor.refPRListMap.get(pSId)) {

			// Check reference PR
			if (refPR.getMode().equals(PRMode.Theatre) 
					|| refPR.getMode().equals(PRMode.Experimental)
					|| refPR.getMode().equals(PRMode.DI2S)) {  // TODO: erased getDi2sAvailabilityFlag()

				for (AcquisitionRequest refAR : refPR.getAcquisitionRequestList()) {

					if (refAR.getEquivalentDTO() != null) {

						logger.info("Following Equivalent DTO: " + refAR.getEquivalentDTO().getEquivalentDtoId()
								+ " is inserted into the reference plan.");

						// Add equivalent DTO
						equivDTOList.add(refAR.getEquivalentDTO());

						if (refAR.getEquivalentDTO().getTaskList() != null
								&& ! refAR.getEquivalentDTO().getTaskList().isEmpty()) {

//							for (Task task : refAR.getEquivalentDTO().getTaskList()) {

							/**
							 * The first maneuver task
							 */
							Task task = refAR.getEquivalentDTO().getTaskList().get(0);

							logger.debug("Added reference Equivalent DTO: " 
							+ refAR.getEquivalentDTO().getEquivalentDtoId());
						
							// Get reference Equivalent DTO Id
							PRListProcessor.equivStartTimeIdMap.get(pSId).put(
									Long.toString(task.getStartTime().getTime()),
									refAR.getEquivalentDTO().getEquivalentDtoId());

							// Add Equivalent Maneuver
							PRListProcessor.equivStartTimeManMap.get(pSId).put(
									Long.toString(task.getStartTime().getTime()),
									 (Maneuver) task);
							
							// Get reference Equivalent scheduling AR Id 
							PRListProcessor.equivIdSchedARIdMap.get(pSId).put(										   
									refAR.getEquivalentDTO().getEquivalentDtoId(),
									ObjectMapper.parseDMToSchedARId(refPR.getUgsId(), 
											refPR.getProgrammingRequestId(), 
											refAR.getAcquisititionRequestId()));

						} else {

							logger.debug("Added reference Equivalent DTO: " 
							+ refAR.getEquivalentDTO().getEquivalentDtoId());
							
							// Get reference Equivalent scheduling AR Id 
							PRListProcessor.equivIdSchedARIdMap.get(pSId).put(
									refAR.getEquivalentDTO().getEquivalentDtoId(),
									ObjectMapper.parseDMToSchedARId(refPR.getUgsId(), 
											refPR.getProgrammingRequestId(), 
											refAR.getAcquisititionRequestId()));		  
						}
					}
				}
			}
		}

		return equivDTOList;
	}
	
	/**
	 * Persist resource values of the planning session
	 *
	 * @param pSId
	 */
	public boolean persistResources(Long pSId) throws Exception {
	
		logger.info("Persist Resources for Planning Session: " + pSId);
		
		/**
		 * The output boolean
		 */
		boolean isSaved = false;
	
		try {
	
			if (!Configuration.debugDPLFlag) {
	
				// TODO: waitSCMFlag Added on 26/11
				if (!FilterDTOHandler.isWaitFiltResultMap.get(pSId)) {
	
					// Save owners transactions
					isSaved = saveOwnersData(pSId);
	
					// Save satellite resources
					isSaved = saveSatResources(pSId);
				
				} else if (!Configuration.waitSCMFlag) {
	
					logger.warn("Expected Filtering Result not achievable for Planning Session: " + pSId);
					logger.info("Anyway SCM Waiting flag is false, resources are going to be saved.");
					
					// Save owners transactions
					isSaved = saveOwnersData(pSId);
	
					// Save satellite resources
					isSaved = saveSatResources(pSId);
	
				} else {
	
					logger.warn("Expected Filtering Result not achievable for Planning Session: " + pSId);
					logger.info("SCM Waiting flag is true, resources are NOT going to be saved.");
				
					/**
					 * Instance handler
					 */
					RulesPerformer rulesPerformer = new RulesPerformer();
					
					// Purge scheduled tasks for Planning Session
					rulesPerformer.purgeSchedTasks(pSId);
				}
	
			} else {
	
				logger.warn("No DPL methods are invoked due to CSPS debug mode.");
				// persistPerformer.reportOwnerTransactions(pSId);
			}
	
		} catch (SPLAException e) {
	
			logger.error("Error saving resources {} - {}", pSId, e.getMessage(), e);
	
			isSaved = false;
	
		} finally {
	
			logger.info("SaveResourceValue processing ended.");
	
		}
	
		return isSaved;
	}

	/**
	 * Save Planning Session Info
	 *
	 * @param pSId
	 * @return
	 * @throws Exception
	 */
	public boolean persistSession(Long pSId) throws Exception {
	
		logger.info("Persist Session Info for Planning Session: " + pSId);
		
		/**
		 * Instance handler
		 */
		SessionScheduler sessionScheduler = new SessionScheduler();

		RulesPerformer rulesPerformer = new RulesPerformer();
	
		/**
		 * The saved boolean
		 */
		boolean isSaved = false;
	
		try {
	
			// Finalize schedule
			sessionScheduler.finalizeSchedule(pSId);
	
			// TODO: waitSCMFlag Added on 26/11
			if (!FilterDTOHandler.isWaitFiltResultMap.get(pSId)) {
	
				// Save Planning Session Info
				isSaved = saveSessionInfo(pSId);
			
			} else if (!Configuration.waitSCMFlag) {
	
				logger.warn("Expected Filtering Result not achievable for Planning Session: " + pSId);
				logger.info("Anyway SCM Waiting flag is false, Planning Session is going to be saved.");		
					
				// Save Planning Session Info
				isSaved = saveSessionInfo(pSId);
	
			} else {
	
				logger.warn("Expected Filtering Result not achievable for Planning Session: " + pSId);
				logger.info("SCM Waiting flag is true, resources are NOT going to be saved.");
						
				// Purge scheduled tasks for Planning Session
				rulesPerformer.purgeSchedTasks(pSId);
			}
	
		} catch (Exception e) {
	
			logger.error("Error saving session {} - {}", pSId, e.getMessage(), e);
	
		} finally {
	
			logger.info("SaveSessionInfo processing ended.");
		}
	
		return isSaved;
	}
	
	/**
	 * Get the data relevant to the scheduling owners from DPL
	 *
	 * // TODO: check number of transactions // TODO: TBD about DPL return for
	 * transactions within multiple sessions // TODO: check NEO BIC initialization
	 * // TODO: check BIC saving for multiple sessions in negotiation
	 *
	 * @param pS
	 * @param workPSId
	 * @param refPSId
	 * @throws SPLAException
	 * @throws ParseException
	 */
	public boolean getOwnersData(PlanningSession pS, Long workPSId, Long refPSId) throws SPLAException, ParseException {

		/**
		 * The Planning Session Id
		 */
		Long pSId = pS.getPlanningSessionId();

		/**
		 * new resource service factory
		 */
		ResourceService rS = new Factory().getResourceService();

		/**
		 * new catalog service factory
		 */
		CatalogService cS = new it.sistematica.spla.dcm.core.service.Factory().getCatalogService();

		/**
		 * The owners list
		 */
		List<Owner> ownerList = new ArrayList<>();

		/**
		 * The initial boolean
		 */
		boolean isInit = false;

		try {

			if (!Configuration.debugDPLFlag) {

				// Get catalog owner list
				logger.info("Get the owners data from DPL between: " + pS.getMissionHorizonStartTime() + " and "
						+ pS.getMissionHorizonStopTime());

				if (workPSId != null && workPSId > 0) {

					ownerList = rS.getOwnerListByPlanningSession(workPSId);

					if (ownerList.isEmpty()) {

						ownerList = rS.getOwnerList(pS.getMissionHorizonStartTime(), pS.getMissionHorizonStopTime());
					}

				} else {

					ownerList = rS.getOwnerList(pS.getMissionHorizonStartTime(), pS.getMissionHorizonStopTime());
				}

				// Set the list of catalog stations
				setCatalogStationList(pSId, cS.getCatalogOwnerList());

				if ((ownerList == null) || ownerList.isEmpty()) {

					logger.warn("Owner data taken from catalog due to inconsistent data in DPL.");

					ownerList.clear();

					for (CatalogOwner catOwner : cS.getCatalogOwnerList()) {

						ownerList.add(new Owner(catOwner, new ArrayList<ResourceValue>(), new AccountChart(),
								new AccountChart(), new ArrayList<ResourceValue>(), new ArrayList<ResourceValue>())); // default
																														// //
																														// owner
					}
				}

				logger.info("The size of the owners list is: " + ownerList.size());

			} else {

				logger.warn("No DPL methods are invoked due to CSPS debug mode.");

				logger.debug("A default owners list is built to support the debug mode.");
				ownerList = DefaultDebugger.getDefaultOwners(pS);

				for (Owner owner : ownerList) {

					SessionActivator.ownerAcqStationListMap.get(pSId).put(owner.getCatalogOwner().getOwnerId(),
							DefaultDebugger.getDefaultAcqStationList(pSId));
				}
			}

			// Initialize owners map
			SessionActivator.ownerListMap.put(pSId, ownerList);

			for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {

				logger.debug("Valid owner Id is: " + owner.getCatalogOwner().getOwnerId());
			}

			// Set partners data
			setPartnersData(pS, refPSId);
			
			// Set owner TKI
			// Added on 25/01/2022
			setOwnerTKI(pSId);

			isInit = true;

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());

			logger.warn("Some data relevant to owners NOT get from DPL.");
		}

		return isInit;
	}

	/**
	 * Set the list of catalog acquisition stations
	 * 
	 * @param pSId
	 * @param catOwnerList
	 */
	private void setCatalogStationList(Long pSId, List<CatalogOwner> catOwnerList) {

		logger.debug("Set the Catalog list of acquisition stations.");

		for (CatalogOwner catOwner : catOwnerList) {

			logger.debug("Set data for Owner: " + catOwner);

			// Fill the map of owner acquisition stations list
			SessionActivator.ownerAcqStationListMap.get(pSId).put(catOwner.getOwnerId(),
					new ArrayList<AcquisitionStation>());

			// Get the catalog ugs list
			for (CatalogUgs catUgs : catOwner.getUgsList()) {

				logger.debug("Set data for UGS: " + catUgs);

				for (CatalogAcquisitionStation catAcqStation : catUgs.getAcquisitionStationList()) {

					SessionActivator.ownerAcqStationListMap.get(pSId).get(catOwner.getOwnerId())
							.add(new AcquisitionStation(catAcqStation, new ArrayList<ResourceStatus>()));
				}

				if (catUgs.getUseBackupStationFlag() != null && catUgs.getUseBackupStationFlag()) {

					logger.info("Add backup stations for UGS: " + catUgs.getUgsId());

					/** 
					 * The list of catalog stations Id
					 */
					ArrayList<String> catBackStationIdList = new ArrayList<String>();

					for (CatalogAcquisitionStation catBackUpStation : catUgs.getBackupStationList()) {

						if (catBackUpStation != null) {

							logger.debug("Backup station found: " + catBackUpStation.getAcquisitionStationId()
									+ " for UGS: " + catUgs.getUgsId());

							// Add backup station Id
							catBackStationIdList.add(catBackUpStation.getAcquisitionStationId());
						}
					}
					
					// Add station Id list
					SessionActivator.ugsBackStationIdListMap.get(pSId).put(catUgs.getUgsId(),
							catBackStationIdList);

				}
			}
		}
	}

	/**
	 * Set Partner data for the Planning Session
	 * // Updated on 24/11/2022
	 * @param pS
	 * @param refPSId
	 */
	@SuppressWarnings("unchecked")
	private void setPartnersData(PlanningSession pS, Long refPSId) {

		logger.debug("Set internal Partners data.");

		try {

			/**
			 * New resource service factory
			 */
			ResourceService rS = new Factory().getResourceService();
			
			/**
			 * The partners list
			 */
			ArrayList<Partner> partnerList = new ArrayList<>();

			/**
			 * The Planning Session Id
			 */
			Long pSId = pS.getPlanningSessionId();

			// the owner counter
			int k = 0;
	
			for (Owner owner : SessionActivator.ownerListMap.get(pSId)) {
				
				/**
				/*  The TUP Id list
				 */
				ArrayList<String> tupIdList = new ArrayList<String>();
				
				// Set multiple UGS for Owner
				for (CatalogUgs ugsCat : owner.getCatalogOwner().getUgsList()) {

					SessionActivator.ugsOwnerIdMap.get(pSId).put(ugsCat.getUgsId(),
							owner.getCatalogOwner().getOwnerId());
					
					SessionActivator.ugsIdSubCompatibilityMap.get(pSId).put(ugsCat.getUgsId(),
						ugsCat.getUgsSubscriptionCompatibility());

					
					SessionActivator.ugsIsTUPMap.get(pSId).put(ugsCat.getUgsId(),
							ugsCat.getIsTup());
					
					// TODO: check
					if (ugsCat.getIsTup()) {
						
						tupIdList.add(ugsCat.getUgsId());
						
						logger.info("Added S-TUP UGS" + ugsCat.getUgsId() 
							+ " for owner " + owner.getCatalogOwner().getOwnerId());
					} else {
						
						logger.info("Added nominal UGS " + ugsCat.getUgsId() 
						+ " for owner " + owner.getCatalogOwner().getOwnerId());
					}
				}
				
				
				/**
				 * The available Premium BICs
				 */
				double availPremiumBIC = owner.getCatalogOwner().getPremiumBIC();

				/**
				 * The available Routine BICs
				 */
				double availRoutineBIC = owner.getCatalogOwner().getRoutineBIC();

				/**
				 * The available NEO BICs
				 */
				double availNEOBIC = owner.getCatalogOwner().getNeoBIC();

				/**
				 * The MH Premium BICs
				 */
				double mhPremiumBIC = owner.getCatalogOwner().getPremiumBIC();

				/**
				 * The MH Routine BICs
				 */
				double mhRoutineBIC = owner.getCatalogOwner().getRoutineBIC();

				/**
				 * The MH NEO BICs
				 */
				double mhNeoBIC = owner.getCatalogOwner().getNeoBIC();

				if (!SessionChecker.isFirstMH(pSId) && refPSId > 0) {
					
					/**
					 * The list of reference owner
					 */
					List<Owner> refOwnerList = rS.getOwnerListByPlanningSession(refPSId);
			
					/**
					 * The reference owner
					 */
					Owner refOwner = null;
					
					for (int i = 0; i < refOwnerList.size(); i++) {
						
						if (refOwnerList.get(i).getCatalogOwner().getOwnerId().equals(
									owner.getCatalogOwner().getOwnerId())) {
							
							refOwner = refOwnerList.get(i);
							
							break;
						}
					}
					
					// Update BICs for the reference Planning Session					
					mhPremiumBIC = refOwner.getAvailablePremiumBIC().get(0).getValue().doubleValue();
					availPremiumBIC = mhPremiumBIC;
					
					mhRoutineBIC = refOwner.getAvailableRoutineBIC().get(0).getValue().doubleValue();
					availRoutineBIC = mhRoutineBIC;
					
					mhNeoBIC = refOwner.getAvailableNEOBIC().get(0).getValue().doubleValue();
					availNEOBIC =mhNeoBIC;
				}
				
				logger.debug("Get data from DPL for owner " + owner.getCatalogOwner().getOwnerId());

				// Update Premium BICs
				// Condition added on 11/11/2022
				if (!owner.getAvailablePremiumBIC().isEmpty() && !SessionChecker.isPP(pSId)) {
 
					availPremiumBIC = owner.getAvailablePremiumBIC().get(0).getValue().doubleValue();
				}

				// Update Routine BICs
				// Condition added on 11/11/2022
				if (!owner.getAvailableRoutineBIC().isEmpty() && !SessionChecker.isFinal(pSId)) {

					availRoutineBIC = owner.getAvailableRoutineBIC().get(0).getValue().doubleValue();
				}

				// Update NEO BICs
				if (!owner.getAvailableNEOBIC().isEmpty()  && !SessionChecker.isPP(pSId)) {

					availNEOBIC = owner.getAvailableNEOBIC().get(0).getValue().doubleValue();
				}
				
				// Recover HP/PP (premium) BICs as RTN 1-5 (routine) BICs
				if (!SessionChecker.isFirstMH(pSId) && (SessionChecker.isRoutine(pSId)
						|| SessionChecker.isUnranked(pSId) || SessionChecker.isSelf(pSId))) {

					logger.info("Set residual Routine BICs available for the Ranked Planning Session.");
					
					if (SessionChecker.isFinal(pSId)) {
						
						availRoutineBIC = owner.getAvailableRoutineBIC().get(0).getValue().doubleValue();
					}
					
					// Update Routine BICs
					availRoutineBIC += availPremiumBIC;

//					mhRoutineBIC += availPremiumBIC;
//					
//					// Premium BICs
//					mhPremiumBIC -= availPremiumBIC;
					
					// Reset MH consumed Premium BICs
					availPremiumBIC = 0;
					
												   
					if (!owner.getAvailablePremiumBIC().isEmpty() && !owner.getAvailableRoutineBIC().isEmpty()) {
						// Routine BICs
						owner.getAvailableRoutineBIC().set(0,
								new ResourceValue(owner.getAvailableRoutineBIC().get(0).getReferenceTime(), BigDecimal
										.valueOf(owner.getAvailableRoutineBIC().get(0).getValue().doubleValue()
												+ owner.getAvailablePremiumBIC().get(0).getValue().doubleValue())));

						// Premium BICs
						owner.getAvailablePremiumBIC().set(0, new ResourceValue(
								owner.getAvailableRoutineBIC().get(0).getReferenceTime(), BigDecimal.valueOf(0)));

						// NEO BICs
						owner.getAvailableNEOBIC().set(0, new ResourceValue(
								owner.getAvailableRoutineBIC().get(0).getReferenceTime(), BigDecimal.valueOf(0)));
					
					} else {

						// Routine BICs
						owner.getAvailableRoutineBIC().add(new ResourceValue(
										SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime(),
										BigDecimal.valueOf(availRoutineBIC)));

						// Premium BICs
						owner.getAvailablePremiumBIC().add(new ResourceValue(
										SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime(),
										BigDecimal.valueOf(0)));

						// NEO BICs
						owner.getAvailableNEOBIC().add(new ResourceValue(
										SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime(),
										BigDecimal.valueOf(0)));
					}


					// Update owner list
					SessionActivator.ownerListMap.get(pSId).set(k, owner);
				}

				/**
				 * The ugs Owner
				 */
				String ugsOwner = owner.getCatalogOwner().getUgsList().get(0).getUgsId();
				
				for (CatalogUgs catUgs : owner.getCatalogOwner().getUgsList()) {
					
					if (!catUgs.getIsTup()) {
						
						ugsOwner = catUgs.getUgsId();
					}
				}
				
				/**
				 * The CSPS partner related to the owner
				 */
				Partner partner = new Partner(owner.getCatalogOwner().getOwnerId(), ugsOwner);

				// Set Partner BICs
				partner.setPremBIC(availPremiumBIC);
				partner.setRoutBIC(availRoutineBIC);
				partner.setNeoBIC(availNEOBIC);
				partner.setMHPremBIC(mhPremiumBIC);
				partner.setMHRoutBIC(mhRoutineBIC);
				partner.setMHNeoBIC(mhNeoBIC);
				partner.setTUPIdList(tupIdList);

				partnerList.add(partner);

				logger.info("Initial BIC amount available for partner " + partner.getId() + ": " + "Premium = "
						+ availPremiumBIC + ", Routine = " + availRoutineBIC + ", NEO = " + availNEOBIC + ".");

				k++;
			}

			SessionActivator.partnerListMap.put(pSId, (ArrayList<Partner>) partnerList.clone());

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString());

			logger.warn("Some data relevant to owners NOT get from DPL.");

		}
	}

	/**
	 * Set the  TKI for owner
	 * @param pSId
	 * @throws Exception 
	 */
	private void setOwnerTKI(Long pSId) throws Exception {
		
		logger.info("Set TKI ranges for Partners...");
		
		/**
		 * The EKMLib
		 */
		EKMLIB ekmlib = new EKMLIB();

        Method getRangeMethod = EKMLIB.class.getDeclaredMethod("getRange", String.class, int.class);
        getRangeMethod.setAccessible(true);   
        
        for (Partner partner : SessionActivator.partnerListMap.get(pSId)) {
        
        	/**
        	 * The TKI range
        	 */
        	String[] range = (String[])(getRangeMethod.invoke(ekmlib, partner.getPartnerId(), 0));
		
	    	if (range.length > 0 && range[0] != null) {
	    		
	    		logger.debug("Update TKI initial range for Partner " + partner.getPartnerId() 
	    			+ " with initial value: " + Integer.getInteger(range[0]));
	    		
	    		Configuration.tkiOwnerMap.put(partner.getPartnerId(), Integer.parseInt(range[0]));
	        
	    	} else {
	        	
	    		logger.warn("No effective TKI range found for Partner " + partner.getPartnerId());
	        }
       }
	}
	
	/**
	 * Fill the default PRList data
	 *
	 * @param pRList
	 */
	private void fillDefaultPRListData(Collection<ProgrammingRequest> pRList) {

		logger.debug("Fill the default PRList data.");

		for (ProgrammingRequest pR : pRList) {

			if (pR.getKind() == null) {

				// Set kind
				pR.setKind(PRKind.BACKGROUND);
			}

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

				for (DTO dto : aR.getDtoList()) {

					if (dto.isDi2SFlag() == null) {

						// Set DI2S flag
						dto.setDi2SFlag(false);
					}
				}
			}

			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {

				if (aR.getEquivalentDTO() != null) {

					if (aR.getEquivalentDTO().getTaskList() == null) {

						aR.getEquivalentDTO().setTaskList(new ArrayList<Task>());
					}
				}
			}
		}
	}

	/**
	 * Set the rejected set of DTO Ids for a given Planning Session
	 *
	 * @param pSId - the Planning Session Id
	 * @return 
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<ProgrammingRequest> getRejPlanSessionPRList(Long pSId) {

		/**
		 * Instance Handlers
		 */
		PRListProcessor pRListProcessor = new PRListProcessor();
		
		logger.debug("Collect Rejected PRs for Planning Session: " + pSId);
		
		/**
		 * The list of rejected PRs
		 */
		ArrayList<ProgrammingRequest> rejPRList = new ArrayList<ProgrammingRequest>();
		
		ArrayList<ProgrammingRequest> pSPRList = pRListProcessor.getPlanSessionPRList(pSId);
		
		for  (ProgrammingRequest pSPR : pSPRList) {
		
			for (PlanProgrammingRequestStatus pRStatus : (ArrayList<PlanProgrammingRequestStatus>) 
					((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()).clone()) {
	
				/**
				 * The Programming Request Id
				 */
				String schedPRId = ObjectMapper.parseDMToSchedPRId(pSPR.getUgsId(),
						pSPR.getProgrammingRequestId());
				
				if (schedPRId.equals(ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), 
						pRStatus.getProgrammingRequestId()))) {
								
					if (! pRStatus.getStatus().equals(PRStatus.Scheduled)) {
						
						logger.debug("Persist Rejected PR: " + schedPRId);
						
						// Add rejected DTO Id
						rejPRList.add(pSPR);	
					}
					
					break;
				}
			}
		}
		
		return rejPRList;
	}
	
	/**
	 * Set the rejected set of DTO Ids for a given Planning Session
	 * 
	 * @param pSId
	 * @param rejPRList
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<PlanProgrammingRequestStatus> getRejPlanSessionPRStatuses(Long pSId, 
			ArrayList<ProgrammingRequest> rejPRList) {
		
		logger.debug("Collect Rejected PRs for Planning Session: " + pSId);
		
		/**
		 * The list of rejected PRs
		 */
		ArrayList<PlanProgrammingRequestStatus> rejPRStatusList = new ArrayList<PlanProgrammingRequestStatus>();
		
		for  (ProgrammingRequest pSPR : rejPRList) {
		
			for (PlanProgrammingRequestStatus pRStatus : (ArrayList<PlanProgrammingRequestStatus>) 
					((ArrayList<PlanProgrammingRequestStatus>) SessionActivator.planSessionMap.get(pSId)
					.getProgrammingRequestStatusList()).clone()) {
	
				/**
				 * The Programming Request Id
				 */
				String schedPRId = ObjectMapper.parseDMToSchedPRId(pSPR.getUgsId(),
						pSPR.getProgrammingRequestId());
				
				if (schedPRId.equals(ObjectMapper.parseDMToSchedPRId(pRStatus.getUgsId(), 
						pRStatus.getProgrammingRequestId()))) {
														
					logger.debug("Persist Rejected PR Status of PR: " + schedPRId);
					
					// Add rejected PR Status
					rejPRStatusList.add(pRStatus);	
					
					break;
				}
			}
		}
		
		return rejPRStatusList;
	}

}


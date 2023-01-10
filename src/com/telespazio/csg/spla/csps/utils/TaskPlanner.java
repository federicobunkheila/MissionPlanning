/**
*
* MODULE FILE NAME: ObjectParser.java
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

package com.telespazio.csg.spla.csps.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.drools.utils.TaskPlanned;
import com.nais.spla.brm.library.main.ontology.enums.Actuator;
import com.nais.spla.brm.library.main.ontology.enums.DownlinkStrategy;
import com.nais.spla.brm.library.main.ontology.enums.Polarization;
import com.nais.spla.brm.library.main.ontology.resourceData.PacketStore;
import com.nais.spla.brm.library.main.ontology.resources.MemoryModule;
import com.nais.spla.brm.library.main.ontology.tasks.RampCMGA;
import com.nais.spla.brm.library.main.ontology.tasks.Storage;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.MacroDLO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.enums.LookSide;
import it.sistematica.spla.datamodel.core.enums.TaskMarkType;
import it.sistematica.spla.datamodel.core.enums.TaskStatus;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.exception.SPLAException;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.bean.UgsOwner;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
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

/**
 * The Task Planner class
 * Hp: every task size is in sector
 *
 */
public class TaskPlanner {

	/**
	 * The proper logger
	 */
	public static Logger logger = LoggerFactory.getLogger(TaskPlanner.class);

	public static HashMap<String, String> schedDTODwlPSIdMap = new HashMap<String, String>();
		
	/**
	 * Plan acquisition task
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws Exception
	 */
	public static Acquisition planAcqTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The Acquisition Task
		 */
		Acquisition task = new Acquisition();

		task.setTaskId(taskId);
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());

		// Check Task Mark
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}

		logger.trace("Set Acquisition Ids.");
		
		/**
		 * The Ids array
		 */
		String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask()
				.split(Configuration.splitChar);
		
		/**
		 * The PRList Id
		 */
		String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
				.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);
		
		// Set task data
		task.setProgrammingRequest(ids[0], pRListId, ids[1]);
		task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
		task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);

		task.setUgsOwnerList(ObjectMapper.parseBRMToDMUserInfo(
				((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getUserInfo()));
		
		// Set Acq DI2SInfo
		task.setDi2s(ObjectMapper.parseBRMToDMDi2sInfo(pSId,
				((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getDi2sInfo(), taskId,
				((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask()));
		task.setTaskStatus(TaskStatus.Unchecked);
		
		// Set DTO data
		task.setImageIdentifier(Long.valueOf(taskId));
		SessionScheduler.dtoImageIdMap.get(pSId).put(
				((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getIdTask(),
				task.getImageIdentifier());	
		task.setLookSide(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getLookSide());
		task.setBic(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getImageBIC());
		task.setEss(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getEss());
		task.setSensorMode(ObjectMapper.parseBRMToDMSensorMode(
				((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getSensorMode()));		
		// Set Acquisition size (sectors)
		task.setSizeH((double) ((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getSizeH());
		task.setSizeV((double) ((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getSizeV());
		
		// Set request Data
		
		/**
		 * The ugs Id
		 */
		String ugsId = ids[0];
		
		if (((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getUserInfo() != null
				&& !((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getUserInfo().isEmpty()) {
				
					ugsId =	(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getUserInfo().get(0).getUgsId());
		} else {
			
			logger.warn("No User Info found for Acquisition of DTO: " + ((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask());
		}
				
		task.setType(ObjectMapper.parseBRMToDMPRType(
				((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).getPrType(), ugsId));
		
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask).isRemovableFlag());
		task.setWeightedRank(Integer.toUnsignedLong(((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) inTask)
				.getPriority()));
		
		// TODO: Added on 24/03/2022 for DI2S polarization management	
		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(
		((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()) 
				&& PRListProcessor.dtoSchedIdMap.get(pSId).get(
				((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()).getPolarization() != null) {
										
			task.setPolarization(PRListProcessor.dtoSchedIdMap.get(pSId)
					.get(((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()).getPolarization());
				
		} else  {
			
			task.setPolarization(ObjectMapper.parseBRMToDMPolar(((Storage) inTask).getPol()));
		}
		
		logger.trace("Add Acquisition SPARC info...");
		TaskInfoParser.setAcqInfo(task, pSId);

		return task;
	}

	/**
	 * Plan BITE task
	 * 
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws Exception
	 */
	public static BITE planBiteTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The BITE task
		 */
		BITE task = new BITE();

		logger.trace("Set Bite Ids.");
		
		/**
		 * The Ids array
		 */
		String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask()
				.split(Configuration.splitChar);
		/**
		 * The PRList Id
		 */
		String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
				.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);
		
		// Set task data
		task.setProgrammingRequest(ids[0], pRListId, ids[1]);
		task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
		task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);
		
		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		
		if (SessionScheduler.dtoImageIdMap.get(pSId).containsKey(
				((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getIdTask())) {
			
			task.setImageIdentifier(SessionScheduler.dtoImageIdMap.get(pSId)
				.get(((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getIdTask()));
		
		} else {
			
			logger.warn("No image Id associated to the BITE of DTO: "
			+ ((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getIdTask());
		}
		
		// Get the applicable contact counter 
		task.setContactCounter(((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getContactCounter());
		task.setTaskStatus(TaskStatus.Unchecked);
		
		// Check Task Mark
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}

		// Set request data
		task.setRemovableFlag(true);
		
		// Set visibility data
		task.setContactCounter(((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getContactCounter());
		task.setAcquisitionStationId(((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getAcqStatId());
		task.setCarrierL2Selection(((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).isCarrierL2Selection());
		// Set temporary look side
		task.addValueInMap("isRightLookSide", (Boolean.toString(
				((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).isEnterInVisibilityInRL())));
		
		task.setPacketStoreSize(BigDecimal.valueOf(
				((com.nais.spla.brm.library.main.ontology.tasks.Bite) inTask).getPacketStoreSize()));	
		task.setFillerWord(((com.nais.spla.brm.library.main.ontology.tasks.Bite)inTask).getFillerWord());
		task.setModuleId(((com.nais.spla.brm.library.main.ontology.tasks.Bite)inTask).getModuleId());
		task.setModuleSelectionFlag(((com.nais.spla.brm.library.main.ontology.tasks.Bite)inTask)
				.getModuleSelectionFlag());
		task.setPacketStoreId(Long.parseLong(((com.nais.spla.brm.library.main.ontology.tasks.Bite)inTask)
				.getPacketStoreId()));
		
		// Set default data
		task.setPacketStoreSequenceId((byte) 0);
		task.setPacketStoreTotalParts((byte) 1);
		task.setPacketStorePartNumber((byte) 1);

		logger.trace("Add Bite SPARC info...");
		TaskInfoParser.setBiteInfo(task, pSId);
		
		return task;
	}

//	/**
//	 * Get the BITE contact counter
//	 * @param pSId
//	 * @return
//	 */
//	private static Long getBiteContactCounter(Long pSId, BITE bite) {
//		
//		Long contactCounter = null;
//		
//		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {
//			
//			for (Visibility vis : sat.getVisibilityList()) {
//				
//				if (vis.getVisibilityStartTime().getTime() >= bite.getStartTime().getTime()
//							&& vis.getVisibilityStopTime().getTime() <= bite.getStartTime().getTime()) {
//											
//					if (isVisOwner(pSId, SessionActivator.ugsOwnerIdMap.get(pSId).get(bite.getUgsId()), 
//							vis.getAcquisitionStationId())) {
//						
//						contactCounter = vis.getContactCounter();
//						
//						break;
//					}
//				}
//			}
//		}
//		
//		return contactCounter;
//	}
//		
//	/**	
//	 * Check if is the owner visibility
//	 * @param pSId
//	 * @param ownerId
//	 * @param acqStationId
//	 * @return
//	 */
//	private static boolean isVisOwner(Long pSId, String ownerId, String acqStationId) {
//		
//		for (AcquisitionStation acqStation : SessionActivator.ownerAcqStationListMap.get(pSId).get(ownerId)) {
//			
//			if (acqStation.getCatalogAcquisitionStation().getAcquisitionStationId().equals(acqStationId)) {
//				
//				return true;
//			}
//		}
//		
//		return false;
//	}
	
	
	/**
	 * Plan CMGAxis task 
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws InputException
	 */
	public static CMGAxis planCMGAxisTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The CMGAxis Task
		 */
		CMGAxis task = new CMGAxis();

		// Set Task data
		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		task.setTaskStatus(TaskStatus.Unchecked);
		
		// Check Task Mark
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}

		// Set request data
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).isRemovableFlag());
		((CMGAxis) task).setRollAxisFlag(!((com.nais.spla.brm.library.main.ontology.tasks.CMGAxis)inTask).isRollToPitch());
		
		return task;
	}

	/**
	 * Plan DLO Task according to the relevant planned task 

	 * @param pSId
	 * @param taskId
	 * @param prodList
	 * @param biteList
	 * @return
	 * @throws SPLAException 
	 */
	@SuppressWarnings("unchecked")
	public static ArrayList<DLO> planDLOTaskList(Long pSId, int taskId, int taskCount,
			ArrayList<Task> prodList, ArrayList<BITE> biteList) throws SPLAException {

		/**
		 * The list of DLOs
		 */
		ArrayList<DLO> dloList = new ArrayList<>();

		ArrayList<Task> taskList = (ArrayList<Task>) prodList.clone();
		
		taskList.addAll(biteList);
		
		// Sort Tasks
		Collections.sort(taskList, new TaskStartTimeComparator());

		for (Satellite sat : SessionScheduler.satListMap.get(pSId)) {

			if (sat.getVisibilityList() != null) {

				for (Visibility vis : sat.getVisibilityList()) {

					if (vis.getVisibilityStopTime().getTime() >= 
							SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime()
							&& vis.isAllocated() && vis.isXbandFlag()) {
					
						/**
						 * The DLO
						 */
						DLO dlo = new DLO();
	
						// Set Task data
						dlo.setSatelliteId(sat.getCatalogSatellite().getSatelliteId());
						dlo.setVisibilityStartDateTime(vis.getVisibilityStartTime());
						dlo.setVisibilityStopDateTime(vis.getVisibilityStopTime());
						dlo.setAcquisitionStationId(vis.getAcquisitionStationId());
						dlo.setContactCounter(vis.getContactCounter());
						dlo.setCarrierL1(false);
						dlo.setCarrierL2(false);

						// Set DLO if relevant tasks are found
						dlo = findDLOTask(pSId, dlo, taskList);
						
						if (dlo != null && isCoherentLookSide(dlo, vis)) {
							
							// Update counter (all visibilities)
							taskCount ++;
							
							dlo.setTaskId(Integer.parseInt(Integer.toString(taskId) + taskCount));
		
							dloList.add(dlo);
						}
					}
				}
			}
		}

		return dloList;
	}
	
	/**
	 * Find DLO based on the relevant Tasks
	 * @param pSId
	 * @param dlo
	 * @param taskList
	 * @return
	 * @throws SPLAException 
	 */
	private static DLO findDLOTask(Long pSId, DLO dlo, ArrayList<Task> taskList) throws SPLAException {
				
		/**
		 * The relevant Task list
		 */
		ArrayList<Task> relTaskList = new ArrayList<Task>();
		
		for (Task task : taskList) {

			// Check Task Type
			if (task.getTaskType().equals(TaskType.DWL)) {
			
				/**
				 * The Download Task
				 */
				Download dwl = (Download) task;
				
				if (dlo.getContactCounter().equals(dwl.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(dwl.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(dwl.getSatelliteId())) {
				
					if ((dwl.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (dwl.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {

						logger.debug("Added Download Task " + task.getTaskId() + " to the DLO.");

						// Add dwl
						relTaskList.add(dwl);
						
						dlo.setCarrierL1(true);
						dlo.setCarrierL2(true);
						
//--------- Commented in order to consider both the carriers always ON
//						
//						if (dwl.getCarrierL2Selection()) {
//		
//							dlo.setCarrierL2(true);
//							
//						} else {
//		
//							dlo.setCarrierL1(true);
//						}
					}
				}
			}
			
			else if (task.getTaskType().equals(TaskType.PASSTHROUGH)) {
				
				/**
				 * The PassThrough Task
				 */
				PassThrough pT = (PassThrough) task;
				
				if (dlo.getContactCounter().equals(pT.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(pT.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(pT.getSatelliteId())) {
				
					if ((pT.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (pT.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {
		
						logger.debug("Added PassThrough Task " + task.getTaskId() + " to the DLO.");
						
						// Add pt
						relTaskList.add(pT);

						if (pT.getPacketStoreSizeH() != null
								&& pT.getPacketStoreSizeH().doubleValue() > 0
								&& pT.getPacketStoreSizeV() != null
								&& pT.getPacketStoreSizeV().doubleValue() > 0) {
							
							dlo.setCarrierL1(true);
							dlo.setCarrierL2(true);

//--------- Commented in order to consider both the carriers always ON
//													
//						} else if (pT.getPacketStoreSizeH() != null 
//								&& pT.getPacketStoreSizeH().doubleValue() > 0) {
//		
//							if (pT.getCarrierL2SelectionH()) {
//								
//								dlo.setCarrierL2(true);
//								
//							} else {
//								
//								dlo.setCarrierL1(true);
//							}
//							
//						} else if (pT.getPacketStoreSizeV() != null 
//								&& pT.getPacketStoreSizeV().doubleValue() > 0) {
//
//							if (pT.getCarrierL2SelectionV()) {
//								
//								dlo.setCarrierL2(true);
//								
//							} else {
//								
//								dlo.setCarrierL1(true);
//							}
						}					
					}
				}
			}
			
			else if (task.getTaskType().equals(TaskType.BITE)) {
				
				/**
				 * The BITE Task
				 */
				BITE bite = (BITE) task;
				
				if (dlo.getContactCounter().equals(bite.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(bite.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(bite.getSatelliteId())) {
				
					if ((bite.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (bite.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {
		
						logger.debug("Added BITE Task " + task.getTaskId() + " to the DLO.");
						
						// Add BITE
						relTaskList.add(bite);

						dlo.setCarrierL1(true);
						dlo.setCarrierL2(true);

//--------- Commented in order to consider both the carriers always ON
//												
//						if (bite.getCarrierL2Selection()) {
//		
//							dlo.setCarrierL2(true);
//						
//						} else {
//		
//							dlo.setCarrierL1(true);
//						}
					}
				}
			}
		}
		
		//--------------------------
		
		// Changed on 24/02/2022 for visibility carrier management
		if (! relTaskList.isEmpty()) {
			
			/**
			 * The associated visibility to the DLO
			 */
			com.nais.spla.brm.library.main.ontology.resourceData.Visibility assVis = 
					RulesPerformer.getVisibility(pSId, dlo.getAcquisitionStationId(), 
							dlo.getContactCounter(), dlo.getSatelliteId());
			
			if (assVis != null && assVis.getStartCarrier()!= null && assVis.getStopCarrier()!= null) {
																		  
				// Sort Tasks by stop time
				Collections.sort(relTaskList, new TaskStopTimeComparator());
			
				dlo.setStopTime(relTaskList.get(relTaskList.size() - 1).getStopTime());
				dlo.setStopCarrier(assVis.getStopCarrier());
				
				// Sort Tasks by start time
				Collections.sort(relTaskList, new TaskStartTimeComparator());
				
				dlo.setStartTime(relTaskList.get(0).getStartTime());
				dlo.setStartCarrier(assVis.getStartCarrier());

				// Set side according to the inside Downloads flags - within method findDLOData
				dlo.setRightLookSide(isDLORightLookSide(dlo, relTaskList));
				dlo.setTaskMark(TaskMarkType.NOMINAL);
				dlo.setTaskStatus(TaskStatus.Unchecked);
				dlo.setRemovableFlag(false);
				
				logger.debug("Scheduled DLO for satellite " + dlo.getSatelliteId()
						+ " associated to visibility from " + dlo.getStartTime() + " to " + dlo.getStopTime() 
						+ " of the acquisition station " + dlo.getAcquisitionStationId() 
						+ " for contact counter " + dlo.getContactCounter()
						+ " with Right look side: " + dlo.getRightLookSide());
				
			} else {
				
				logger.warn("Null visibility data found for the DLO!");
				
				dlo = null;
			}
			
			//---------------------
			
		} else {
			
			dlo = null;
		}
		
		return dlo;
	}
	
	/**
	 * Check if the side of the DLO is right looking
	 * 
	 * @param taskList
	 * @return
	 * @throws SPLAException 
	 */
	private static boolean isDLORightLookSide(DLO dlo, ArrayList<Task> taskList) throws SPLAException {
		
		/**
		 * The isRight side boolean
		 */
		boolean isRightSide = true;
		
		for (Task task : taskList) {
			
			// Check Task Type
			if (task.getTaskType().equals(TaskType.DWL)) {
			
				/**
				 * The Download Task
				 */
				Download dwl = (Download) task;
				
				if (dlo.getContactCounter().equals(dwl.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(dwl.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(dwl.getSatelliteId())) {
				
					if ((dwl.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (dwl.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {
				
						try {
						
							if ((((Download) task).readMap("isRightLookSide")).equals("true")) {
	
								isRightSide = true;
							
							} else {
									
								isRightSide = false;	
							}
							
							break;
						
						} catch (Exception ex) {
							
							ex.getMessage();
						}
					}
				}
				
			} else if (task.getTaskType().equals(TaskType.PASSTHROUGH)) {

				/**
				 * The PassThrough Task
				 */
				PassThrough pt = (PassThrough) task;
				
				if (dlo.getContactCounter().equals(pt.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(pt.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(pt.getSatelliteId())) {
				
					if ((pt.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (pt.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {
				
						if ((((PassThrough) task).readMap("isRightLookSide")).equals("true")) {

							isRightSide = true;
						
						} else {
							
							isRightSide = false;
						}
						
						break;
					}
				}		
			
			} else if (task.getTaskType().equals(TaskType.BITE)) {

				/**
				 * The BITE Task
				 */
				BITE bite = (BITE) task;
				
				if (dlo.getContactCounter().equals(bite.getContactCounter())
					&& dlo.getAcquisitionStationId().equals(bite.getAcquisitionStationId())
					&& dlo.getSatelliteId().equals(bite.getSatelliteId())) {
				
					if ((bite.getStartTime().compareTo(dlo.getVisibilityStartDateTime()) >= 0)
							&& (bite.getStopTime().compareTo(dlo.getVisibilityStopDateTime()) <= 0)) {
				
						if (((BITE) task).readMap("isRightLookSide").equals("true")) {

							isRightSide = true;
						
						} else {
							
							isRightSide = false;
						}
						
						break;
					}
				}
			}
		}
		
		return isRightSide;	
	}
	
	/**
	 * Check the coherence of the look side between DLO and visibility
	 * @param dlo
	 * @param vis
	 * @return
	 */
	private static boolean isCoherentLookSide(DLO dlo, Visibility vis) {
		
		// Add DLO condition (according look side, both or notSpecified only)					
		if ((vis.getLookSide().equals(LookSide.Right) && dlo.getRightLookSide())
			|| (vis.getLookSide().equals(LookSide.Left) && ! dlo.getRightLookSide())
			|| vis.getLookSide().equals(LookSide.Both)
			|| vis.getLookSide().equals(LookSide.NotSpecified)) {
			
			return true;
		}
		
		return false;
	}

	/**
	 * Plan Maneuver task // TODO: complete data
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws InputException
	 */
	public static Maneuver planManTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The Maneuver Task
		 */
		Maneuver task = new Maneuver();

		// Set Task data
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		task.setTaskId(taskId);
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}
		task.setTaskStatus(TaskStatus.Unchecked);
		
		// Set Maneuver data
		task.setRightToLeftFlag(
				((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).isRightToLeftFlag());
		task.setManeuverType(((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).getType().toString());
				
		// Set Maneuver Ids
		task.setAcqIdFrom(((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).getAcq1Id());
		task.setAcqIdTo(((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).getAcq2Id());

		// Set Maneuver Info
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).isRemovableFlag());		
		if (((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask).getActuator()
				.equals(Actuator.ReactionWheels)) {
			task.setActuator("RW");	
		} else {
			task.setActuator("CMG");			
		}
			
		// Add equivalent maneuver data
		TaskInfoParser.addEquivManInfo(task, 
				(com.nais.spla.brm.library.main.ontology.tasks.Maneuver) inTask, pSId);

		return task;
	}
	
	/**
	 * Plan Download task
	 * // TODO: change according to the dwl MM 
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @param stoList
	 * @return
	 * @throws Exception
	 */
	public static Download planDwlTask(Long pSId, Object inTask, int taskId, ArrayList<Store> stoList)
			throws Exception {

		/**
		 * The Download Task
		 */
		Download task = new Download();
		
		/**
		 * Instance handlers
		 */
		TaskPlanned taskPlanned = new TaskPlanned();
		
		logger.trace("Set Download Ids.");

		// Set Task data
		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).isRemovableFlag());
				
		if (!((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId().contains("GPS")
				&& !((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId().contains("null")) {

			/**
			 * The Ids Array
			 */
			String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId()
					.split(Configuration.splitChar);
			
			/**
			 * The PRList Id
			 */ 
			String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
					.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);

			// Set Task data
			task.setProgrammingRequest(ids[0], pRListId, ids[1]);
			task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
			task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);
			task.setUgsOwnerList(getUgsOwners(pSId, ((com.nais.spla.brm.library.main.ontology.tasks.Download) 
					inTask).getUgsOwnerList(), ids[0]));
					
			// Set download DI2S Info
			task.setDi2s(ObjectMapper.parseBRMToDMDi2sInfo(
					pSId, ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getDi2sInfo(), taskId,
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId()));
	
			/**
			 * The Download Storage TreeMap
			 */
			TreeMap<String, Storage> dwlStoMap = taskPlanned.receiveAllStorages(pSId.toString(), 0, 
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getSatelliteId());
			
			/**
			 * The Download Storage
			 */
			Storage dwlSto = dwlStoMap.get(
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getIdTask());
			
			// Set download packet store data
			setDwlPacketStoreData(pSId, task, ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask), stoList);
	
			if (!schedDTODwlPSIdMap.containsKey(task.getPacketStoreId())) {
			
				schedDTODwlPSIdMap.put(task.getPacketStoreId(),
						((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getIdTask());
			
			} else if (!schedDTODwlPSIdMap.get(task.getPacketStoreId()).equals(  
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getIdTask())) {
				logger.warn("More than one dwl with the same Packet Store Id is found!");
			}
			
			if (dwlSto != null) {
				
				// Set download Memory Modules
				task.setMemoryModules(getDwlMMList(pSId, dwlSto, ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPol()));
			
			} else {
				
				logger.warn("No associated Storage found for Download: " 
						+ ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getReferredEquivalentDto());
			}
			
			logger.trace("Add Download SPARC info...");
			TaskInfoParser.setDwlInfo(pSId, task, ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPol());
			
		} else {

			logger.debug("Set GPS Ids.");
	
			// Set GPS Ids
			task.setUgsOwnerList(getGPSUgsOwners(pSId));
			
			// Set GPS packet store data
			task.setPacketStoreSize(BigDecimal.valueOf(
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getDownloadedSize()));	
			task.setPacketStorePartNumber((byte) 1);
			task.setPacketStoreTotalParts((byte) 1);
			task.setSourcePacketNumberH(BigDecimal.ONE); // TODO: TBD?
			task.setSourcePacketNumberV(BigDecimal.ZERO); // TODO: TBD?
	
			// Set packet Store Id
			task.setPacketStoreId(Integer.toString(
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPacketStoreNumber()));
			
			/**
			 * The data strategy
			 */
			Boolean dataStrategy = false;
			
			// Set Packet Store data strategy
			if (((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPacketStoreStrategy()
					.equals(DownlinkStrategy.DELETE)) {
	
				dataStrategy = true;			
			}
						
			task.setDataStrategy(dataStrategy);
			
			// Added on 29/09/2021 for GPS DWL management from CSPS-1.11.7 
			if (Configuration.defGPSAddParam1 != null
					&& !Configuration.defGPSAddParam1.contentEquals("NA"))  {
			
				task.setAdditionalPar1(Configuration.defGPSAddParam1);
			}
		}
			
		// Set add data
		task.setTaskStatus(TaskStatus.Unchecked);
		
		// Set visibility data
		task.setContactCounter(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getContactCounter());
		task.setAcquisitionStationId(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getAcqStatId());
		task.setCarrierL2Selection(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).isCarrierL2Selection());
		// Set temporary look side
		task.addValueInMap("isRightLookSide", (Boolean.toString(
				((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).isEnterInVisibilityInRL())));
	
		// Set Download shifts
		if  (((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getSectorShift() < 0)
		{
			task.setShiftPointer(null);
		
		} else {			 
		
			task.setShiftPointer(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getSectorShift());
		}
		
		task.setTimeShift((long)(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getTimeShiftMillisec() / 1000.0));

		if (SessionScheduler.dtoImageIdMap.get(pSId) != null && SessionScheduler.dtoImageIdMap.get(pSId).containsKey(
				((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId())) {
			
			task.setImageIdentifier(SessionScheduler.dtoImageIdMap.get(pSId)
				.get(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId()));
		
		} else if (!((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId().contains("GPS")
				&& !((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId().contains("null")) {
			
			logger.warn("No image Id associated to the download of DTO: "
			+ ((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId());
			
			// Nullify task
			task = null;
		}
		
		return task;
	}

	/**
	 * Plan PassThrough task 
	 * // TODO: finalize parameters with BRM 
	 * // TODO: sequenceId with encryption
	 * // TODO: handle subscribers (FAT-2)
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws InputException
	 */
	public static PassThrough planPTTask(Long pSId, Object inTask, int taskId, int iter) throws Exception {

		/**
		 * The PassThrough Task
		 */
		PassThrough task = new PassThrough();
		
		/**
		 * The passThrough packet Id
		 */
		Long packetId = (long) iter;
		
		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setMacroActivityTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartDwlTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		
		logger.trace("Set PassThrough Ids.");
		String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask()
				.split(Configuration.splitChar);
		String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
				.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);
		task.setProgrammingRequest(ids[0], pRListId, ids[1]);
		task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
		task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);
		
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}
		task.setTaskStatus(TaskStatus.Unchecked);
		
		if (SessionScheduler.dtoImageIdMap.get(pSId).containsKey(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getIdTask())) {
			
			task.setImageIdentifier(SessionScheduler.dtoImageIdMap.get(pSId)
				.get(((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getIdTask()));
		
		} else {
			
			logger.debug("No image Id associated to the PassThrough of DTO: "
			+ ((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getIdTask());
		
			// TOOODO: added on 21/07/2022 instead of null value
			task.setImageIdentifier(Integer.toUnsignedLong(taskId + 1));
		}

		task.setUgsOwnerList(getUgsOwners(pSId,
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getUgsOwnerList(), ids[0]));		
		// Set visibility data
		task.setAcquisitionStationId(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getGroundStationId().get(0));
		task.setContactCounter(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getContactCounterVis());	
		task.setCarrierL2SelectionH(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).isCarrierL2SelectionH());
		task.setCarrierL2SelectionV(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).isCarrierL2SelectionV());
		// Set temporary look side
		task.addValueInMap("isRightLookSide", (Boolean.toString(
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).isEnterInVisibilityInRL())));
		
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).isRemovableFlag());
		task.setDi2s(ObjectMapper.parseBRMToDMDi2sInfo(pSId,
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getDi2sInfo(), taskId,
				((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getIdTask()));
		task.setHMemoryModules(((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getMemoryModulesH());
		task.setVMemoryModules(((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getMemoryModulesV());
		
		// Set PassThrough Packet Store data
		setPTPacketStoreData(pSId, task, (com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask, packetId);
		 
		logger.trace("Add PassThrough SPARC info...");
		TaskInfoParser.setPTInfo(pSId, task, 
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getPacketStoreSizeH(),
				((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask).getPacketStoreSizeV());

		return task;
	}

	/**
	 * Plan Ramp Task
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws Exception
	 */
	public static Ramp planRampTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The Ramp Task
		 */
		Ramp task = new Ramp();

		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTimeWithPrepTime(((RampCMGA) inTask).getStartTimeWithPrep());
		task.setStartTime(((RampCMGA) inTask).getStartTime()); // TODO: added on 29/11/2019
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);
		}
		task.setTaskStatus(TaskStatus.Unchecked);
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).isRemovableFlag());
		task.setUpFlag(((RampCMGA) inTask).isRampUp());

		return task;
	}

	/**
	 * Plan Silent task
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws InputException
	 */
	public static Silent planSilTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The Silent Task
		 */
		Silent task = new Silent();

		String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).getAssociatedAcq()
				.split(Configuration.splitChar);
		String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
				.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);
		task.setProgrammingRequest(ids[0], pRListId, ids[1]);
		task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
		task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);

		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);

		}
		task.setTaskStatus(TaskStatus.Unchecked);
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).isRemovableFlag());
		task.setDi2s(ObjectMapper
				.parseBRMToDMDi2sInfo(pSId, ((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).getDi2sInfo(), taskId,
						((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).getAssociatedAcq()));
		task.setEss(((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).getEnergy());
		task.setLoanedESS(((com.nais.spla.brm.library.main.ontology.tasks.Silent) inTask).getLoanFromEss());

		return task;
	}

	/**
	 * Plan StoreAux Task
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @return
	 * @throws Exception
	 */
	public static StoreAux planStoreAuxTask(Long pSId, Object inTask, int taskId) throws Exception {

		/**
		 * The StoreAux task
		 */
		StoreAux task = new StoreAux();

		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper
					.parseBRMToDMTaskMark(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);
		}
		
		task.setTaskStatus(TaskStatus.Unchecked);
		task.setRemovableFlag(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).isRemovableFlag());
		task.setPacketStoreId(((com.nais.spla.brm.library.main.ontology.tasks.StoreAUX) inTask).getPacketStoreId());
		task.setEnableFlag(((com.nais.spla.brm.library.main.ontology.tasks.StoreAUX) inTask).isEnableFlag());
		if (task.getEnableFlag() == null) {
			
			task.setEnableFlag(false);
		}
		
		return task;
	}

	/**
	 * Plan Store Task 
	 *
	 * @param pSId
	 * @param inTask
	 * @param taskId
	 * @param acqList
	 * @param iter
	 * @return
	 * @throws Exception
	 */
	public static Store planStoreTask(Long pSId, Object inTask, int taskId, ArrayList<Acquisition> acqList, 
			int iter) throws Exception {

		/**
		 * The Store Task
		 */
		Store task = new Store();

		task.setTaskId(taskId);
		task.setSatelliteId(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getSatelliteId());
		task.setStartTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getStartTime());
		task.setStopTime(((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getEndTime());
		if (((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark() != null) {

			task.setTaskMark(ObjectMapper.parseBRMToDMTaskMark(
					((com.nais.spla.brm.library.main.ontology.tasks.Task) inTask).getTaskMark()));
		} else {

			task.setTaskMark(TaskMarkType.NOMINAL);
		}
		task.setTaskStatus(TaskStatus.Unchecked);
		task.setRemovableFlag(((Storage) inTask).isRemovableFlag());

		logger.trace("Set Storage Ids.");
		
		String[] ids = ((com.nais.spla.brm.library.main.ontology.tasks.Storage) inTask).getIdTask()
				.split(Configuration.splitChar);
		String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
				.get(ObjectMapper.parseDMToSchedPRId(ids[0], ids[1])).get(0);
		task.setProgrammingRequest(ids[0], pRListId, ids[1]);
		task.setAcquisitionRequestId(ids[2], ids[0], pRListId, ids[1]);
		task.setDtoId(ids[3], ids[2], ids[0], pRListId, ids[1]);
		task.setDi2s(ObjectMapper.parseBRMToDMDi2sInfo(pSId, 
				((com.nais.spla.brm.library.main.ontology.tasks.Storage) inTask).getDi2sInfo(), taskId,
				((com.nais.spla.brm.library.main.ontology.tasks.Storage) inTask).getIdTask()));

		logger.trace("Set Storage packet store data.");
		
		task.setPacketStoreSizeH(null);
		task.setPacketStoreSizeV(null);
//		task.setSourcePacketNumberH(0L);
//		task.setSourcePacketNumberV(0L);
		
		// Changed on 24/03/2022 for DI2S polarization management
		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(
		((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()) 
				&& PRListProcessor.dtoSchedIdMap.get(pSId).get(
				((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()).getPolarization() != null) {
										
			task.setPolarization(PRListProcessor.dtoSchedIdMap.get(pSId)
					.get(((com.nais.spla.brm.library.main.ontology.tasks.Task)inTask).getIdTask()).getPolarization());
				
		} else  {
			
			task.setPolarization(ObjectMapper.parseBRMToDMPolar(((Storage) inTask).getPol()));
		}
		
		// Set Memory Modules
		task.setMemoryModules(getStoMMList(pSId, (Storage) inTask));

		if (((Storage) inTask).getPacketsAssociated() != null) {

			for (int i = 0; i < ((Storage) inTask).getPacketsAssociated().size(); i++) {

				/**
				 * The storage packet store
				 */
				PacketStore packetStore = ((Storage) inTask).getPacketsAssociated().get(i);
				
				/**
				 * The storage packet Id
				 */
				Long packetId = (long) iter;
				
				// Handle multiple polarization
				packetId += i;
				
				if (packetStore.getPolarization() != null) {

					if (packetStore.getPolarization().equals(Polarization.HH)) {

						// Set Packet Store Id
						task.setPacketStoreIdH(packetId);
						// Set Packet Store size (sectors)
						task.setPacketStoreSizeH((long) (((com.nais.spla.brm.library.main.ontology.tasks.Storage) inTask)
										.getNumberOfSectors(Polarization.HH)));
//						// Set Packet Store number // TODO: as SPARC property from 17/12/2019					
//						task.setSourcePacketNumberH(1L);

					} else if (packetStore.getPolarization().equals(Polarization.VV)) {

						// Set Packet Store Id
						task.setPacketStoreIdV(packetId);
						// Set Packet Store size (sectors)
						task.setPacketStoreSizeV((long) (((com.nais.spla.brm.library.main.ontology.tasks.Storage) inTask)
										.getNumberOfSectors(Polarization.VV)));
//						// Set Packet Store number // TODO: as SPARC property from 17/12/2019		
//						task.setSourcePacketNumberV(1L);
					}
				}
			}
		}
		
		logger.trace("Add Storage SPARC info...");
		TaskInfoParser.setStoreInfo(task, pSId);
		
		return task;
	}

	/**
	 * Get UGS owners from Partners Id list
	 * 
	 * @param pSId
	 * @param ownerIdList
	 * @param ugsId
	 * @return
	 */
	public static List<UgsOwner> getUgsOwners(Long pSId, List<String> ownerIdList, String ugsId) {
		
		/**
		 * The list of UGS owners
		 */
		List<UgsOwner> ugsOwnerList = new ArrayList<>();

		for (String partnerId : ownerIdList) {

			for (com.telespazio.csg.spla.csps.model.impl.Partner partner : SessionActivator.partnerListMap.get(pSId)) {
				
				if (partner.getTupIdList().contains(ugsId)) {
					
					logger.info("Set TUP UGS " + ugsId + " for owner: " + partner.getId());
					
					/**
					 * The ugs owner
					 */
					UgsOwner ugsOwner = new UgsOwner();
					ugsOwner.setOwnerId(partner.getId());
					ugsOwner.setUgsId(ugsId);
					ugsOwnerList.add(ugsOwner);
					
					break;
				
				} else if (partner.getId().equals(partnerId)) {
					
					logger.debug("Set Partner UGS " + partner.getUgsId() + " for owner: " + partner.getId());
					
					/**
					 * The ugs owner
					 */
					UgsOwner ugsOwner = new UgsOwner();
					ugsOwner.setOwnerId(partner.getId());
					ugsOwner.setUgsId(partner.getUgsId());
					ugsOwnerList.add(ugsOwner);
				}
			}
		}

		return ugsOwnerList;
	}

	/**
	 * Get UGS owners from Partner Id list
	 *
	 * @param pSId
	 * @param partnerIdList
	 * @return
	 */
	private static List<UgsOwner> getGPSUgsOwners(Long pSId) {
		
		/**
		 * The list of UGS owners
		 */
		List<UgsOwner> ugsOwnerList = new ArrayList<>();

		UgsOwner ugsOwner = new UgsOwner();
		ugsOwner.setOwnerId("2000");
		ugsOwner.setUgsId("200");

		ugsOwnerList.add(ugsOwner);

		return ugsOwnerList;
	}
	
	/**
	 * Set packet store data
	 * Hp: Packet Store total parts found in the current Mission Horizon + delta time
	 *
	 * @param pSId
	 * @param task
	 * @param inTask
	 * @param stoList
	 * @return
	 */
	public static Download setDwlPacketStoreData(Long pSId, Download task,
			com.nais.spla.brm.library.main.ontology.tasks.Download inTask,
			ArrayList<Store> stoList) {

			
		logger.debug("Set Packet Store Data for Download related to DTO: " + inTask.getRelatedTaskId());
	
		try {
			
			/**
			 * The data strategy
			 */
			Boolean dataStrategy = false;
			
			// Set Packet Store data strategy
			if (((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPacketStoreStrategy()
					.equals(DownlinkStrategy.DELETE)) {
	
				dataStrategy = true;			
			}
	
			task.setDataStrategy(dataStrategy);
			
			// Set Packet Store part number
			task.setPacketStorePartNumber((byte) 1);
				
			/**
			 * The Packet Store total parts number
			 */
			// Refined  on 20/01/2022 for DownlinkStrategy management in case of stitching
			 int totalParts = computeDwlParts(pSId, 
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getRelatedTaskId(),
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPol(), 
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getUgsOwnerList().get(0),
					((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask).getPacketStoreStrategy());
			
			// Set Packet Store total parts
			task.setPacketStoreTotalParts((byte) totalParts);

			// Set Packet Store Id
			task.setPacketStoreId(getDwlPacketStoreId(pSId, task, inTask.getPol(), stoList));
				
			/**
			 * The related acquisition
			 */
			ArrayList<String> acqIdList = RulesPerformer.getPlannedDTOIds(pSId);
			
			String acqId = null;
			
			if (acqIdList.contains(inTask.getRelatedTaskId())) {
				
				acqId = inTask.getRelatedTaskId();
			
			} else {
				
				logger.debug("Acquisition Id not found for Download: " + inTask.getRelatedTaskId());
			}
			
			// Set Packet Store data
			if (inTask.getPol().equals(Polarization.HH)) {
	
				// Set Packet Store size (sectors)
				task.setPacketStoreSize(BigDecimal.valueOf(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask)
								.getDownloadedSize()));
				
				// Set Packet Store numbers H 						
				if (acqId != null) {
					
					if (inTask.getPacketStoreStrategy().equals(DownlinkStrategy.DELETE)) {

						task.setPacketStorePartNumber((byte) totalParts);
						
					} else if (inTask.getInitialSector() > 1) {
						
						task.setPacketStorePartNumber((byte) 2);
					
					} else {
						
						task.setPacketStorePartNumber((byte) 1);
					}
					
				} else if (inTask.getPacketStoreStrategy().equals(DownlinkStrategy.DELETE)) {
					
					task.setPacketStorePartNumber((byte) totalParts);				
				}
		
			} else if (inTask.getPol().equals(Polarization.VV)) {
				
				// Set Packet Store size (sectors)
				task.setPacketStoreSize(BigDecimal.valueOf(((com.nais.spla.brm.library.main.ontology.tasks.Download) inTask)
								.getDownloadedSize()));
				
				// Set Packet Store numbers V
				if (acqId != null) {	
									
					if (inTask.getPacketStoreStrategy().equals(DownlinkStrategy.DELETE)) {
						
						task.setPacketStorePartNumber((byte) totalParts);
					
					} else if (inTask.getInitialSector() > 1) {
						
						task.setPacketStorePartNumber((byte) 2);
					
					} else {
						
						task.setPacketStorePartNumber((byte) 1);
					}							

				} else if (inTask.getPacketStoreStrategy().equals(DownlinkStrategy.DELETE)) {
					
					task.setPacketStorePartNumber((byte) totalParts);			
				}
			}
			
			if (task.getPacketStorePartNumber() == null || task.getPacketStorePartNumber() == 0) {
				
				task.setPacketStorePartNumber((byte) 1);
				logger.warn("Data for download " + inTask.getIdTask() + " not found!");
				logger.warn("Packet Store Part Number for download " + inTask.getIdTask() + " set to 1 by default.");			
			}
			
			if (task.getPacketStoreTotalParts() == 0) {

				task.setPacketStoreTotalParts((byte) 1);
				logger.warn("Data for download " + inTask.getIdTask() + " not found!");
				logger.warn("Packet Store Total Parts for download " + inTask.getIdTask() + " set to 1 by default.");			
			}
			
			logger.debug("Final strategy " + inTask.getPacketStoreStrategy()
				+ " of part number " + task.getPacketStorePartNumber() 
				+ " with total parts " + totalParts + " set for download " + inTask.getIdTask());
			
		} catch (Exception ex) {

			ex.printStackTrace();
			
			logger.error("Exception raised: "  + ex.getStackTrace()[0].toString());
		}	

		return task;
	}
	
	/**
	 * Compute download parts relevant to an acquisition polarization
	 * @param pSId
	 * @param schedDTOId
	 * @param polar
	 * @param ownerId
	 * @param dwlStrategy
	 * @return
	 */
	public static int computeDwlParts(Long pSId, String schedDTOId, Polarization polar, 
			String ownerId, DownlinkStrategy dwlStrategy) {
		
		/**
		 * Instance handlers
		 */
		TaskPlanned taskPlanned = new TaskPlanned();
		
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		/**
		 * The Packet Store total parts number
		 */
		int totalParts = 0;
		
		/**
		 * The retain Strategy parts
		 */
		int retainParts = 0;		
		
		try {
		
			/**
			 * The list of downloads associated to DTO
			 */
			if (! schedDTOId.contains("null")) {

				/**
				 * The list of Downloads
				 */
				List<com.nais.spla.brm.library.main.ontology.tasks.Download> brmDwlist = taskPlanned.receiveAllDwlAssociatedToDto(
						schedDTOId, pSId.toString(), RulesPerformer.brmInstanceMap.get(pSId), RulesPerformer.brmParamsMap.get(pSId)); 
		
				for (com.nais.spla.brm.library.main.ontology.tasks.Download brmDwl : brmDwlist) {
					
					if (polar.equals(brmDwl.getPol())) {

						// Added on 13/07/2021 for international packet store total parts management
						// Updated on 29/08/2022
						if (PRListProcessor.pRIntBoolMap.get(pSId).containsKey(ObjectMapper.getSchedPRId(schedDTOId))
								&& PRListProcessor.pRIntBoolMap.get(pSId).get(ObjectMapper.getSchedPRId(schedDTOId))
								&& RequestChecker.isDefence(ObjectMapper.getUgsId(schedDTOId) )
								&& (equivDTOHandler.getDI2SVisibility(pSId, brmDwl) == 0)) {	
						
							for (String ugsOwner : brmDwl.getUgsOwnerList()) {
								
								if (ugsOwner.equals(ownerId)) {
									
									totalParts ++;
									
									logger.debug("Found matching download for international DTO " 
											+ schedDTOId + " and owner " + ownerId);
									
									// Added on 07/04/2022 to extend anomalies in RETAIN DownlinkStrategy
									if (brmDwl.getPacketStoreStrategy().equals(DownlinkStrategy.RETAIN)) {
									
										retainParts ++;
									}
								}
							}
						
						} else  {
								
							logger.debug("Found matching download for nominal DTO " 
									+ schedDTOId + " and owner " + ownerId);
							
							totalParts ++;
							
							// Added on 07/04/2022 to extend anomalies in RETAIN DownlinkStrategy
							if (brmDwl.getPacketStoreStrategy().equals(DownlinkStrategy.RETAIN)) {
							
								retainParts ++;
							}
						}
					}
				}
				
			} else { // GPS
				
				totalParts = 1;	
			}
			
//			// Added on 05/01/2022 to manage anomalies in RETAIN DownlinkStrategy
//			// Removed on 07/04/2022 
//			if (totalParts == 1 && dwlStrategy.equals(DownlinkStrategy.RETAIN)) {
//				
//				logger.debug("Update at 2 the total parts for the Download of DTO: " + schedDTOId);
//				
//				totalParts = 2;			
//			}
			
			// Added on 07/04/2022 to extend anomalies in RETAIN DownlinkStrategy
			if (totalParts == retainParts) {
				
				totalParts = retainParts + 1;
				
				logger.debug("Update to " + totalParts 
						+ " the total parts for the Download of DTO: " + schedDTOId);							
			}

		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getMessage());
		}
		
		return totalParts;
	}
	
	/**
	 * Set PassThtough packet store data
	 * 
	 * @param pSId
	 * @param task
	 * @param inTask
	 * @param packetId
	 * @return
	 */
	public static PassThrough setPTPacketStoreData(Long pSId, PassThrough task,
			com.nais.spla.brm.library.main.ontology.tasks.PassThrough inTask, Long packetId) {
		
		logger.debug("Set Packet Store Data for Download related to DTO: " + inTask.getAcquisitionRequestId());
				
		// Set Packet Store data strategy Hp: always delete
		task.setDataStrategyH(true);
		task.setDataStrategyV(true);
		
		// Set Delay (0.0 by default)
		task.setDelayH(0.0);
		task.setDelayV(0.0);

		// Set Packet Store part number 
		task.setPacketStorePartNumber((byte) 1);
		
		// Set Packet Store total parts
		task.setPacketStoreTotalPart((byte) 1);
			
		task.setPacketStoreSizeH(null);
		task.setPacketStoreSizeV(null);

		
		if (inTask.getPacketStoreSizeH() > 0) {

			// Set Packet Store size (sectors)
			task.setPacketStoreSizeH(BigDecimal.valueOf(
					((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask)
					.getPacketStoreSizeH()));	

			// Set Packet Store Number			
			task.setPacketStoreHId(packetId.toString());
			
			packetId ++;
		}
	
		if (inTask.getPacketStoreSizeV() > 0) {

			// Set Packet Store size (sectors)
			task.setPacketStoreSizeV(BigDecimal.valueOf(
					((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) inTask)
					.getPacketStoreSizeV()));

			// Set Packet Store Number
			task.setPacketStoreVId(packetId.toString());
		}

		return task;
	}
	
	/**
	 * Get the Download packet store Id
	 * 
	 * @param pSId
	 * @param task
	 * @param pol
	 * @param stoList
	 * @return
	 */
	private static String getDwlPacketStoreId(Long pSId, Task task, Polarization pol, 
			ArrayList<Store> stoList) {

		logger.trace("Get Packet Store of the relevant storage for Download: " + task.getTaskId());
		
		// working tasks
		for (Store sto : stoList) {
		
			if (sto.getUgsId().equals(task.getUgsId())
					&& sto.getProgrammingRequestId().equals(task.getProgrammingRequestId())
					&& sto.getAcquisitionRequestId().equals(task.getAcquisitionRequestId())
					&& sto.getDtoId().equals(task.getDtoId())) {
	
				// Check polarization
				if (pol.equals(Polarization.HH) && 
						!sto.getTaskMark().equals(TaskMarkType.DELETED)) {
					
					// Return PacketStoreId HH
					return Long.toString(sto.getPacketStoreIdH());
				
				} else if (pol.equals(Polarization.VV) && 
						!sto.getTaskMark().equals(TaskMarkType.DELETED)) {
					
					// Return PacketStoreId VV
					return Long.toString(sto.getPacketStoreIdV());
	
				}
			}
		}

		// Check reference tasks
		if (PersistPerformer.refTaskListMap.containsKey(pSId)) {
			
			for (Task refTask : PersistPerformer.refTaskListMap.get(pSId)) {
				
				if (refTask.getTaskType().equals(TaskType.STORE)) {
					
					if (refTask.getUgsId().equals(task.getUgsId())
							&& refTask.getProgrammingRequestId().equals(task.getProgrammingRequestId())
							&& refTask.getAcquisitionRequestId().equals(task.getAcquisitionRequestId())
							&& refTask.getDtoId().equals(task.getDtoId())) {
						
						// Check polarization
						if (pol.equals(Polarization.HH)) {
							
							// Return PacketStoreId H
							return Long.toString(((Store) refTask).getPacketStoreIdH());
						
						} else if (pol.equals(Polarization.VV)) {
							
							// Return PacketStoreId V
							return Long.toString(((Store) refTask).getPacketStoreIdV());		
						}					
					}
				}
			}
		}
		
		// Null packet store id
		logger.warn("No packet store Id found for Task: " + task.getTaskId() + " of DTO: " + ObjectMapper.parseDMToSchedDTOId(
				task.getUgsId(), task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()));
		
		return "0";
	}

	/**
	 * Get the storage list of MMs
	 * // TODO: review according  to MM handling
	 *
	 * @param pSId
	 * @param sto
	 * @return
	 * @throws Exception 
	 */
	public static List<Integer> getStoMMList(Long pSId, Storage sto) throws Exception {
		
		/**
		 * The list of MM storage values
		 */
		Integer[] mms = {0, 0, 0, 0, 0, 0};

		for (PacketStore packet : sto.getPacketsAssociated()) {
			
			/**
			 * The Memory Module iterator
			 */
			Iterator<Map.Entry<MemoryModule, Long>> it = packet.getPlannedOnMemModule().entrySet().iterator();
	
			if (it != null) {
	
				while (it.hasNext()) {
	
					/**
					 * The Memory Module Entry
					 */
					Entry<MemoryModule, Long> mmEntry = it.next();
	
					if (mmEntry != null) {
	
						if (mmEntry.getKey().getId().equals("mm1")) {
	
							mms[0] += (mmEntry.getValue().intValue());
	
						} else if (mmEntry.getKey().getId().equals("mm2")) {
	
							mms[1] += (mmEntry.getValue().intValue());
	
						} else if (mmEntry.getKey().getId().equals("mm3")) {
	
							mms[2] += (mmEntry.getValue().intValue());
	
						} else if (mmEntry.getKey().getId().equals("mm4")) {
	
							mms[3] += (mmEntry.getValue().intValue());
	
						} else if (mmEntry.getKey().getId().equals("mm5")) {
	
							mms[4] += (mmEntry.getValue().intValue());
	
						} else if (mmEntry.getKey().getId().equals("mm6")) {
	
							mms[5] += (mmEntry.getValue().intValue());
						}
					}
				}
			}
		}
		
		return Arrays.asList(mms);
	}
	
	/**
	 * Get the storage list of MMs
	 * // TODO: review according to MM handling
	 *
	 * @param pSId
	 * @param sto
	 * @param pol
	 * @return
	 * @throws Exception 
	 */
	public static List<Integer> getDwlMMList(Long pSId, Storage sto, Polarization pol) throws Exception {
		
		/**
		 * The list of MM storage values
		 */
		Integer[] mms = {0, 0, 0, 0, 0, 0};

		for (PacketStore packet : sto.getPacketsAssociated()) {
			
			if (packet.getPolarization().equals(pol)) {
			
				/**
				 * The Memory Module iterator
				 */
				Iterator<Map.Entry<MemoryModule, Long>> it = packet.getPlannedOnMemModule().entrySet().iterator();
		
				if (it != null) {
		
					while (it.hasNext()) {
		
						/**
						 * The Memory Module Entry
						 */
						Entry<MemoryModule, Long> mmEntry = it.next();
		
						if (mmEntry != null) {
		
							if (mmEntry.getKey().getId().equals("mm1")) {
		
								mms[0] += (mmEntry.getValue().intValue());
		
							} else if (mmEntry.getKey().getId().equals("mm2")) {
		
								mms[1] += (mmEntry.getValue().intValue());
		
							} else if (mmEntry.getKey().getId().equals("mm3")) {
		
								mms[2] += (mmEntry.getValue().intValue());
		
							} else if (mmEntry.getKey().getId().equals("mm4")) {
		
								mms[3] += (mmEntry.getValue().intValue());
		
							} else if (mmEntry.getKey().getId().equals("mm5")) {
		
								mms[4] += (mmEntry.getValue().intValue());
		
							} else if (mmEntry.getKey().getId().equals("mm6")) {
		
								mms[5] += (mmEntry.getValue().intValue());
							}
						}
					}
				}
			}
		}
		
		return Arrays.asList(mms);
	}
		
	/**
	 * Fill the data relevant to the incoming download products according 
	 * to the relevant DLOs.
	 * 
	 * @param pSId
	 *            - the Planning Session Id
	 * @param dwlList
	 *            - the list of output downloads
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<Download> fillDwlData(Long pSId, ArrayList<Download> dwlList) {
		
		/**
		 * Instance handlers
		 */
		EquivDTOHandler equivDTOHandler = new EquivDTOHandler();
		
		/**
		 * The final download list
		 */
		ArrayList<Download> finDwlList = new ArrayList<Download>();
		
		logger.debug("Fill the Download Tasks according to the MacroDLO data.");
		
		try {
		
			// Sort downloads by start time
			Collections.sort(dwlList, new TaskStartTimeComparator());
			
			// Associate download auxiliary data
			if (SessionScheduler.macroDLOListMap.get(pSId) != null) {
				
				for (MacroDLO macroDLO : SessionScheduler.macroDLOListMap.get(pSId)) {
					/**
					 * The initial offset
					 */
					int initOffset = macroDLO.getPacketSeqId();
					
					/**
					 * The useful variables
					 */
					double packetTime = macroDLO.getStartTime().getTime();
					
					String satId = macroDLO.getSatId();
					
					String prevUgsId = "";
					
					String stationId = "";
					
					Boolean isPrevL2 = false;
					
					/**
					 * The list of previous owners
					 */
					List<UgsOwner> prevUgsOwnerList = null;
					
					/**
					 * The list of macro DLO
					 */
					ArrayList<Download> macroDwlList = getMacroDwlList(macroDLO, dwlList);
					
					// Sort Macro downloads by carrier selection and start time 
					Collections.sort(macroDwlList, new DwlOffsetComparator());
					
					if (! macroDwlList.isEmpty()) {
						
						/**
						 * The scheduling AR Id of the initial Download 
						 */
						String initSchedARId = null;
						
						if (macroDwlList.get(0).getProgrammingRequestId() != null) {
							
							/**
							 * The scheduling AR Id of the initial Download 
							 */
							initSchedARId = ObjectMapper.parseDMToSchedARId(macroDwlList.get(0).getUgsId(), 
									macroDwlList.get(0).getProgrammingRequestId(), macroDwlList.get(0).getAcquisitionRequestId());						
						}
						
						// The download counter
						int i = 0;
						
						for (Download dwl : macroDwlList) {
							
							/**
							 * The separation boolean
							 */
							boolean isSeparated = false;
							
							if (dwl.getUgsOwnerList() != null && dwl.getUgsOwnerList().size() > 1) {
								
								isSeparated = true;
						
							} else if (prevUgsOwnerList != null && prevUgsOwnerList.size() > 1) {
																	
								isSeparated = true;
							}
						
							// Check packet store initial offset update				
							if (dwl.getProgrammingRequestId() != null) {
								
								if (checkOffsetUpdate(pSId, prevUgsId, satId, stationId, 
										isPrevL2.booleanValue(), packetTime, dwl, isSeparated)) {
								
									// Set scheduling AR Id of initial Download							
									initSchedARId = ObjectMapper.parseDMToSchedARId(dwl.getUgsId(), 
											dwl.getProgrammingRequestId(), dwl.getAcquisitionRequestId());
									
									initOffset =  i;
									
								} else {
										
									// Change download Encryption Info according to the initial Download
									TaskInfoParser.setDwlEnchryptionInfo(pSId, dwl, initSchedARId);

								}
								
							} else { // GPS case
								
								initOffset = i;
							}
								
							// Update variables									
							packetTime = dwl.getStopTime().getTime();							
							prevUgsId = dwl.getUgsId();
							prevUgsOwnerList = dwl.getUgsOwnerList();
							stationId = dwl.getAcquisitionStationId();
							satId = dwl.getSatelliteId();
							isPrevL2 = dwl.getCarrierL2Selection();
											
							dwl.setPacketStoreInitialOffset((byte) initOffset);
								
							logger.info("Product " + dwl.getTaskId() + " starting at " + dwl.getStartTime().toString()
									+ " ending at " + dwl.getStopTime().toString() + " on carrier L2 " + dwl.getCarrierL2Selection()
									+ " has PSIO: " + ((Download)dwl).getPacketStoreInitialOffset());
												
							i ++;
						}
						
						finDwlList.addAll((ArrayList<Download>) macroDwlList.clone());
					}
				
					macroDLO.setPacketSeqId(initOffset);
				}
			}
			
		} catch (Exception ex) {
			
			logger.error("Exception raised: " + ex.getMessage());
		}	

		return finDwlList;
	}
	
	/**
	 * Get the list of downloads within the MacroDLO
	 * @param macroDLO
	 * @param dwlList
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Download> getMacroDwlList(MacroDLO macroDLO, ArrayList<Download> dwlList) {
		
		/**
		 * The list of downloads within the MacroDLO
		 */
		ArrayList<Download> macroDwlList = new ArrayList<Download>();
			
		for (Task dwlTask : (ArrayList<Task>) dwlList.clone()) {
		
			if (dwlTask.getTaskType().equals(TaskType.DWL)) {
			
				if (dwlTask.getSatelliteId().equals(macroDLO.getSatId())
						&& (dwlTask.getStartTime().compareTo(macroDLO.getStartTime()) >= 0)
						&& (dwlTask.getStopTime().compareTo(macroDLO.getStopTime()) <= 0)) {
					
					macroDwlList.add((Download)dwlTask);
				
				} else if (dwlTask.getStartTime().getTime() > macroDLO.getStopTime().getTime()) { // Exit condition
					
					break;
				}
			}
		}
			
		return 	macroDwlList;
			
	}
	
	/**
	 * Check the packet store initial offset update
	 * // TODO: check update wrt encryption keys for subscribers
	 * 
	 * @param pSId
	 * @param prevUgsId
	 * @param satId
	 * @param stationId
	 * @param packetTime
	 * @param dwl 
	 * @param isSeparated
	 * @return
	 */
	private boolean checkOffsetUpdate(Long pSId, String prevUgsId, String satId, String stationId,
			 boolean isPrevL2, double packetTime, Download dwl, boolean isSeparated) {
		
		// dwl properties updated at 11/05 for encryption
		if ((!dwl.getUgsId().equals(prevUgsId))) { // ugs/subscription case
		
			isSeparated = true;
		}
		
		if (!dwl.getCarrierL2Selection().equals(isPrevL2)) {
			
			isSeparated = true;
		}
		
		if (dwl.getStartTime().getTime() > (packetTime + Configuration.minDwlOffsetTime)) { // delta time case (RETAIN included)
			
			isSeparated = true;
		}
		
		if (!dwl.getAcquisitionStationId().equals(stationId)) { // delta station (RETAIN included)
			
			isSeparated = true;
		}
		
		return isSeparated;
	}

	/**
	 * Check and update time in case of overlap with a MacroDLO
	 * @param pSId
	 * @param planStartTIme
	 * @return
	 */
	long checkDLOOverlap(Long pSId, long time) {
		
		long marginTime = 10000;
		
		for (MacroDLO macroDLO : SessionScheduler.macroDLOListMap.get(pSId)) {
			
			if (macroDLO.getStartTime().getTime() - marginTime <= time 
					&& macroDLO.getStopTime().getTime() + marginTime >= time) {
				
				time = macroDLO.getStartTime().getTime() - marginTime; // TODO: TBD
			}
		}
		
		return time;
	}
	
}

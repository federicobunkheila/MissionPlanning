/**
*
* MODULE FILE NAME: Predecessor.java
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.processor.SessionActivator;

import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.task.DLO;
import it.sistematica.spla.datamodel.core.model.task.Download;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.datamodel.core.model.task.PassThrough;
import it.sistematica.spla.datamodel.core.model.task.Predecessor;
import it.sistematica.spla.datamodel.core.model.task.Ramp;

/**
 * The predecessor task counter
 *
 * @author bunkheila
 */
public class PredecessorCounter {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(PredecessorCounter.class);

	/**
	 * Assign the Tasks predecessors
	 *
	 * // TODO: manage multiple Downloads Ids // TODO: check Download predecessors!
	 *
	 * @param pSId
	 * @param taskList
	 * @param dloList
	 */
	public void assignPredecessors(Long pSId, ArrayList<Task> taskList) {

		logger.info("Assign scheduled Tasks predecessors for Planning Session: " + pSId);

		/**
		 * Predecessor elements
		 */
		Long predPSRampId = 0L;
		BigDecimal predRampId = new BigDecimal(pSId);
		Long predPSManId = 0L;
		BigDecimal predManId = new BigDecimal(pSId);
		Long predPSAcqId = 0L;
		BigDecimal predAcqId = new BigDecimal(pSId);
		Long predPSStoId = 0L;
		BigDecimal predStoId = new BigDecimal(pSId);
		Long predPSDwlId = 0L;
		BigDecimal predDwlId = new BigDecimal(pSId);

		/**
		 * The maneuver flag
		 */
		boolean manFlag = false;
		/**
		 * The ramp flag
		 */	
		boolean rampFlag = true;

		/**
		 * The store map
		 */
		HashMap<String, HashMap<BigDecimal, Long>> storeMap = new HashMap<>();

		/**
		 * The download map
		 */
		HashMap<String, HashMap<BigDecimal, Long>> dwlMap = new HashMap<>();

//		/**
//		 * The reference list of tasks
//		 */
//		ArrayList<Task> refTaskList = new ArrayList<>();

		/**
		 * The list of DLOs
		 */
		ArrayList<DLO> dloList = new ArrayList<>();		
		
//		if (PersistPerformer.refTaskListMap.get(pSId) != null) {
//
//			refTaskList = PersistPerformer.refTaskListMap.get(pSId);
//
//			// Sort reference tasks lists
//			Collections.sort(refTaskList, new TaskStartTimeComparator());
//		}

		// Sort current tasks lists
		Collections.sort(taskList, new TaskStartTimeComparator());

		for (Task task : taskList) {
			
			if (task.getTaskType().equals(TaskType.DLO)) {
				
				// Add DLO
				dloList.add((DLO) task);
			}
		}

		Iterator<Entry<Long, ArrayList<Task>>> it = PersistPerformer.refPSTaskListMap
				.get(pSId).entrySet().iterator();

		while (it.hasNext()) {
			
			Entry<Long, ArrayList<Task>> refPSTaskListEntry = it.next();
			/**
			 * The reference Planning Session Id
			 */
			Long refPSId = refPSTaskListEntry.getKey();

			// Cycle reference tasks
			for (Task task : refPSTaskListEntry.getValue()) {

				// Collect task info about reference Planning Sessions
				if (task.getStartTime()
						.compareTo(SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime()) < 0) {

					if (task.getTaskType().equals(TaskType.MANEUVER)) {

						predManId = task.getTaskId();
						predPSManId = refPSId;

						manFlag = true;

					} else if (task.getTaskType().equals(TaskType.RAMP)) {

						if (((Ramp) task).getUpFlag() == true) {

							predRampId = task.getTaskId();
							predPSRampId = refPSId;

							rampFlag = true;
						}

					} else if (task.getTaskType().equals(TaskType.STORE)) {

						/**
						 * The store Id map
						 */
						HashMap<BigDecimal, Long> storeIdMap = new HashMap<>();
						storeIdMap.put(task.getTaskId(), refPSId);
						storeMap.put(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
								task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()),
								storeIdMap);

					} else if (task.getTaskType().equals(TaskType.DWL) 
							&& (task.getDtoId() != null)) {

						/**
						 * The download Id map
						 */
						HashMap<BigDecimal, Long> dwlIdMap = new HashMap<>();
						dwlIdMap.put(task.getTaskId(), refPSId);
						dwlMap.put(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(), 
								task.getProgrammingRequestId(), task.getAcquisitionRequestId(), 
								task.getDtoId()), dwlIdMap);
					}
				}
			}
		}

		// Cycle working tasks
		for (Task task : taskList) {

			/**
			 * The task predecessor
			 */
			Predecessor pred = new Predecessor();

			if (task.getTaskType().equals(TaskType.RAMP)) {

				if (((Ramp) task).getUpFlag() == true) {

					predRampId = task.getTaskId();
					predPSRampId = pSId;

					rampFlag = true;
				}
			}

			if (task.getTaskType().equals(TaskType.MANEUVER)) {

				predManId = task.getTaskId();
				predPSManId = pSId;

				manFlag = true;

				if (((Maneuver) task).getActuator().equalsIgnoreCase("CMGA") 
						&& rampFlag && (predPSRampId > 0)) {

					// Add ramp predecessor
					pred.setPlanningSessionId(predPSRampId);
					pred.setTaskId(predRampId);

					task.addPredecessor(pred);
					logger.trace("Predecessor added: " + pred);
				}

			} else if (task.getTaskType().equals(TaskType.ACQ)) {

				predAcqId = task.getTaskId();
				predPSAcqId = pSId;

				if (manFlag && (predPSManId > 0)) {

					// Add maneuver predecessor
					pred.setPlanningSessionId(predPSManId);
					pred.setTaskId(predManId);

					task.addPredecessor(pred);
					logger.trace ("Predecessor added: " + pred);
				}

			} else if (task.getTaskType().equals(TaskType.PASSTHROUGH)) {

				for (DLO dlo : dloList) {

					if (((PassThrough) task).getContactCounter().equals(dlo.getContactCounter())) {

						// Add DLO predecessor
						pred.setPlanningSessionId(pSId);
						pred.setTaskId(dlo.getTaskId());

						task.addPredecessor(pred);
						logger.trace("Predecessor added: " + pred);

						break;
					}
				}

			} else if (task.getTaskType().equals(TaskType.STORE)) {

				/**
				 * The store Id map
				 */
				HashMap<BigDecimal, Long> storeIdMap = new HashMap<>();
				storeIdMap.put(task.getTaskId(), pSId);

				storeMap.put(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(), task.getProgrammingRequestId(),
						task.getAcquisitionRequestId(), task.getDtoId()), storeIdMap);

				if (predPSAcqId > 0) {

					// Add acquisition predecessor
					pred.setPlanningSessionId(predPSAcqId);
					pred.setTaskId(predAcqId);

					task.addPredecessor(pred);
					logger.trace("Predecessor added: " + pred);

				}

			} else if (task.getTaskType().equals(TaskType.DWL) && (task.getUgsId() != null)) {

				/**
				 * The download Id map
				 */
				HashMap<BigDecimal, Long> dwlIdMap = new HashMap<>();
				dwlIdMap.put(task.getTaskId(), pSId);

				dwlMap.put(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(), task.getProgrammingRequestId(),
						task.getAcquisitionRequestId(), task.getDtoId()), dwlIdMap);

				// Add store predecessor
				if (storeMap.containsKey(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
						task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))) {

						predStoId = storeMap.get(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
							task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))
							.keySet().iterator().next();
					
						predPSStoId = storeMap.get(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
							task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))
							.get(predStoId);

					if (!isPredecessor(task, predStoId) && predPSStoId > 0) {

						pred.setPlanningSessionId(predPSStoId);
						pred.setTaskId(predStoId);

						task.addPredecessor(pred);
						logger.trace("Predecessor added: " + pred);											
					}
				}

				// Add partial download predecessor
				if (((Download) task).getPacketStorePartNumber() > 1) {

					if (dwlMap.containsKey(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
							task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))) {

						predDwlId = dwlMap.get(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
								task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))
								.keySet().iterator().next();

						predPSDwlId = dwlMap.get(ObjectMapper.parseDMToSchedDTOId(task.getUgsId(),
								task.getProgrammingRequestId(), task.getAcquisitionRequestId(), task.getDtoId()))
								.get(predDwlId);

						if (predPSDwlId > 0) {

							pred.setPlanningSessionId(predPSDwlId);
							pred.setTaskId(predDwlId);

							task.addPredecessor(pred);
							logger.trace("Predecessor added: " + pred);
						}
					}
				}

				// Add DLO predecessor
				for (DLO dlo : dloList) {

					/**
					 * The DLO predecessor
					 */
					Predecessor pred2 = new Predecessor();

					if (((Download) task).getContactCounter().equals(dlo.getContactCounter())) {

						pred2.setPlanningSessionId(pSId);
						pred2.setTaskId(dlo.getTaskId());

						task.addPredecessor(pred2);
						logger.trace("Predecessor added: " + pred);

						break;
					}
				}
			}
		}
	}


	/**
	 * 
	 * @param task
	 * @param predId
	 * @return
	 */
	private boolean isPredecessor(Task task, BigDecimal predId) {

		/**
		 * The predecessor boolean
		 */
		boolean isPred = false;

		for (Predecessor pred : task.getPredecessors()) {

			if (pred.getTaskId().equals(predId)) {

				// Set predecessor
				isPred = true;

				break;
			}
		}

		return isPred;
	}
}

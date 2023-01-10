/**
*
* MODULE FILE NAME: IntMatrixPerformer.java
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.SessionScheduler;

import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.model.Task;

/**
 * The handler of the Intersection Matrix of the AR DTOs for the GS strategy.
 *
 * @author bunkheila
 *
 */
public class IntMatrixCalculator {

	/**
	 * The proper logger
	 */
	protected static Logger logger = LoggerFactory.getLogger(IntMatrixCalculator.class);

	/**
	 * The intersection matrix map
	 */
	public static HashMap<Long, HashMap<String, Map<String, Integer>>> intDTOMatrixMap;

	/**
	 * The intersection matrix map
	 */
	public static HashMap<Long, HashMap<String, Map<String, Integer>>> intTaskMatrixMap;

//	/**
//	 * Build the Intersection Matrix of the DTOs for a given PRList
//	 *
//	 * @param pSId
//	 * @param pRList
//	 * @return
//	 * @throws Exception
//	 */
//	public void buildPRDTOIntMatrix(Long pSId, ArrayList<ProgrammingRequest> pRList) throws Exception {
//
//		/**
//		 * The list of intersected DTOs
//		 */
//		ArrayList<SchedDTO> intDTOList = new ArrayList<>();
//
//		for (ProgrammingRequest pR : pRList) {
//
//			for (AcquisitionRequest aR : pR.getAcquisitionRequestList()) {
//				 
//				// Add scheduling DTO list
//				intDTOList.addAll(ObjectMapper.parseDMToSchedDTOList(pSId, pR.getUserList().get(0).getUgsId(),
//						pR.getProgrammingRequestId(), aR.getAcquisititionRequestId(), aR.getDtoList(),
//						pR.getUserList().get(0).getAcquisitionStationIdList(), false));
//			}
//		}
//	}

	/**
	 * Build the Intersection Matrix of the DTOs ordered by rank
	 *
	 * @param rankedDTOList
	 *            - the ranked list of DTOs
	 * @return the Intersection Matrix
	 */
	public void buildDTOIntMatrix(Long pSId, ArrayList<SchedDTO> rankedDTOList) {

		for (int i = 0; i < rankedDTOList.size(); i++) {

			/**
			 * The Ids array
			 */
			String[] ids = new String[rankedDTOList.size() - 1];
			
			/**
			 * The intersection matrix data array
			 */		
			Map<String, Integer> intMatrixData = new HashMap<>();

			/**
			 * The conflict data array
			 */
			int[] conflictDatas = new int[rankedDTOList.size() - 1];

			/**
			 * The Id counter
			 */			
			int jj = 0;

			for (int j = 0; j < rankedDTOList.size(); j++) {

				if (i != j) {

					ids[jj] = rankedDTOList.get(j).getDTOId();

					conflictDatas[jj] = findDTOIntConflict(pSId, rankedDTOList.get(i), rankedDTOList.get(j));

					intMatrixData.put(ids[jj], conflictDatas[jj]);

					jj++;
				}
			}

			if ((ids.length > 0) && (ids[0] != null)) {

				if (!ids[jj - 1].isEmpty()) {

					intDTOMatrixMap.get(pSId).put(rankedDTOList.get(i).getDTOId(), intMatrixData);
				}
			}
		}
	}

	/**
	 * Build the Intersection Matrix of Tasks
	 *
	 * @param pSId
	 *            - the planning session Id
	 * @param taskList
	 *            - the ranked list of DTOs
	 */
	public void buildTaskIntMatrix(Long pSId, ArrayList<Task> taskList) {

		for (int i = 0; i < taskList.size(); i++) {

			/**
			 * The Ids array
			 */
			String[] ids = new String[taskList.size() - 1];

			/**
			 * The intersection matrix data array
			 */			
			Map<String, Integer> intMatrixData = new HashMap<>();

			/**
			 * The conflict data array
			 */
			int[] conflictDatas = new int[taskList.size() - 1];
			
			/**
			 * The Id counter
			 */	
			int jj = 0;

			for (int j = 0; j < taskList.size(); j++) {

				if (i != j) {

					ids[jj] = ObjectMapper.parseDMToSchedDTOId(taskList.get(j).getUgsId(),
							taskList.get(j).getProgrammingRequestId(), taskList.get(j).getAcquisitionRequestId(),
							taskList.get(j).getDtoId());

					conflictDatas[jj] = findTaskIntConflict(pSId, taskList.get(i), taskList.get(j));

					intMatrixData.put(ids[jj], conflictDatas[jj]);

					jj++;

				}
			}

			if ((ids.length > 0) && (ids[0] != null)) {

				if (!ids[jj - 1].isEmpty()) {

					intTaskMatrixMap.get(pSId)
							.put(ObjectMapper.parseDMToSchedDTOId(taskList.get(i).getUgsId(),
									taskList.get(i).getProgrammingRequestId(),
									taskList.get(i).getAcquisitionRequestId(), taskList.get(i).getDtoId()),
									intMatrixData);
				}
			}
		}
	}

	/**
	 * Build the Intersection Matrix of the DTOs ordered by rank
	 *
	 * @param rankedDTOList
	 *            - the ranked list of DTOs
	 * @return the Intersection Matrix
	 */
	@SuppressWarnings("unchecked")
	public void buildARIntMatrix(Long pSId, ArrayList<SchedAR> optARList) {

		logger.debug("Build the the AR DTOs intersection Matrix...");

		/**
		 * The list of total scheduled DTOs
		 */			
		ArrayList<SchedDTO> totDTOList = new ArrayList<>();

		if (SessionScheduler.schedDTOListMap.get(pSId) != null) {

			totDTOList.addAll((ArrayList<SchedDTO>) SessionScheduler.schedDTOListMap.get(pSId).clone());

		}

		for (int i = 0; i < optARList.size(); i++) {

			totDTOList.addAll((ArrayList<SchedDTO>) optARList.get(i).getDtoList().clone());
		}

		// Build the DTO intersection matrix
		buildDTOIntMatrix(pSId, (ArrayList<SchedDTO>) totDTOList.clone());

	}

	/**
	 * Find the intersection conflict value between DTOs TODO: if according to an
	 * imposed minimum overlap
	 * @param pSId
	 * @param dTO1
	 *            - the selected DTO
	 * @param dTO2
	 *            - the DTO in comparison
	 * @return the conflict index between DTOs: 1 if DTO2 precedes DTO1, -1 if DTO1
	 *         succeeds DTO2, 0 otherwise.
	 */
	private int findDTOIntConflict(Long pSId, SchedDTO dTO1, SchedDTO dTO2) {

		// DTO conflict element
		int intConfl = 0;

		double minTime = getMinTimeBtwDTOs(pSId, dTO1, dTO2);

		// Preceding
		if ((dTO2.getStopTime().getTime() - dTO1.getStartTime().getTime()) < minTime) {

			// DTO2 precedent to DTO1
			intConfl = 1;

			// Succeeding
		} else if ((dTO2.getStartTime().getTime() - dTO1.getStopTime().getTime()) > minTime) {

			// DTO2 subsequent to DTO1
			intConfl = -1;
		}

		return intConfl;
	}

	/**
	 * Find the intersection conflict value between tasks TODO: if according to an
	 * imposed minimum overlap
	 *
	 * @param pSId
	 * @param task1
	 *            - the selected task
	 * @param task2
	 *            - the task in comparison
	 * @return the conflict index between Tasks: 1 if Task2 precedes Task1, -1 if
	 *         Task1 succeeds Task2, 0 otherwise.
	 *
	 */
	private int findTaskIntConflict(Long pSId, Task task1, Task task2) {

		/**
		 *	The task conflict element
		 */
		int intConfl = 0;

		/**
		* The minimum time
		*/	
		double minTime = getMinTimeBtwTasks(pSId, task1, task2);

		// Preceding
		if ((task2.getStopTime().getTime() - task1.getStartTime().getTime()) < minTime) {

			// task2 precedent to task1
			intConfl = 1;

			// Succeeding
		} else if ((task2.getStartTime().getTime() - task1.getStopTime().getTime()) > minTime) {

			// task2 subsequent to task1
			intConfl = -1;

		}

		return intConfl;
	}

	/**
	 * Get the minimum time between DTOs
	 *
	 * @param dTO1
	 *            - first DTO by time
	 * @param dTO2
	 *            - second DTO by time
	 * @return the minimum time between DTOs in (ms)
	 */
	private double getMinTimeBtwDTOs(Long pSId, SchedDTO dTO1, SchedDTO dTO2) {
		
		/**
		 * The minimum time
		 */
		double minTime = 0;

		if (! dTO1.getLookSide().equals(dTO2.getLookSide())) {

			minTime = RulesPerformer.brmParamsMap.get(pSId).getTimeForManeuverCmga() * 1000;
		}

		return minTime;
	}

	/**
	 * Get the minimum time between tasks
	 * // TODO: adapt with distance data for sensor mode
	 *
	 * @param pSId
	 *            - the planning session Id
	 * @param task1
	 *            - first task by time
	 * @param task2
	 *            - second task by time
	 * @return the minimum time between tasks
	 */
	private double getMinTimeBtwTasks(Long pSId, Task task1, Task task2) {
		
		/**
		 * The minimum time
		 */
		double minTime = 0;

		if (task1.getTaskType().equals(TaskType.ACQ) && task2.getTaskType().equals(TaskType.ACQ)) {

			minTime = 0; // TODO: TBD!
		}

		return minTime;
	}

}

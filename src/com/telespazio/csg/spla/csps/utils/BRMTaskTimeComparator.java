/**
*
* MODULE FILE NAME: BRMTaskTimeComparator.java
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

import java.util.Comparator;

import com.nais.spla.brm.library.main.ontology.enums.TaskType;
import com.nais.spla.brm.library.main.ontology.tasks.Task;

/**
 * The comparator between BRM tasks by start time.
 *
 * @author bunkheila
 */
public class BRMTaskTimeComparator implements Comparator<Task> {

	/**
	 * The comparator between tasks by start time.
	 *
	 * @param task1
	 *            - the first task
	 * @param task2
	 *            - the second task
	 * @return value > 0 if task1 start time is higher than task2 start time
	 */
	@Override
	public int compare(Task task1, Task task2) {

		/**
		 * The comparing value
		 */
		int compVal = Double.compare(task1.getStartTime().getTime(), task2.getStartTime().getTime());

		if (compVal == 0) {

			if (task1.getTaskType().equals(TaskType.ACQUISITION) && task2.getTaskType().equals(TaskType.STORE)) {

				// Set value
				compVal = -1;

			} else if (task1.getTaskType().equals(TaskType.STORE) && task2.getTaskType().equals(TaskType.ACQUISITION)) {

				// Set value
				compVal = 1;
			}
		}

		return compVal;
	}
}

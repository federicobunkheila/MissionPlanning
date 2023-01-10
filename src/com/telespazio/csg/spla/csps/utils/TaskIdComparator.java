/**
*
* MODULE FILE NAME: TaskTimeComparator.java
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

import it.sistematica.spla.datamodel.core.model.Task;

/**
 * The comparator between tasks by Id.
 *
 * @author bunkheila
 */
public class TaskIdComparator implements Comparator<Task> {

	/**
	 * The comparator between tasks by Id.
	 *
	 * @param task1
	 *            - the first task
	 * @param task2
	 *            - the second task
	 * @return value > 0 if task1 Id is higher than task2 Id
	 */
	@Override
	public int compare(Task task1, Task task2) {

		int compVal = Double.compare(task1.getTaskId().doubleValue(), task2.getTaskId().doubleValue());

		return compVal;
	}
}

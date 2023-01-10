/**
*
* MODULE FILE NAME: DwlTimeComparator.java
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
 * The comparator between tasks by stop time.
 *
 * @author bunkheila
 *
 */
public class TaskStopTimeComparator implements Comparator<Task> {

	/**
	 * The comparator between downloads by start time.
	 *
	 * @param task1
	 *            - the first task
	 * @param task2
	 *            - the second task
	 * @return value > 0 if task1 stop time is higher than task2 stop time
	 */
	@Override
	public int compare(Task task1, Task task2) {
		
		/**
		 * The comparing value
		 */
		int compVal = Double.compare(task1.getStopTime().getTime(), task2.getStopTime().getTime());

		return compVal;
	}
}
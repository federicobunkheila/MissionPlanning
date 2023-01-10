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

import com.nais.spla.brm.library.main.ontology.tasks.Acquisition;

/**
 * The comparator between tasks by start time.
 *
 * @author bunkheila
 */
public class AcqPriorityComparator implements Comparator<Acquisition> {

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
	public int compare(Acquisition acq1, Acquisition acq2) {

		int compVal = Double.compare(acq1.getPriority(), acq2.getPriority());

		return compVal;
	}
}

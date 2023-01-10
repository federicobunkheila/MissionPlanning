///**
// *
// * MODULE FILE NAME: DTOWorthComparator.java
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
//
//package com.telespazio.csg.spla.csps.utils;
//
//import java.util.Comparator;
//
//import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
//import com.telespazio.csg.spla.csps.performer.RulesPerformer;
//
///**
// * The comparator between DTO worth by link..
// *
// * @author bunkheila
// *
// */
//public class SchedDTOLinkComparator implements Comparator<SchedDTO> {
//
//	/**
//	 * Compare linked DTOs
//	 * 
//	 * @param x
//	 * @param y
//	 */
//	public int compare(SchedDTO dto1, SchedDTO dto2, Long pSId) {
//
//		/**
//		 * The first DTO link
//		 */
//		double link1 = 0;
//		
//		if (dto1.getLinkDtoIdList() != null && !dto1.getLinkDtoIdList().isEmpty()) {
//
//			if (RulesPerformer.getPlannedDTOIds(pSId).contains(dto1.getLinkDtoIdList().get(0))) {
//				link1 = 1;
//			}
//		}
//		
//		/**
//		 * The second DTO link
//		 */
//		double link2 = 0;
//
//		if (dto2.getLinkDtoIdList() != null && !dto2.getLinkDtoIdList().isEmpty()) {
//
//			if (RulesPerformer.getPlannedDTOIds(pSId).contains(dto2.getLinkDtoIdList().get(0))) {
//				link2 = 1;
//			}
//		}
//		
//		/**
//		 * The comparing value
//		 */
//		int compVal = Double.compare(link1, link2);
//
//		return compVal;
//	}
//
//	@Override
//	public int compare(SchedDTO o1, SchedDTO o2) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//}

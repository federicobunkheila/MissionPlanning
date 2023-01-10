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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nais.spla.brm.library.main.ontology.enums.Actuator;
import com.nais.spla.brm.library.main.ontology.enums.DownlinkStrategy;
import com.nais.spla.brm.library.main.ontology.enums.ManeuverType;
import com.nais.spla.brm.library.main.ontology.enums.Polarization;
import com.nais.spla.brm.library.main.ontology.enums.TypeOfAcquisition;
import com.nais.spla.brm.library.main.ontology.resourceData.Attitude;
import com.nais.spla.brm.library.main.ontology.resourceData.CreditCard;
import com.nais.spla.brm.library.main.ontology.resourceData.DebitCard;
import com.nais.spla.brm.library.main.ontology.resourceData.Di2sInfo;
import com.nais.spla.brm.library.main.ontology.resourceData.PAW;
import com.nais.spla.brm.library.main.ontology.resourceData.PacketStore;
import com.nais.spla.brm.library.main.ontology.resourceData.SatelliteState;
import com.nais.spla.brm.library.main.ontology.resources.EncryptionInfo;
import com.nais.spla.brm.library.main.ontology.resources.MemoryModule;
import com.nais.spla.brm.library.main.ontology.resources.PDHT;
import com.nais.spla.brm.library.main.ontology.resources.Partner;
import com.nais.spla.brm.library.main.ontology.tasks.Bite;
import com.nais.spla.brm.library.main.ontology.tasks.RampCMGA;
import com.nais.spla.brm.library.main.ontology.tasks.Storage;
import com.nais.spla.brm.library.main.ontology.tasks.StoreAUX;
import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.telespazio.csg.spla.csps.handler.EquivDTOHandler;
import com.telespazio.csg.spla.csps.handler.FilterDTOHandler;
import com.telespazio.csg.spla.csps.model.impl.SchedAR;
import com.telespazio.csg.spla.csps.model.impl.SchedDTO;
import com.telespazio.csg.spla.csps.performer.PersistPerformer;
import com.telespazio.csg.spla.csps.performer.RulesPerformer;
import com.telespazio.csg.spla.csps.processor.PRListProcessor;
import com.telespazio.csg.spla.csps.processor.SessionActivator;
import com.telespazio.splaif.protobuf.Common;

import it.sistematica.spla.datamodel.core.enums.AcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.enums.DTOSensorMode;
import it.sistematica.spla.datamodel.core.enums.DtoStatus;
import it.sistematica.spla.datamodel.core.enums.LookSide;
import it.sistematica.spla.datamodel.core.enums.PAWType;
import it.sistematica.spla.datamodel.core.enums.PRKind;
import it.sistematica.spla.datamodel.core.enums.PRMode;
import it.sistematica.spla.datamodel.core.enums.PRType;
import it.sistematica.spla.datamodel.core.enums.PlanningSessionStatus;
import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.enums.TaskMarkType;
import it.sistematica.spla.datamodel.core.enums.TaskStatus;
import it.sistematica.spla.datamodel.core.enums.TaskType;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.model.AcquisitionRequest;
import it.sistematica.spla.datamodel.core.model.DTO;
import it.sistematica.spla.datamodel.core.model.EquivalentDTO;
import it.sistematica.spla.datamodel.core.model.PlanAcquisitionRequestStatus;
import it.sistematica.spla.datamodel.core.model.PlanDtoStatus;
import it.sistematica.spla.datamodel.core.model.PlanProgrammingRequestStatus;
import it.sistematica.spla.datamodel.core.model.Task;
import it.sistematica.spla.datamodel.core.model.UserInfo;
import it.sistematica.spla.datamodel.core.model.bean.PitchIntervalDetail;
import it.sistematica.spla.datamodel.core.model.bean.PitchIntervals;
import it.sistematica.spla.datamodel.core.model.bean.UgsOwner;
import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.CMGA;
import it.sistematica.spla.datamodel.core.model.resource.HPExclusion;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.Pdht;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogCMGA;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogHPLimitation;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogPdht;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogSatellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.HPLimitationEntry;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Eclipse;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ElevationMask;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.PlatformActivityWindow;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceStatus;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceValue;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Transaction;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;
import it.sistematica.spla.datamodel.core.model.task.Acquisition;
import it.sistematica.spla.datamodel.core.model.task.BITE;
import it.sistematica.spla.datamodel.core.model.task.CMGAxis;
import it.sistematica.spla.datamodel.core.model.task.DI2SInfo;
import it.sistematica.spla.datamodel.core.model.task.Download;
import it.sistematica.spla.datamodel.core.model.task.Maneuver;
import it.sistematica.spla.datamodel.core.model.task.PassThrough;
import it.sistematica.spla.datamodel.core.model.task.Ramp;
import it.sistematica.spla.datamodel.core.model.task.Store;
import it.sistematica.spla.datamodel.core.model.task.StoreAux;

/**
 * The Object Mapper class
 * Hp: every task size is in sector
 * 
 */
public class ObjectMapper {

	/**
	 * The proper logger
	 */
	private static Logger logger = LoggerFactory.getLogger(ObjectMapper.class);

	/**
	 * Parse DataModel AR into scheduling format
	 *
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aR
	 * @param acqStationIdList
	 * @param prevPlan
	 * @return
	 * @throws Exception
	 */
	public static SchedAR parseDMToSchedAR(Long pSId, String ugsId, String pRId, AcquisitionRequest aR, 
			List<String> acqStationIdList, Double pitchCost, boolean prevPlan) throws Exception {
		logger.trace("Parse AR: " + aR.getAcquisititionRequestId() + " for PR " + pRId + " and UGS Id " + ugsId);

		/**
		 * The output AR
		 */
		SchedAR schedAR = new SchedAR();

		schedAR.setARId(ugsId.replace(Configuration.splitChar, "-") + Configuration.splitChar
				+ pRId.replace(Configuration.splitChar, "-") + Configuration.splitChar
				+ aR.getAcquisititionRequestId().replace(Configuration.splitChar, "-") + Configuration.splitChar);
		schedAR.setPRType(PRListProcessor.pRSchedIdMap.get(pSId).get(
				ObjectMapper.parseDMToSchedPRId(ugsId, pRId)).getType());
		schedAR.setPRMode(PRListProcessor.pRSchedIdMap.get(pSId).get(
				ObjectMapper.parseDMToSchedPRId(ugsId, pRId)).getMode());
		schedAR.setEquivalentDTO(aR.getEquivalentDTO());

		if (pitchCost != null) {

			schedAR.setPitchExtraBic(Double.valueOf(pitchCost));

		} else {

			schedAR.setPitchExtraBic(0);
		}

		List<PlanAcquisitionRequestStatus> aRStatusList = new ArrayList<>();

		List<PlanProgrammingRequestStatus> pRStatusList = SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList();

		for (int i = 0; i < pRStatusList.size(); i++) {

			if (pRStatusList.get(i).getUgsId().equals(ugsId)
					&& pRStatusList.get(i).getProgrammingRequestId().equals(pRId)) {

				aRStatusList = pRStatusList.get(i).getAcquisitionRequestStatusList();

				break;
			}
		}

		if (!aRStatusList.isEmpty()) {

			for (int j = 0; j < aRStatusList.size(); j++) {

				if (aRStatusList.get(j).getAcquisitionRequestId().equals(aR.getAcquisititionRequestId())) {

					schedAR.setARStatus(aRStatusList.get(j).getStatus());

					schedAR.setSchedDTOList(parseDMToSchedDTOList(pSId, schedAR.getARId(), 
							aR.getDtoList(), acqStationIdList, prevPlan));

					break;
				}
			}
		} 

		if (schedAR.getDtoList() == null ||schedAR.getDtoList().isEmpty()) {

			schedAR.setARStatus(AcquisitionRequestStatus.New);

			schedAR.setSchedDTOList(parseDMToSchedDTOList(pSId, schedAR.getARId(), aR.getDtoList(),
					acqStationIdList, prevPlan));
		}

		if (SessionActivator.initARIdEquivDTOMap.get(pSId).containsKey(schedAR.getARId())) {

			schedAR.setEquivalentDTO(aR.getEquivalentDTO());
		}

		return schedAR;

	}

	/**
	 * Parse DataModel AR List into scheduling format
	 * 
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRList
	 * @param acqStationIdList
	 * @param prevProc
	 * @param pitchCost
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<SchedAR> parseDMToSchedARList(Long pSId, String ugsId, String pRId,
			List<AcquisitionRequest> aRList, List<String> acqStationIdList, double pitchCost,
			boolean prevPlan) throws Exception {

		/**
		 * The output AR List
		 */
		ArrayList<SchedAR> schedARList = new ArrayList<>();

		for (int i = 0; i < aRList.size(); i++) {

			schedARList.add(parseDMToSchedAR(pSId, ugsId, pRId, aRList.get(i),
					acqStationIdList, pitchCost, prevPlan));
		}

		return schedARList;

	}

	/**
	 * Parse DataModel to scheduling DTOList
	 *
	 * // TODO reset as List<UserInfo> for single AR DTOs each time
	 * 
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param dtoList
	 * @param acqStationIdList
	 * @param prevPlan
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<SchedDTO> parseDMToSchedDTOList(Long pSId, String ugsId, String pRId, String aRId,
			List<DTO> dtoList, List<String> acqStationIdList, boolean prevPlan) throws Exception {

		/**
		 * The output DTO List
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<>();

		for (int i = 0; i < dtoList.size(); i++) {

			schedDTOList.add(parseDMToSchedDTO(pSId, ugsId, pRId, aRId, dtoList.get(i),
					acqStationIdList, prevPlan));
		}

		return schedDTOList;
	}

	/**
	 * Parse DataModel DTOList into scheduling format
	 *
	 * // TODO reset as List<UserInfo> for single AR DTOs each time
	 *
	 * @param pSId
	 * @param schedARId
	 * @param dtoStatusList
	 * @param dtoList
	 * @param pRType
	 * @param pRMode
	 * @param userInfoList
	 * @param isNEO
	 * @param acqStationIdList
	 * @param PrevPlanned
	 * @return the list of scheduled DTOs
	 */
	public static ArrayList<SchedDTO> parseDMToSchedDTOList(Long pSId, String schedARId, List<DTO> dtoList,
			List<String> prefStationIdList, boolean prevPlan) throws Exception {

		/**
		 * The output DTO List
		 */
		ArrayList<SchedDTO> schedDTOList = new ArrayList<>();

		for (int i = 0; i < dtoList.size(); i++) {

			schedDTOList.add(parseDMToSchedDTO(pSId, getUgsId(schedARId), getPRId(schedARId), getARId(schedARId),
					dtoList.get(i), prefStationIdList, prevPlan));
		}
		return schedDTOList;
	}

	/**
	 * Parse BRM to Data Model CMGA
	 *
	 * @param brmCMGs
	 * @return
	 */
	public static CMGA parseBRMToDMCMGA(com.nais.spla.brm.library.main.ontology.resources.CMGA[] brmCMGs) {

		logger.trace("Reparse CMGA data.");

		CMGA cmga = new CMGA(
				new CatalogCMGA(brmCMGs[0].getCmgaId(), Boolean.valueOf(brmCMGs[0].isOperative()),
						Double.valueOf(brmCMGs[0].gettAccUp()), Double.valueOf(brmCMGs[0].gettAccDown()),
						Double.valueOf(brmCMGs[0].getwAcc()), Double.valueOf(brmCMGs[0].getwPlat()), 
						Double.valueOf(brmCMGs[0].getwRest())),
				new CatalogCMGA(brmCMGs[1].getCmgaId(), Boolean.valueOf(brmCMGs[1].isOperative()),
						Double.valueOf(brmCMGs[1].gettAccUp()), Double.valueOf(brmCMGs[0].gettAccDown()),
						Double.valueOf(brmCMGs[1].getwAcc()), Double.valueOf(brmCMGs[1].getwPlat()), 
						Double.valueOf(brmCMGs[1].getwRest())),
				new CatalogCMGA(brmCMGs[2].getCmgaId(), Boolean.valueOf(brmCMGs[2].isOperative()),
						Double.valueOf(brmCMGs[2].gettAccUp()), Double.valueOf(brmCMGs[0].gettAccDown()),
						Double.valueOf(brmCMGs[2].getwAcc()), Double.valueOf(brmCMGs[2].getwPlat()), 
						Double.valueOf(brmCMGs[2].getwRest())));

		return cmga;
	}

	/**
	 * Parse Data Model to BRM HP limitations
	 * @param hpLimCatalog
	 * @param satId
	 * @return
	 */
	public static HashMap<Integer, Double> parseDMToBRMHPLimit(CatalogHPLimitation hpLimCatalog, String satId) {

		logger.trace("Parse HP Limitation.");

		HashMap<Integer, Double> hpLimitMap = new HashMap<Integer, Double>();

		for (HPLimitationEntry hpLimitEntry : hpLimCatalog.getEntries()) {

			hpLimitMap.put(hpLimitEntry.getOrbit(), hpLimitEntry.getBic());
		}

		return hpLimitMap;
	}

	/**
	 * Parse DataModel to BRM HP Exclusion orbits
	 * 
	 * @param hpExcl
	 * @param satId
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.resourceData.HPExclusion parseDMToBRMHPExcl(
			HPExclusion hpExcl, String satId) {

		com.nais.spla.brm.library.main.ontology.resourceData.HPExclusion brmHPExcl = new com.nais.spla.brm.library.main.ontology.resourceData.HPExclusion(
				hpExcl.getHpExclusionListId(), hpExcl.getStartTime(), hpExcl.getStartLookSide(),
				hpExcl.getStopTime(), hpExcl.getStopLookSide(), satId, hpExcl.getIsEnabled(), hpExcl.getIsPeriodic());

		return brmHPExcl;

	}

	/**
	 * Parse Data Model to BRM CMGA
	 * // TODO: add cmga.getCatalogCMGA3().gettAccDown() 
	 *
	 * @param cmga
	 * @param satId
	 * @return
	 */
	public static ArrayList<com.nais.spla.brm.library.main.ontology.resources.CMGA> parseDMToBRMCMGA(CMGA cmga,
			String satId) {

		logger.trace("Parse CMGA.");

		/**
		 * The list of CMGAs
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.resources.CMGA> cmgList = new ArrayList<>();
		com.nais.spla.brm.library.main.ontology.resources.CMGA cmga1 = new com.nais.spla.brm.library.main.ontology.resources.CMGA(
				cmga.getCatalogCMGA1().getCmgaId(), cmga.getCatalogCMGA1().getwAcc(), 
				cmga.getCatalogCMGA1().gettAccUp(), cmga.getCatalogCMGA1().gettAccDown(),
				cmga.getCatalogCMGA1().getwPlat(), cmga.getCatalogCMGA1().getwRest(),
				cmga.getCatalogCMGA1().isEnabledFlag().booleanValue(), satId);
		cmgList.add(cmga1);
		com.nais.spla.brm.library.main.ontology.resources.CMGA cmga2 = new com.nais.spla.brm.library.main.ontology.resources.CMGA(
				cmga.getCatalogCMGA2().getCmgaId(), cmga.getCatalogCMGA2().getwAcc(), 
				cmga.getCatalogCMGA1().gettAccUp(), cmga.getCatalogCMGA1().gettAccDown(),
				cmga.getCatalogCMGA2().getwPlat(), cmga.getCatalogCMGA2().getwRest(),
				cmga.getCatalogCMGA2().isEnabledFlag().booleanValue(), satId);
		cmgList.add(cmga2);
		com.nais.spla.brm.library.main.ontology.resources.CMGA cmga3 = new com.nais.spla.brm.library.main.ontology.resources.CMGA(
				cmga.getCatalogCMGA3().getCmgaId(), cmga.getCatalogCMGA3().getwAcc(), 
				cmga.getCatalogCMGA1().gettAccUp(), cmga.getCatalogCMGA1().gettAccDown(),
				cmga.getCatalogCMGA3().getwPlat(), cmga.getCatalogCMGA3().getwRest(),
				cmga.getCatalogCMGA3().isEnabledFlag().booleanValue(), satId);
		cmgList.add(cmga3);

		return cmgList;
	}

	/**
	 * Parse DataModel DTO into scheduling format
	 * 
	 * @param pSId
	 * @param schedDTOId
	 * @param dto
	 * @param prevProc
	 * @return
	 */
	public static SchedDTO parseDMToSchedDTO(Long pSId, String schedDTOId, DTO dto, boolean prevPlan) {

		/**
		 * The scheduled DTO
		 */
		SchedDTO schedDTO = parseDMToSchedDTO(pSId, ObjectMapper.getUgsId(schedDTOId), ObjectMapper.getPRId(schedDTOId), 
				ObjectMapper.getARId(schedDTOId), dto, PRListProcessor.pRSchedIdMap.get(pSId)
				.get(ObjectMapper.getSchedPRId(schedDTOId)).getUserList().get(0).getAcquisitionStationIdList(), 
				prevPlan);

		return schedDTO;
	}

	/**
	 * Parse DataModel DTO into scheduling format
	 *
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param dto
	 * @param prefStationIdList
	 * @param prevPlanned
	 * @return
	 */
	public static SchedDTO parseDMToSchedDTO(Long pSId, String ugsId, String pRId, String aRId, DTO dto, 
			List<String> prefStationIdList, boolean prevPlan) {

		/**
		 * The output DTO
		 */
		SchedDTO schedDTO = new SchedDTO();

		try {

			logger.trace("Parse DTO: " + dto.getDtoId() 
			+ " for AR " + aRId + ", PR " + pRId + " and UGS " + ugsId);

			if (dto.getBeamID() != null) {
				schedDTO.setBeamId(dto.getBeamID());
			} else {
				logger.trace("Null Beam Id set.");
			}

			if ((pRId.contains(ugsId)) && (aRId.contains(ugsId)) && (aRId.contains(pRId))) {
				schedDTO.setPRId(pRId);
				schedDTO.setARId(aRId);
			} else {
				schedDTO.setPRId(ugsId + Configuration.splitChar + pRId + Configuration.splitChar);
				schedDTO.setARId(schedDTO.getPRId() + aRId + Configuration.splitChar);
			}

			// logger.trace("Parse Ids.");
			String schedDTOId = schedDTO.getARId() + dto.getDtoId().replace(Configuration.splitChar, "-")
					+ Configuration.splitChar;			
			schedDTO.setDTOId(schedDTOId);
			schedDTO.setPrevPlanned(prevPlan);

			if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedDTO.getARId())) {

				schedDTO.setRank(PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId()).getRank());	

				if (PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId()).getEquivalentDTO() != null) {

					schedDTO.setEquivDTOId(PRListProcessor.aRSchedIdMap.get(pSId).get(schedDTO.getARId())
							.getEquivalentDTO().getEquivalentDtoId());
				}
			}

			if (PRListProcessor.schedARIdRankMap.get(pSId).containsKey(schedDTO.getARId())) {

				schedDTO.setPriority(PRListProcessor.schedARIdRankMap.get(pSId).get(schedDTO.getARId()));

			} else {			

				logger.debug("No rank found for AR: " + schedDTO.getARId());
				schedDTO.setPriority(schedDTO.getRank() + SessionActivator.initSchedDTOListMap.get(pSId).size());
			}

			if (dto.getLinkedDtoList() != null && !dto.getLinkedDtoList().isEmpty()) // TODO: expand for multiple links
			{
				logger.trace("Linked DTO found. First link is set.");

				schedDTO.setDTOLinkType(dto.getLinkedDtoList().get(0).getDtoLinkType());
				schedDTO.setLinkDtoIdList(new ArrayList<String>());
				schedDTO.getLinkDtoIdList().add(ObjectMapper.parseDMToSchedDTOId(
						getUgsId(schedDTO.getDTOId()), getPRId(schedDTO.getDTOId()), 
						dto.getLinkedDtoList().get(0).getLinkedAcquisitionRequestId(), 
						dto.getLinkedDtoList().get(0).getLinkedDtoId()));
			}
			if (dto.getTargetDistance() != null) {
				schedDTO.setTargetDistance(dto.getTargetDistance());
			} else {
				schedDTO.setTargetDistance(0.0);
				logger.trace("Default Target Distance set.");
			}
			if (dto.getLookAngle() != null) {			
				schedDTO.setLookAngle(dto.getLookAngle());
			} else {
				schedDTO.setLookAngle(0.0);
				logger.trace("Default Look Angle set.");
			}
			if (dto.getLookSide() != null) {
				schedDTO.setLookSide(dto.getLookSide());
			} else {
				schedDTO.setLookSide("Right");	
				logger.trace("Default Look Side set.");
			}
			if (dto.getOrbitDirection() != null) {
				schedDTO.setOrbDir(dto.getOrbitDirection());
			} else {
				logger.trace("Null Orbit Direction set.");
			}

			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedDTO.getPRId())) {

				schedDTO.setUserInfoList(PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getUserList());

				// Remove filtered subscribers
				// TODO: check if DTO is previously planned from BRM
				for (int i = 1; i < schedDTO.getUserInfoList().size(); i++) {

					if (FilterDTOHandler.filtRejDTOIdListMap.get(pSId).contains(
							parseDMToSchedDTOId(schedDTO.getUserInfoList().get(i).getUgsId(), getPRId(schedDTO.getDTOId()),
									getARId(schedDTO.getDTOId()), getDTOId(schedDTO.getDTOId())))) {

						schedDTO.getUserInfoList().remove(i);

						i --;
					}
				}
				schedDTO.setPRType(PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getType());
				schedDTO.setPRMode(PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getMode());

			} else {

				logger.info("No PR scheduling info are found for PR: " + schedDTO.getPRId());
			}
			schedDTO.setSatelliteId(dto.getSatelliteId());			
			schedDTO.setSensorMode(dto.getSensorMode());

			// Set preferred acquisition stations
			schedDTO.setPrefStationIdList(prefStationIdList);

			// Set backup acquisition stations
			ArrayList<String> backStationIdList = new ArrayList<String>();

			if (SessionActivator.ugsBackStationIdListMap.get(pSId).containsKey(ugsId)) {
																		 

				backStationIdList.addAll(SessionActivator.ugsBackStationIdListMap.get(pSId).get(ugsId));
																 
			}
			schedDTO.setBackStationIdList(backStationIdList);

			schedDTO.setTimePerf(false);

			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey (schedDTO.getPRId())) {

				if (PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getKind() != null 
						&& PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getKind()
						.equals(PRKind.TIMEPERFORMANCE)) {
					schedDTO.setTimePerf(true);
				}

				if (schedDTO.getSensorMode().equals(DTOSensorMode.BITE)) {

					/**
					 * The BITE task
					 */
					BITE bite = new BITE();
					if (PRListProcessor.pRSchedIdMap.get(pSId)
							.get(schedDTO.getPRId()).getBiteModuleId() != null
							&& PRListProcessor.pRSchedIdMap.get(pSId)
							.get(schedDTO.getPRId()).getBiteFillerWord() != null
							&& PRListProcessor.pRSchedIdMap.get(pSId)
							.get(schedDTO.getPRId()).getBitePacketStoreId() != null) {

						bite.setModuleId(PRListProcessor.pRSchedIdMap.get(pSId)
								.get(schedDTO.getPRId()).getBiteModuleId());
						bite.setFillerWord(PRListProcessor.pRSchedIdMap.get(pSId)
								.get(schedDTO.getPRId()).getBiteFillerWord());
						bite.setModuleSelectionFlag(PRListProcessor.pRSchedIdMap.get(pSId)
								.get(schedDTO.getPRId()).getBiteModuleSelectionFlag());					

						try {

							bite.setPacketStoreId(Long.parseLong(PRListProcessor.pRSchedIdMap.get(pSId)
									.get(schedDTO.getPRId()).getBitePacketStoreId()));

						} catch (Exception ex) {
							bite.setPacketStoreId(0L);
							logger.warn("A default PacketStoreId is built for DTO: " + schedDTO.getDTOId());
						}	
					} else {
						logger.warn("A default BITE Task is built for DTO: " + schedDTO.getDTOId());
						bite.setModuleId("0");
						bite.setFillerWord("0");
						bite.setModuleSelectionFlag(0);
						bite.setPacketStoreId(0L);
					}
					schedDTO.setBite(bite);
				}
				if (dto.getInterleavedChannel() != null) {				
					logger.trace("Interleaved Channel found.");
					schedDTO.setInterleaved(dto.getInterleavedChannel());
				} 

				if (dto.getPolarization() != null) {
					schedDTO.setPolarization(dto.getPolarization());
				} else {
					schedDTO.setPolarization("HH");
					logger.trace("A default polarization is set.");
				}

				
				if ((dto.isDi2SFlag() != null && dto.isDi2SFlag())
						|| (PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getDi2sAvailabilityFlag())) {
					
					logger.trace("Set DI2S flag for DTO: " + schedDTO.getDTOId());
					schedDTO.setDi2sAvailable(true);
					
					// Commented on 11/4/2022 for testing
					// Readded on 29/04/2022 for DI2S-able requests management
					// Completed on 23/06/2022 for DI2S-able requests management
					if (RequestChecker.isDefence(ObjectMapper.getUgsId(schedDTO.getDTOId()))
							&& PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getDi2sAvailabilityFlag()) {
						
						PRListProcessor.pRIntBoolMap.get(pSId).put(
								ObjectMapper.getSchedPRId(schedDTO.getDTOId()), true);							
					}
					
				} else {
					
					schedDTO.setDi2sAvailable(false);				
					logger.trace("A default DI2S flag is set.");
				}

				if ((dto.getSizeH() == null)) {
					dto.setSizeH(0.0);
				}
				if ((dto.getSizeV() == null)) {
					dto.setSizeV(0.0);
				}
				schedDTO.setSizeH(dto.getSizeH()); // in Mbit
				schedDTO.setSizeV(dto.getSizeV()); // in Mbit

				if (! schedDTO.getSensorMode().equals(DTOSensorMode.BITE)
						&& (schedDTO.getSizeH() + schedDTO.getSizeV()) == 0) {
					logger.warn("No sizes set for DTO: " + schedDTO.getDTOId());
				}

				schedDTO.setStartTime(dto.getStartTime());
				schedDTO.setStopTime(dto.getStopTime());

				if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedDTO.getPRId())) {
					schedDTO.setNEOAvailable(RequestChecker.isNEO(
							PRListProcessor.pRSchedIdMap.get(pSId).get(schedDTO.getPRId()).getVisibility()));
				} else {

					logger.warn("No visibility info set for DTO " + schedDTO.getDTOId());
				}
				if (dto.getOrbitNumber() != null) {
					schedDTO.setRevNum(dto.getOrbitNumber().intValue());
				}
				if (dto.getPassthroughFlag() != null) {
					schedDTO.setPTAvailable(dto.getPassthroughFlag());
				} else {
					schedDTO.setPTAvailable(false);
					logger.debug("A default Passthrough flag is set.");
				}
				if (!schedDTO.getUserInfoList().isEmpty()) {
					schedDTO.setBIC(dto.getBic());
				} else {
					logger.warn("The PR User Info list is empty.");
					schedDTO.setBIC(0);
				}

				// Set BIC decrement policy
				schedDTO.setDecrBIC(BICCalculator.isDecrBIC(pSId, schedDTO));

				initSchedDTOStatus(schedDTO, pSId);
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return schedDTO;
	}

	/**
	 * Set scheduling DTO status
	 * @param schedDTO
	 * @param pSId
	 */
	private static void initSchedDTOStatus(SchedDTO schedDTO, Long pSId) {

		logger.trace("Initialize Scheduling DTO statuses.");

		// TODO: check for DTOs overlapping MH bounds
		if ((schedDTO.getStopTime().getTime() < SessionActivator.planSessionMap.get(pSId)
				.getMissionHorizonStartTime().getTime())
				|| (schedDTO.getStartTime().getTime() > SessionActivator.planSessionMap.get(pSId)
						.getMissionHorizonStopTime().getTime())) {

			// Set DTO Status			
			schedDTO.setStatus(DtoStatus.Rejected);

			logger.debug("DTO " + schedDTO.getDTOId() +"outside the relevant Mission Horizon!");
		} else {

			// Set DTO Status
			schedDTO.setStatus(DtoStatus.Unused);
		}

		/**
		 * The list of DTO statuses
		 */
		List<PlanDtoStatus> dtoStatusList = new ArrayList<>();

		for (PlanProgrammingRequestStatus pRStatus : SessionActivator.planSessionMap.get(pSId)
				.getProgrammingRequestStatusList()) {

			/**
			 * The list of AR statuses
			 */
			List<PlanAcquisitionRequestStatus> aRStatusList = (pRStatus.getAcquisitionRequestStatusList());

			for (PlanAcquisitionRequestStatus aRStatus : aRStatusList) 
			{
				if ((schedDTO.getPRId().contains(pRStatus.getUgsId()))
						&& (schedDTO.getPRId().contains(pRStatus.getProgrammingRequestId())) 
						&& schedDTO.getARId().contains(aRStatus.getAcquisitionRequestId())) 
				{
					dtoStatusList = aRStatus.getDtoStatusList();

					for (PlanDtoStatus dtoStatus : dtoStatusList) {

						if (schedDTO.getDTOId().contains(dtoStatus.getDtoId())) {

							// Set DTO Status
							schedDTO.setStatus(dtoStatus.getStatus());

							break;
						}
					}

					break;
				}
			}
		}
	}

	/**
	 * Parse DTO from scheduling to BRM format
	 *
	 * @param schedDTO
	 * @param pRMode
	 * @param rank
	 * @return
	 * @throws InputException
	 */
	public static com.nais.spla.brm.library.main.ontology.resourceData.DTO parseSchedToBRMDTO(Long pSId, SchedDTO schedDTO)
			throws Exception {

		logger.trace("Parse internal DTO: " + schedDTO.getDTOId());

		/**
		 * The output DTO
		 */
		com.nais.spla.brm.library.main.ontology.resourceData.DTO dto = new com.nais.spla.brm.library.main.ontology.resourceData.DTO(
				schedDTO.getDTOId(), schedDTO.getStartTime(), schedDTO.getStopTime(), schedDTO.getLookSide(),
				schedDTO.getSatelliteId(), parseDMToBRMSensorMode(schedDTO.getSensorMode()),
				parseDMToBRMPolar(schedDTO.getPolarization()));

		dto.setArID(schedDTO.getARId());
		dto.setUserInfo(parseDMToBRMUserInfo(schedDTO.getUserInfoList()));
		dto.setImageBIC(schedDTO.getBIC());
		dto.setPrType(parseDMToBRMPRType(schedDTO.getPRType()));
		dto.setRevolutionNumber((int) (schedDTO.getRevNum() % Configuration.orbRevNumber));
		dto.setNeoAvailable(schedDTO.isNEO());
		dto.setPtAvailable(schedDTO.isPTAvailable());
		// Set preferred visibilities
		dto.setPreferredVis(schedDTO.getPrefStationIdList());	
		// Set backup visibilities
		dto.setBackupVis(schedDTO.getBackStationIdList());
		dto.setPreviousSession(schedDTO.isPrevPlanned());
		dto.setTimePerformance(schedDTO.isTimePerf());
		dto.setPrMode(parseDMToBRMPRMode(schedDTO.getPRMode()));
		dto.setReferredEquivalentDto(schedDTO.getEquivDTOId());
		// Set sizes
		if (schedDTO.getSizeH() != null && !RequestChecker.isSinglePolarV(schedDTO.getPolarization())) {
			dto.setSizeH((int)(Math.ceil(schedDTO.getSizeH() / (4.0 * 8.0) / Configuration.mega2MebiFac))); // sectors																		// (4 MB)
		}
		if (schedDTO.getSizeV() != null && !RequestChecker.isSinglePolarH(schedDTO.getPolarization())) {
			dto.setSizeV((int)(Math.ceil(schedDTO.getSizeV() / (4.0 * 8.0) / Configuration.mega2MebiFac))); // sectors
		}		

		// Set info
		dto.setPtAvailable(schedDTO.isPTAvailable());								
		dto.setPriority(schedDTO.getPriority());
		dto.setDecrementBic(schedDTO.isDecrBIC());
		dto.setReplacedRequestListId(schedDTO.getReplDTOIdList());
		if (schedDTO.getLinkDtoIdList() != null) {
			dto.setStereopair(true);
			dto.setLinkedDtoId(schedDTO.getLinkDtoIdList());
		} else {
			dto.setStereopair(false);
		}
		if (schedDTO.isDi2sAvailable()) {
			dto.setInterleavedChannel(schedDTO.getInterleaved());
			// TODO: added on 28/03/2022
			logger.debug("Set DI2S availability for DTO: " + schedDTO.getDTOId());
			dto.setDi2sAvailable(schedDTO.isDi2sAvailable());
		}
		if (schedDTO.getSensorMode().equals(DTOSensorMode.BITE)) {
			dto.setAssociatedBite(
					new Bite(schedDTO.getBite().getModuleSelectionFlag(), schedDTO.getBite().getModuleId(),
							schedDTO.getBite().getFillerWord(), schedDTO.getBite().getPacketStoreId().toString()));
		}

		// Added on 20/4/2020 for the Enchryption management
		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedARId(schedDTO.getARId()))) {

			// The Encryption Info
			EncryptionInfo encryptionInfo = new EncryptionInfo(
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedDTO.getARId())).getEncryptionStrategy(),
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedDTO.getARId())).getEncryptionEKSelection(), 
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedDTO.getARId())).getEncryptionIVSelection());

			dto.setEncrypInfo(encryptionInfo);
		}
		
		// Added on 22/1/2021 for S-TUP expansion
		dto.setTup(TUPCalculator.isUgsTUP(pSId, dto.getUserInfo().get(0).getUgsId()));
		
		return dto;
	}

	/**
	 * Parse list of DTOs from scheduling to BRM format
	 *
	 * @param pSId
	 * @param schedDTOList
	 * @return
	 * @throws InputException
	 */
	public static ArrayList<com.nais.spla.brm.library.main.ontology.resourceData.DTO> parseSchedToBRMDTOList(
			Long pSId, ArrayList<SchedDTO> schedDTOList) throws Exception {

		/**
		 * The output DTOList
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.resourceData.DTO> dtoList = new ArrayList<>();

		for (int i = 0; i < schedDTOList.size(); i++) {

			dtoList.add(parseSchedToBRMDTO(pSId, schedDTOList.get(i)));
		}

		return dtoList;
	}

	/**
	 * Parse the DTO Id from Data Model to univocal scheduling format
	 *
	 * @param ugsId
	 *            - the UGS Id
	 * @param pRId
	 *            - the PR Id
	 * @param aRId
	 *            - the AR Id
	 * @param dtoId
	 *            - the DTO Id
	 * @return
	 */
	public static String parseDMToSchedDTOId(String ugsId, String pRId, String aRId, String dtoId) {

		String schedDTOId = ugsId + Configuration.splitChar + pRId + Configuration.splitChar + aRId
				+ Configuration.splitChar + dtoId + Configuration.splitChar;

		return schedDTOId;
	}

	/**
	 * Parse the AR Id from Data Model to univocal scheduling format
	 *
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 *
	 * @return
	 */
	public static String parseDMToSchedARId(String ugsId, String pRId, String aRId) {

		/**
		 * The scheduled AR Id
		 */
		String schedARId = ugsId + Configuration.splitChar + pRId + Configuration.splitChar + aRId
				+ Configuration.splitChar;

		return schedARId;
	}

	/**
	 * Parse the PR Id from Data Model to univocal scheduling format
	 *
	 * @param ugsId
	 * @param pRId
	 * @return
	 */
	public static String parseDMToSchedPRId(String ugsId, String pRId) {

		/**
		 * The scheduled PR Id
		 */
		String schedPRId = ugsId + Configuration.splitChar + pRId + Configuration.splitChar;

		return schedPRId;
	}

	/**
	 * Return the associated scheduling PR Id of the scheduling DTO
	 *
	 * @param schedDTOId
	 */
	public static String getSchedPRId(String schedId) {

		String[] ids = schedId.split(Configuration.splitChar);
		String schedPRId = ids[0] + Configuration.splitChar + ids[1] + Configuration.splitChar;

		return schedPRId;
	}

	/**
	 * Return the associated scheduling AR Id of the scheduling DTO Id
	 *
	 * @param schedId
	 */
	public static String getSchedARId(String schedId) {

		/**
		 * The scheduling AR Id
		 */
		String schedARId = null;		

		String[] ids = schedId.split(Configuration.splitChar);
		schedARId = ids[0] + Configuration.splitChar + ids[1] + Configuration.splitChar
				+ ids[2] + Configuration.splitChar;

		return schedARId;
	}

	/**
	 * Return the associated UGS Id of the scheduling PR/AR/DTO Id
	 *
	 * @param schedId
	 */
	public static String getUgsId(String schedId) {

		String[] ids = schedId.split(Configuration.splitChar);
		String ugsId = ids[0];

		return ugsId;
	}

	/**
	 * Return the associated PR Id of the scheduling AR/DTO Id
	 *
	 * @param schedId
	 */
	public static String getPRId(String schedId) {

		String[] ids = schedId.split(Configuration.splitChar);
		String pRId = ids[1];

		return pRId;
	}

	/**
	 * Return the associated AR Id of the scheduling AR/DTO Id
	 *
	 * @param schedId
	 */
	public static String getARId(String schedId) {

		String[] ids = schedId.split(Configuration.splitChar);
		String aRId = ids[2];

		return aRId;
	}

	/**
	 * Return the associated DTO Id of the scheduling DTO Id
	 *
	 * @param schedARId
	 */
	public static String getDTOId(String schedId) {

		String[] ids = schedId.split(Configuration.splitChar);
		String aRId = ids[3];

		return aRId;
	}

	/**
	 * Parse Acquisition from BRM to scheduling DTO
	 *
	 * // TODO: add DTO info where necessary
	 *
	 * @param pSId
	 * @param acq
	 * @return
	 * @throws Exception
	 */
	public static SchedDTO parseBRMAcqToSchedDTO(Long pSId, com.nais.spla.brm.library.main.ontology.tasks.Acquisition acq)
			throws Exception {

		/**
		 * The output scheduling DTO
		 */
		SchedDTO schedDTO = new SchedDTO();

		logger.debug("");

		String[] ids = acq.getId().split(Configuration.splitChar);
		schedDTO.setPRId(ids[0].concat(Configuration.splitChar).concat(ids[1].concat(Configuration.splitChar)));
		schedDTO.setARId(ids[0].concat(Configuration.splitChar)
				.concat(ids[1].concat(Configuration.splitChar).concat(ids[2]).concat(Configuration.splitChar)));
		schedDTO.setDTOId(ids[0].concat(Configuration.splitChar).concat(ids[1].concat(Configuration.splitChar).concat(ids[2])
				.concat(Configuration.splitChar).concat(ids[3]).concat(Configuration.splitChar)));
		schedDTO.setDTOId(acq.getId());
		schedDTO.setStartTime(acq.getStartTime());
		schedDTO.setStopTime(acq.getEndTime());
		schedDTO.setBIC(acq.getImageBIC());
		schedDTO.setNEOAvailable(acq.isNeo());
		schedDTO.setSensorMode(parseBRMToDMSensorMode(acq.getSensorMode()));
		schedDTO.setSatelliteId(acq.getSatelliteId());
		schedDTO.setLookSide(acq.getLookSide());
		schedDTO.setStatus(DtoStatus.Scheduled);
		schedDTO.setUserInfoList(getPRUserInfoList(pSId, schedDTO.getPRId()));
		schedDTO.setNEOAvailable(acq.isNeo());
		schedDTO.setRevNum(acq.getRevolutionNumber());
		
		// Changed on 17/03/2022 from SPLA-4.5.4 -------
		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey(schedDTO.getDTOId()) 
				&& PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTO.getDTOId()).getPolarization() != null) {

			schedDTO.setPolarization(PRListProcessor.dtoSchedIdMap.get(pSId).get(schedDTO.getDTOId()).getPolarization());
		
		} else {
			
			schedDTO.setPolarization(parseBRMToDMPolar(acq.getPolarization()));
		}

		
		schedDTO.setPRType(parseBRMToDMPRType(acq.getPrType(), acq.getUserInfo().get(0).getUgsId()));
		schedDTO.setSizeH(acq.getSizeH() * 4.0 * 8.0 * Configuration.mega2MebiFac);
		schedDTO.setSizeV(acq.getSizeV() * 4.0 * 8.0 * Configuration.mega2MebiFac);
		schedDTO.setPTAvailable(acq.isPassThroughFlag());
		schedDTO.setLinkDtoIdList(new ArrayList<String>());
		schedDTO.getLinkDtoIdList().addAll(acq.getLinkedDtoId());
		// schedDTO.setJoinedDtoId(joinedDTOId);
		// schedDTO.setLookAngle(lookAngle);
		// schedDTO.setOrbDir(orbDir);
		// schedDTO.setBeamId(beamId);

		return schedDTO;
	}

	/**
	 * Parse DM to BRM equivalent DTO List
	 * 
	 * @param pSId
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param dtoId
	 * @return
	 * @throws Exception
	 */
	public static String parseDMToEquivDTOId(String ugsId, String pRId, String aRId, String dtoId) throws Exception {

		String brmEquivDTOId = "Equiv_DI2S" + Configuration.splitChar + ugsId + Configuration.splitChar 
				+ pRId + Configuration.splitChar + aRId + Configuration.splitChar + dtoId + Configuration.splitChar;

		return brmEquivDTOId;
	}

	/**
	 * Parse Data Model to BRM task list
	 *
	 * // TODO: parse additional tasks (CMGAxis, StoreAux, DeleteAux)
	 * 
	 * @param pSId
	 * @param dmTaskList
	 * @return
	 * @throws Exception
	 */
	public static List<com.nais.spla.brm.library.main.ontology.tasks.Task> parseDMToBRMTaskList(Long pSId,
			List<Task> dmTaskList) throws Exception {

		logger.trace("Reparse Task List...");

		List<com.nais.spla.brm.library.main.ontology.tasks.Task> taskList = new ArrayList<>();

		for (it.sistematica.spla.datamodel.core.model.Task dmTask : dmTaskList) {

			taskList.add(parseDMToBRMTask(pSId, dmTask));
		}

		return taskList;
	}

	/**
	 * Transform task from Data Model to BRM according to BRM 
	 * // TODO: finalize tasks!
	 *
	 * @param pSId
	 * @param dmTask
	 * @return
	 * @throws InputException
	 */
	public static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMTask(Long pSId, Task dmTask) throws Exception {

		logger.trace("Parse retrieved Task: " + dmTask.getTaskId());

		/**
		 * The output Task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Task();

		// Get specific task info
		if (dmTask.getTaskType().equals(TaskType.ACQ)) {

			task = parseDMToBRMAcq(pSId, dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.MANEUVER)) {

			task = parseDMToBRMMan(pSId, dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.RAMP)) {

			task = parseDMToBRMRamp(dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.STORE)) {

			task = parseDMToBRMStore(pSId, dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.DWL)) {

			task = parseDMToBRMDwl(pSId, dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.SILENT)) {

			task = parseDMToBRMSil(dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.PASSTHROUGH)) {

			task = parseDMToBRMPT(pSId, dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.CMGAXIS)) {

			task = parseDMToBRMAxesReconf(dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.STOREAUX)) {

			task = parseDMToBRMStoAux(dmTask);

		} else if (dmTask.getTaskType().equals(TaskType.BITE)) {

			task = parseDMToBRMBite(dmTask);
		}

		// TODO: check default task info
		if (dmTask.getTaskId() == null) {			

			task.setIdTask(BigDecimal.valueOf(pSId + new Random(new Random().nextInt(9999999))
					.nextLong()).toString());
		}

		if (dmTask.getSatelliteId() != null) {

			task.setSatelliteId(dmTask.getSatelliteId()); 

		} else {
			task.setSatelliteId("SSAR1"); 
		}

		// Check basic tasks data
		if (dmTask.getTaskMark() != null) {

			task.setTaskMark(parseDMToBRMTaskMark(dmTask.getTaskMark()));
		} else {
			dmTask.setTaskMark(TaskMarkType.NOMINAL);
		}

		if (dmTask.getTaskStatus() != null) {

			dmTask.setTaskStatus(dmTask.getTaskStatus());
		} else {

			dmTask.setTaskStatus(TaskStatus.Unchecked);	
		}

		if (dmTask.isRemovableFlag() == null || dmTask.isRemovableFlag()) 
		{
			task.setRemovableFlag(true);
		} 
		else 
		{
			task.setRemovableFlag(false);
		}

		// Set Previous MH flag
		if (RequestChecker.isInsideMH(pSId, dmTask)) {

			task.setPreviousMh(true);
		}

		// Set Di2SInfo
		if (dmTask.getDi2s() != null) {

			task.setDi2sInfo(parseDMToBRMDI2SInfo(pSId, dmTask.getDi2s(), 
					parseDMToSchedDTOId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(), 
							dmTask.getAcquisitionRequestId(), dmTask.getDtoId()),
					SessionActivator.ugsOwnerIdMap.get(pSId).get(dmTask.getUgsId()))); 
		}

		return task;
	}

	/**
	 * Parse DM To BRM Acquisition
	 * // TODO: Theatre and standard PRType
	 * @param pSId
	 * @param dmTask
	 * @return
	 * @throws Exception
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMAcq(Long pSId, Task dmTask) 
			throws Exception {

		/**
		 * The acquisition Id
		 */		
		String acqId = parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId());

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Acquisition(
				acqId, dmTask.getStartTime(), dmTask.getStopTime(), ((Acquisition) dmTask).getLookSide(), dmTask.getSatelliteId(),
				parseDMToBRMSensorMode(((Acquisition) dmTask).getSensorMode()));

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.ACQUISITION);

		logger.trace("Parse acquisition data.");

		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition)task).setPrType(
				parseDMToBRMPRType(((Acquisition) dmTask).getType()));

		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
		.setImageBIC(((Acquisition) dmTask).getBic());
		
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
		.setNeo(RequestChecker.isNEOTask(pSId, dmTask));		
		
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setUserInfo(
				parseDMToBRMUserInfo(getAcqUserInfoList(pSId, parseDMToSchedPRId(
						dmTask.getUgsId(), dmTask.getProgrammingRequestId()), 
						((Acquisition)dmTask).getUgsOwnerList())));

		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setEss(
				((Acquisition) dmTask).getEss());

		if (((Acquisition) dmTask).getWeightedRank() != null) {
			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
			.setPriority(((Acquisition) dmTask).getWeightedRank().intValue());
		}

		// TODO: change according to the DM polarization
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
		.setPolarization(parseDMToBRMPolar(((Acquisition) dmTask).getPolarization()));

		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
		.setPrMode(com.nais.spla.brm.library.main.ontology.enums.PRMode.Standard);
		
		if (dmTask.getDi2s()!= null) {	
			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task)
			.setPrMode(com.nais.spla.brm.library.main.ontology.enums.PRMode.DI2S);
		} 

		// else if (Theatre, Experimental)

		if (((Acquisition) dmTask).getSizeH() == null) {
			((Acquisition) dmTask).setSizeH(0.0);
		}

		if (((Acquisition) dmTask).getSizeV() == null) {
			((Acquisition) dmTask).setSizeV(0.0);
		}

		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setSizeH(Integer.valueOf(
				((Acquisition) dmTask).getSizeH().intValue()));
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setSizeV(Integer.valueOf(
				((Acquisition) dmTask).getSizeV().intValue()));

		if (dmTask.getDi2s() != null) {

			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setDi2sAvailable(true);
		}

		// Added on 20/4/2020
		String schedARId = parseDMToSchedARId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(), 
				dmTask.getAcquisitionRequestId());

		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(schedARId)) {

			EncryptionInfo encryptionInfo = new EncryptionInfo(
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionStrategy(),
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionEKSelection(), 
					PRListProcessor.aRSchedIdMap.get(pSId).get(schedARId).getEncryptionIVSelection());

			task.setEncrypInfo(encryptionInfo);
		}		
		
		/**
		 * The preferred stations id list
		 */	 
		ArrayList<String> prefStationIdList = new ArrayList<String>();
		
		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(getSchedPRId(schedARId))) {

			prefStationIdList.addAll(PRListProcessor.pRSchedIdMap.get(pSId).get(getSchedPRId(schedARId))
					.getUserList().get(0).getAcquisitionStationIdList());
		}
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setPreferredVis(prefStationIdList);
	
		/**
		 * The backup stations id list
		 */
		ArrayList<String> backStationIdList = new ArrayList<String>();

		if (SessionActivator.ugsBackStationIdListMap.get(pSId).containsKey(dmTask.getUgsId())) {
																					

			backStationIdList.addAll(SessionActivator.ugsBackStationIdListMap.get(pSId)
					.get(dmTask.getUgsId()));
		}
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setBackupVis(backStationIdList);

		// Added on 20/01/2021: add backup visibilities for S-TUP acquisition inside antePrevious MH
		if (RequestChecker.isInsideAntePrevMH(pSId, dmTask.getStartTime().getTime())) {
			
			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).getPreferredVis()
			.addAll(backStationIdList);
		}
		
		// Added on 19/11/2020
		String schedDTOId = parseDMToSchedDTOId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(), 
				dmTask.getAcquisitionRequestId(), dmTask.getAcquisitionRequestId());

		// Updated on 07/09/2022 from CSPS-2.8.0
//		Integer totalPartsH = computeDwlTotalParts(pSId, schedDTOId,  Polarization.HH);
//		Integer totalPartsV = computeDwlTotalParts(pSId, schedDTOId,  Polarization.VV);
//		
//		if (totalPartsH > 0) {
			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setTotalPartH(
					computeDwlTotalParts(pSId, schedDTOId,  Polarization.HH));
//		}
//		
//		if (totalPartsV > 0) {
			((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setTotalPartV(
					computeDwlTotalParts(pSId, schedDTOId,  Polarization.VV));
//		}

		// Added on 22/1/2021 for S-TUP expansion
		((com.nais.spla.brm.library.main.ontology.tasks.Acquisition) task).setTup(TUPCalculator.isUgsTUP(pSId, dmTask.getUgsId()));
		
		return task;
	}

	/**
	 * Parse DM to BRM Storage
	 * @param dmTask
	 * @return
	 * @throws Exception 
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMStore(Long pSId, Task dmTask) throws Exception {

		/**
		 * The storage Id
		 */
		String stoId = parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId());

		if (((Store) dmTask).getSourcePacketNumberH() == null) {
			((Store) dmTask).setSourcePacketNumberH(0L);
		}

		if (((Store) dmTask).getSourcePacketNumberV() == null) {
			((Store) dmTask).setSourcePacketNumberV(0L);
		}

		if (((Store) dmTask).getPacketStoreSizeH() == null) {
			((Store) dmTask).setPacketStoreSizeH(0L);
		}

		if (((Store) dmTask).getPacketStoreSizeV() == null) {
			((Store) dmTask).setPacketStoreSizeV(0L);
		}

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new Storage(stoId, 
				(int) (((Store) dmTask).getPacketStoreSizeV().longValue()),
				(int) (((Store) dmTask).getPacketStoreSizeH().longValue()), 
				dmTask.getStartTime(), dmTask.getStopTime(),
				parseDMToBRMPolar(((Store) dmTask).getPolarization()));

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.STORE);
		task.setSatelliteId(dmTask.getSatelliteId());

		logger.trace("Parse storage data.");

		((Storage) task).setIdTask(stoId);
		((Storage) task).setRelatedAcqId(stoId);
		
		// Added on 13/07/2022
		((Storage) task).setUgsId(dmTask.getUgsId());

		/**
		 * The list of stores
		 */
		ArrayList<Store> storeList = new ArrayList<>();
		storeList.add(((Store) dmTask));
		((Storage) task).setPacketsAssociated(getPacketData(dmTask.getUgsId(),
				dmTask.getProgrammingRequestId(), dmTask.getAcquisitionRequestId(), 
				dmTask.getDtoId(), storeList));
		((Storage) task).setPreviousMh(true);

		logger.info("Previous storage: " + task.toString());

		return task;
	}

	/**
	 * Parse DM to BRM Maneuver
	 * 
	 * @param pSId
	 * @param dmTask
	 * @param prevMH
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMMan(Long pSId, Task dmTask) { 

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Maneuver(
				((Maneuver) dmTask).getAcqIdFrom(), ((Maneuver) dmTask).getAcqIdTo(), dmTask.getStartTime(), dmTask.getStopTime(),
				dmTask.getSatelliteId());

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.MANEUVER);
		task.setIdTask(((Maneuver) dmTask).getAcqIdFrom() + Configuration.splitChar + ((Maneuver) dmTask).getAcqIdTo());
		task.setPreviousMh(true);

		logger.trace("Parse Maneuver data.");

		Boolean rlFlag = ((Maneuver) dmTask).getRightToLeftFlag();

		if (rlFlag == null) {

			rlFlag = false;
		}

		if (PRListProcessor.equivStartTimeIdMap.get(pSId).containsKey(Long.toString(dmTask.getStartTime().getTime())))  {

			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setReferredEquivalentDto(
					PRListProcessor.equivStartTimeIdMap.get(pSId).get(Long.toString(dmTask.getStartTime().getTime())));			
		}

		((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setRightToLeftFlag(rlFlag);

		if (((Maneuver) dmTask).getActuator().toString().contains("CMG")) {
			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setActuator(Actuator.CMGA);

		} else {

			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setActuator(Actuator.ReactionWheels);
		}

		if (((Maneuver) dmTask).getManeuverType().toString().equalsIgnoreCase(("PitchSlew"))) {

			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setType(ManeuverType.PitchSlew);

			logger.debug("Check Pitch Interval for Maneuver " + ((Maneuver) dmTask).getTaskId()
					+ " with start time: " + dmTask.getStartTime().getTime());

			if (((Maneuver) dmTask).retrievePitchIntervals() != null
					&& !((Maneuver) dmTask).retrievePitchIntervals().getPitchIntervalDetailsList().isEmpty()) {

				((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setPitchIntervals(
						parseDMToBRMPitchInts(dmTask.getTaskId().toString(), 
								((Maneuver) dmTask).retrievePitchIntervals()));

				logger.debug("Found Pitch Intervals in Maneuver: " + dmTask.getTaskId());

				logger.debug("The Maneuver has the following Pitch Intervals: " +
						((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).getPitchIntervals().toString());	

			} else if (PersistPerformer.pitchIntervalMap.containsKey(Long.toString(dmTask.getStartTime().getTime()))) {

				((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setPitchIntervals(
						parseDMToBRMPitchInts(dmTask.getTaskId().toString(), 
								PersistPerformer.pitchIntervalMap.get(Long.toString(dmTask.getStartTime().getTime()))));

				logger.debug("Found Pitch Intervals in internal map: " + dmTask.getTaskId());

				logger.debug("The Maneuver has the following Pitch Intervals: " +
						((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).getPitchIntervals().toString());

			} else {

				logger.warn("No Pitch Intervals associated to Task: " + dmTask.getTaskId());				
			}

		} else if (((Maneuver) dmTask).getManeuverType().toString().equalsIgnoreCase(("PitchCPS"))) {

			logger.debug("Check Pitch Interval for Maneuver: " + ((Maneuver) dmTask).getManeuverType()
					+ " with start time: " + dmTask.getStartTime().getTime());

			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setType(ManeuverType.PitchCPS);

			if (((Maneuver) dmTask).retrievePitchIntervals() != null
					&& !((Maneuver) dmTask).retrievePitchIntervals().getPitchIntervalDetailsList().isEmpty()) {

				((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setPitchIntervals(
						parseDMToBRMPitchInts(dmTask.getTaskId().toString(), ((Maneuver) dmTask).retrievePitchIntervals()));

				logger.debug("Found Pitch Intervals in Maneuver: " + dmTask.getTaskId());

				logger.debug("The Maneuver has the following Pitch Intervals: " +
						((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).getPitchIntervals().toString());

			} else if (PersistPerformer.pitchIntervalMap.containsKey(Long.toString(dmTask.getStartTime().getTime()))) {

				((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setPitchIntervals(
						parseDMToBRMPitchInts(dmTask.getTaskId().toString(), 
								PersistPerformer.pitchIntervalMap.get(Long.toString(dmTask.getStartTime().getTime()))));

				logger.debug("Found Pitch Intervals in internal map: " + dmTask.getTaskId());

				logger.debug("The Maneuver has the following Pitch Intervals: " +
						((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).getPitchIntervals().toString());


			} else {

				logger.warn("No Pitch Intervals associated to Task: " + dmTask.getTaskId());				
			}

		} else {

			((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) task).setType(ManeuverType.RollSlew);
		}

		return task;
	}

	/**
	 * The parser between DataModel and BRM Pitch Intervals
	 * // Updated on 28/04/2022 for intervalId sorting
	 * 
	 * @param manId
	 * @param pInts
	 * @return
	 */
	private static ArrayList<com.nais.spla.brm.library.main.ontology.utils.PitchIntervals> parseDMToBRMPitchInts(
			String manId, PitchIntervals pInts) {

		logger.debug("Parse Pitch Interval for Maneuver: " + manId);

		/**
		 * The BRM Pitch Interval
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.utils.PitchIntervals> brmPInts = new ArrayList<>();

		for (int i = 0; i < pInts.getPitchIntervalDetailsList().size(); i ++) {

			/**
			 * The Pitch interval details
			 */
			PitchIntervalDetail pIntDet = pInts.getPitchIntervalDetailsList().get(i);

			/**
			 * The BRM Pitch intervals
			 */
			com.nais.spla.brm.library.main.ontology.utils.PitchIntervals brmPInt = new com.nais.spla.brm.library.main.ontology.utils.PitchIntervals(
					pIntDet.getIntervalId(), pIntDet.getIntervalDuration(), pIntDet.getMaxPich());

			brmPInts.add(brmPInt);

			logger.debug("Pitch interval added to Maneuver with Id: " + brmPInt.getIntervalId());				

		}
		
		// Added on 28/04/2022 for sorting by IntervalId
		Collections.sort(brmPInts, new PitchIntDetComparator());
		
		return brmPInts;
	}

	/**
	 * Parse DM to BRM ramp
	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMRamp(Task dmTask) {

		/**
		 * The Ramp Id
		 */
		String rampId = parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId());

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new RampCMGA(dmTask.getTaskId().toString(), dmTask.getStartTime(), dmTask.getStopTime(),
				((Ramp) dmTask).getUpFlag(), dmTask.getSatelliteId());		

		logger.trace("Parse Ramp data.");
		task.setIdTask(rampId);
		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.RAMP);

		return task;

	}

	/**
	 * Parse DM to BRM silent
	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMSil(Task dmTask) {

		/**
		 * The silent Id
		 */
		String silId = parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId());

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Silent(silId,
				((it.sistematica.spla.datamodel.core.model.task.Silent) dmTask).getEss());
		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.SILENT);
		task.setStartTime(dmTask.getStartTime());
		task.setEndTime(dmTask.getStopTime());
		task.setPreviousMh(true);
		task.setSatelliteId(dmTask.getSatelliteId());
		task.setIdTask(parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId()));

		logger.trace("Parse Silent data.");

		((com.nais.spla.brm.library.main.ontology.tasks.Silent) task)
		.setLoanFromEss(((it.sistematica.spla.datamodel.core.model.task.Silent) dmTask).getLoanedESS());

		return task;

	}

	/**
	 * Parse DM to BRM Passthrough
	 * 
	 * // TODO: handle subscribed PTs
	 * @param dmTask
	 * @return
	 * @throws Exception 
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMPT(Long pSId, Task dmTask) throws Exception {

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.PassThrough();

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.PASSTHROUGH);
		task.setStartTime(dmTask.getStartTime());
		task.setEndTime(dmTask.getStopTime());
		task.setPreviousMh(true);
			
		task.setSatelliteId(dmTask.getSatelliteId());
		task.setIdTask(parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId()));
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough)task)
		.setAcqStatId(((PassThrough) dmTask).getAcquisitionStationId());
		
		// Added on 18/02/2022 for PassThrough management
		ArrayList<String> acqStationIdList = new ArrayList<String>();
		acqStationIdList.add(((PassThrough) dmTask).getAcquisitionStationId());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough)task).setGroundStationId(acqStationIdList);

		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task).setUgsOwnerList(parseDMToBRMUgsOwnerList(
				(((PassThrough) dmTask).getUgsOwnerList())));

		logger.trace("Parse passthrough data.");

		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setContactCounterVis(((PassThrough) dmTask).getContactCounter());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setCarrierL2SelectionH(((PassThrough) dmTask).getCarrierL2SelectionH());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setCarrierL2SelectionV(((PassThrough) dmTask).getCarrierL2SelectionV());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setDataStrategyH(((PassThrough) dmTask).getDataStrategyH());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setDataStrategyV(((PassThrough) dmTask).getDataStrategyV());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setMemoryModulesH(((PassThrough) dmTask).getHMemoryModules());
		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setMemoryModulesV(((PassThrough) dmTask).getVMemoryModules());

		if (((PassThrough) dmTask).getPacketStoreSizeH() == null) {
			((PassThrough) dmTask).setPacketStoreSizeH(BigDecimal.ZERO);
		}

		if (((PassThrough) dmTask).getPacketStoreSizeV() == null) {
			((PassThrough) dmTask).setPacketStoreSizeV(BigDecimal.ZERO);
		}			

		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setPacketStoreSizeH((int)(((PassThrough) dmTask)
				.getPacketStoreSizeH().longValue()));

		((com.nais.spla.brm.library.main.ontology.tasks.PassThrough) task)
		.setPacketStoreSizeV((int)(((PassThrough) dmTask)
				.getPacketStoreSizeV().longValue()));

		// Added on 20/4/2020
		String schedARId = parseDMToSchedARId(dmTask.getUgsId(), 
				dmTask.getProgrammingRequestId(), dmTask.getAcquisitionRequestId());

		if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedARId(schedARId))) {

			EncryptionInfo encryptionInfo = new EncryptionInfo(
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionStrategy(),
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionEKSelection(), 
					PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionIVSelection());

			task.setEncrypInfo(encryptionInfo);
		}
		
		

		return task;
	}

	/**
	 * Parse DM to BRM Bite

	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMBite(Task dmTask) {

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Bite(((BITE) dmTask).getModuleSelectionFlag(),
				((BITE) dmTask).getModuleId(), ((BITE) dmTask).getFillerWord(),
				Long.toString(((BITE) dmTask).getPacketStoreId()));

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.BITE);
		task.setStartTime(dmTask.getStartTime());
		task.setEndTime(dmTask.getStopTime());
		task.setPreviousMh(true);
		task.setSatelliteId(dmTask.getSatelliteId());
		task.setIdTask(parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId()));

		((com.nais.spla.brm.library.main.ontology.tasks.Bite)task).setPacketStoreSize(
				((BITE) dmTask).getPacketStoreSize().doubleValue());	

		// Set visibility data
		((com.nais.spla.brm.library.main.ontology.tasks.Bite)task).setContactCounter(
				((BITE) dmTask).getContactCounter());
		((com.nais.spla.brm.library.main.ontology.tasks.Bite)task).setAcqStatId(
				((BITE) dmTask).getAcquisitionStationId());

		((com.nais.spla.brm.library.main.ontology.tasks.Bite)task).setCarrierL2Selection(
				((BITE) dmTask).getCarrierL2Selection());	

		return task;
	}

	/**
	 * Parse DM to BRM Download
	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMDwl(Long pSId, Task dmTask) {

		/**
		 * The download Id
		 */
		String dwlId = parseDMToBRMTaskId(dmTask.getUgsId(), dmTask.getProgrammingRequestId(),
				dmTask.getAcquisitionRequestId(), dmTask.getDtoId());

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.Download(dmTask.getSatelliteId());

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.DOWNLOAD);
		task.setStartTime(dmTask.getStartTime());
		task.setEndTime(dmTask.getStopTime());
		task.setIdTask(dwlId);
		task.setRelatedTaskId(dwlId);

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setRelatedTaskId(dwlId);

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setUgsOwnerList(
				parseDMToBRMUgsOwnerList((((Download) dmTask).getUgsOwnerList())));

		logger.trace("Parse download data.");

		if (((Download) dmTask).getPacketStoreSize() == null) {

			logger.warn("Null packet store size for download: " + dwlId);
			((Download) dmTask).setPacketStoreSize(BigDecimal.ZERO);
		}

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task)
		.setDownloadedSize((int)(((Download) dmTask)
				.getPacketStoreSize().doubleValue()));

		if (((Download) dmTask).getPacketStoreId()== null) {

			logger.warn("Null packet store Id for download: " + dwlId);
			((Download) dmTask).setPacketStoreId("0");
		} 

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task)
		.setPacketStoreNumber(Integer.valueOf(((Download) dmTask).getPacketStoreId()));

		if (((Download) dmTask).getSourcePacketNumberH() == null 
				|| ((Download) dmTask).getSourcePacketNumberH().equals(BigDecimal.ZERO))
		{	
			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setPol(Polarization.VV);

		} else {

			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setPol(Polarization.HH);						
		}

		if (! task.getIdTask().contains("null")) {

			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setPlannedOnMemModule(getMMMap( 
					dwlId, ((Download) dmTask).getMemoryModules()));
		}

		if (((Download) dmTask).getDataStrategy() != null) {

			/**
			 * The data strategy
			 */
			DownlinkStrategy dwlStrategy = DownlinkStrategy.RETAIN;

			if (((Download) dmTask).getDataStrategy().equals(true)) 
			{
				dwlStrategy = DownlinkStrategy.DELETE;
			}		
			((com.nais.spla.brm.library.main.ontology.tasks.Download) task)
			.setPacketStoreStrategy(dwlStrategy);
		} 
		else 
		{
			logger.warn("Null data strategy for download: " + dwlId);
		}

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setContactCounter(
				((Download) dmTask).getContactCounter());

		if (((Download) dmTask).getCarrierL2Selection() != null) {
			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setCarrierL2Selection(
					((Download) dmTask).getCarrierL2Selection());
		} else {

			logger.warn("Null carrier selection for download: " + dwlId);
		}

		((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setAcqStatId(
				((Download) dmTask).getAcquisitionStationId());

		// Set Download shifts
		if (((Download) dmTask).getShiftPointer() != null) {

			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setSectorShift(
					((Download) dmTask).getShiftPointer());
		}
		if (((Download) dmTask).getTimeShift() != null) {

			((com.nais.spla.brm.library.main.ontology.tasks.Download) task).setTimeShiftMillisec(
					(int)(((Download) dmTask).getTimeShift() * 1000.0));
		}

		// Added on 20/4/2020
		if (dmTask.getProgrammingRequestId() != null) { // No GPS case

			String schedARId = parseDMToSchedARId(dmTask.getUgsId(), 
					dmTask.getProgrammingRequestId(), dmTask.getAcquisitionRequestId());

			if (PRListProcessor.aRSchedIdMap.get(pSId).containsKey(ObjectMapper.getSchedARId(schedARId))) {

				EncryptionInfo encryptionInfo = new EncryptionInfo(
						PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionStrategy(),
						PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionEKSelection(), 
						PRListProcessor.aRSchedIdMap.get(pSId).get(ObjectMapper.getSchedARId(schedARId)).getEncryptionIVSelection());

				task.setEncrypInfo(encryptionInfo);
			}
		}

		return task;
	}

	/**
	 * Get the Packet Store Memory Modules List
	 * 
	 * @param taskId
	 * @param mmList
	 * @return
	 */
	private static HashMap<MemoryModule, Long> getMMMap(String taskId, List<Integer> mmList) {

		/**
		 * The map of download Memory Modules
		 */
		HashMap<MemoryModule, Long> dwlMMList = new HashMap<>();

		if (mmList != null && ! mmList.isEmpty()) {

			for (int i = 0; i < mmList.size(); i++) {

				// Add MM
				dwlMMList.put(new MemoryModule("mm" + (i + 1)), new Long(mmList.get(i)));
			}

		} else {

			logger.warn("No Memory Modules are associated to task: " + taskId);
		}

		return dwlMMList;
	}

	/**
	 * Parse DM to BRM Axes reconfiguration
	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMAxesReconf(Task dmTask) {

		/**
		 * The output task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new com.nais.spla.brm.library.main.ontology.tasks.CMGAxis(dmTask.getStartTime(),
				dmTask.getStopTime());

		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.AXES_RECONF);
		task.setSatelliteId(dmTask.getSatelliteId());
		task.setPreviousMh(true);
		task.setSatelliteId(dmTask.getSatelliteId());
		((com.nais.spla.brm.library.main.ontology.tasks.CMGAxis) task).setRollToPitch(((
				CMGAxis) dmTask).getRollAxisFlag());

		return task;

	}

	/**
	 * Parse DM to BRM StoreAux
	 * @param dmTask
	 * @return
	 */
	private static com.nais.spla.brm.library.main.ontology.tasks.Task parseDMToBRMStoAux(Task dmTask) {

		/**
		 * The input task
		 */
		com.nais.spla.brm.library.main.ontology.tasks.Task task = new StoreAUX(((StoreAux)dmTask).getEnableFlag(), ((StoreAux)dmTask).getPacketStoreId());

		task.setStartTime(dmTask.getStartTime());
		task.setEndTime(dmTask.getStopTime());
		task.setTaskType(com.nais.spla.brm.library.main.ontology.enums.TaskType.STORE_AUX);
		task.setSatelliteId(dmTask.getSatelliteId());
		task.setPreviousMh(true);
		task.setSatelliteId(dmTask.getSatelliteId());

		return task;
	}

	/**
	 * Parse DM Planning Session type to BRM Session type
	 *
	 * @param pSId
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.enums.SessionType parseDMToBRMSessionType(Long pSId) throws Exception {

		/**
		 * The session type
		 */
		com.nais.spla.brm.library.main.ontology.enums.SessionType sessionType = com.nais.spla.brm.library.main.ontology.enums.SessionType.premium;

		if (SessionChecker.isRoutine(pSId)) {

			sessionType = com.nais.spla.brm.library.main.ontology.enums.SessionType.routine;

		} else if (SessionChecker.isDelta(pSId)) {

			sessionType = com.nais.spla.brm.library.main.ontology.enums.SessionType.urgent;

		} else {

			sessionType = com.nais.spla.brm.library.main.ontology.enums.SessionType.premium;
		}

		logger.debug("A Planning Session " + sessionType.toString() + " is parsed.");

		return sessionType;
	}

	/**
	 * Parse Common to Planning Session type
	 *
	 * @param commonType
	 * @return
	 */
	public static PlanningSessionType parseCommonToPlanSessionType(Common.SessionType commonType) throws Exception {

		switch (commonType) {

		case AllPartnersCheckConflict:
			return PlanningSessionType.AllPartnersCheckConflict;
		case AllPartnersLimitedCheckConflict:
			return PlanningSessionType.AllPartnersLimitedCheckConflict;
		case InterCategoryRankedRoutine:
			return PlanningSessionType.InterCategoryRankedRoutine;
		case IntraCategoryRankedRoutine:
			return PlanningSessionType.IntraCategoryRankedRoutine;
		case Negotiation:
			return PlanningSessionType.Negotiation;
		case PartnerOnlyCheckConflict:
			return PlanningSessionType.PartnerOnlyCheckConflict;
		case Poll:
			return PlanningSessionType.Poll;
		case LastMinutePlanning:
			return PlanningSessionType.LastMinutePlanning;
		case VeryUrgent:
			return PlanningSessionType.VeryUrgent;
		case UnrankedRoutine:
			return PlanningSessionType.UnrankedRoutine;
		case ManualPlanning:
			return PlanningSessionType.ManualPlanning;
		case SelfGenerated:
			return PlanningSessionType.SelfGenerated;

		default:
			logger.warn("A default Session type is parsed.");
			return PlanningSessionType.AllPartnersCheckConflict;
		}
	}

	/**
	 * Parse Planning Session Status to session Status
	 *
	 * @param commonStatus
	 * @return
	 */
	public static PlanningSessionStatus parseCommonToPlanSessionStatus(Common.PlanningSessionStatus commonStatus)
			throws Exception {

		switch (commonStatus) {

		case Candidate:
			return PlanningSessionStatus.Candidate; // # 1.1 Main: HP Intra
			// Harm
		case Current:
			return PlanningSessionStatus.Current;
		case Draft:
			return PlanningSessionStatus.Draft;
		case Expired:
			return PlanningSessionStatus.Expired;
		case Nominated:
			return PlanningSessionStatus.Nominated; // # 1.1.E1: PP Inter
			// Harm
			// # 1.1.E3: RR Inter
			// Harm
		case Obsolete:
			return PlanningSessionStatus.Obsolete;
		case Previous:
			return PlanningSessionStatus.Previous;
		case Disposable:
			return PlanningSessionStatus.Disposable;

		case Final:
			return PlanningSessionStatus.Final; // # 1.1.E4: Unrank Harm
		case Ranked:
			return PlanningSessionStatus.Ranked; // # 1.1.E2: RR Intra Harm
			// // TODO: Add case
		default:
			logger.warn("A default Session status is parsed.");		
			return PlanningSessionStatus.Candidate;

		}
	}

	/**
	 * Parse BRM to DataModel Task Mark
	 *
	 * @param commonStatus
	 * @return
	 */
	public static TaskMarkType parseBRMToDMTaskMark(com.nais.spla.brm.library.main.ontology.enums.TaskMarkType taskMark)
			throws Exception {

		switch (taskMark) {

		case UNCHANGED:
			return TaskMarkType.UNCHANGED;
		case CONFIRMED:
			return TaskMarkType.CONFIRMED;
		case DELETED:
			return TaskMarkType.DELETED;
		case NEW:
			return TaskMarkType.NEW;
		case NOMINAL:
			return TaskMarkType.NOMINAL;
		}
		return null;
	}

	/**
	 * Parse DataModel to BRM Task Mark
	 *
	 * @param commonStatus
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.enums.TaskMarkType parseDMToBRMTaskMark(
			TaskMarkType taskMark)
					throws Exception {

		switch (taskMark) {

		case NOMINAL:
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.NOMINAL;
		case UNCHANGED:
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.UNCHANGED;
		case CONFIRMED:
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.CONFIRMED;
		case DELETED:
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.DELETED;
		case NEW:
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.NEW;
		default:
			logger.warn("A default Task mark type is parsed.");
			return com.nais.spla.brm.library.main.ontology.enums.TaskMarkType.NOMINAL;
		}

	}

	/**
	 * Parse from Data Model to BRM DTO sensor mode // TODO: add Experimental modes
	 *
	 * @param commonType
	 * @return
	 */
	public static TypeOfAcquisition parseDMToBRMSensorMode(
			DTOSensorMode sensorMode) throws Exception {

		if (sensorMode != null) {

			switch (sensorMode) {

			// Case BITE
			case BITE:
				return TypeOfAcquisition.BITE;
		
			// Case CalVal
			case POC2048:
				return TypeOfAcquisition.POC2048;
			case TRCAL:
				return TypeOfAcquisition.TRCAL;
	
			// Case NoSmolt				
				  
									   
			case SPOTLIGHT_1_EXP:
				return TypeOfAcquisition.SPOTLIGHT_1_EXP;
			case SPOTLIGHT_2_EXP:
				return TypeOfAcquisition.SPOTLIGHT_2_EXP;
			case SPOTLIGHT_2_MOS:
				return TypeOfAcquisition.SPOTLIGHT_2_MOS;
			case SPOTLIGHT_2_MSJN:
				return TypeOfAcquisition.SPOTLIGHT_2_MSJN;
			case SPOTLIGHT_1_MSOR:
				return TypeOfAcquisition.SPOTLIGHT_1_MSOR;
			case SPOTLIGHT_2_MSOS:
				return TypeOfAcquisition.SPOTLIGHT_2_MSOS;
			
			// Case Nominal
			case QUADPOL:
				return TypeOfAcquisition.QUADPOL;
			case SCANSAR_1:
				return TypeOfAcquisition.SCANSAR_1;
			case SCANSAR_2:
				return TypeOfAcquisition.SCANSAR_2;
			case PINGPONG:
				return TypeOfAcquisition.PINGPONG;
			case SPOTLIGHT_1A:
				return TypeOfAcquisition.SPOTLIGHT_1A;
			case SPOTLIGHT_1B:
				return TypeOfAcquisition.SPOTLIGHT_1B;
				// case SPOTLIGHT_1S:
				// return TypeOfAcquisition.SPOTLIGHT_1S;
			case SPOTLIGHT_2A:
				return TypeOfAcquisition.SPOTLIGHT_2A;
			case SPOTLIGHT_2B:
				return TypeOfAcquisition.SPOTLIGHT_2B;
			case SPOTLIGHT_2C:
				return TypeOfAcquisition.SPOTLIGHT_2C;
			case STRIPMAP:
				return TypeOfAcquisition.STRIPMAP;

			// Case Quadpol	
			case SPOTLIGHT_1_EQR:
				return TypeOfAcquisition.SPOTLIGHT_1_EQR;
			case SPOTLIGHT_1_OQR: 
				return TypeOfAcquisition.SPOTLIGHT_1_OQR;
			case SPOTLIGHT_2_EQS:
				return TypeOfAcquisition.SPOTLIGHT_2_EQS;
			case SPOTLIGHT_2_OQS:
				return TypeOfAcquisition.SPOTLIGHT_2_OQS;											   
			
			// Case default
			default:										
				return TypeOfAcquisition.SPOTLIGHT_1A;
			}
		}

		return null;
	}

	/**
	 * Parse from BRM to Data Model DTO sensor mode // TODO: finalize cases in BRM
	 *
	 * @param commonType
	 * @return
	 */
	public static DTOSensorMode parseBRMToDMSensorMode(
			TypeOfAcquisition sensorMode) throws Exception {

		switch (sensorMode) {

		// Case BITE
		case BITE:
			return DTOSensorMode.BITE;
		
		// Case Calibration
		case POC2048:
			return DTOSensorMode.POC2048;
		case TRCAL:
			return DTOSensorMode.TRCAL;
		
		// Case Nominal 
		case PINGPONG:
				return DTOSensorMode.PINGPONG;			
		case QUADPOL:
			return DTOSensorMode.QUADPOL;
		case SCANSAR_1:
			return DTOSensorMode.SCANSAR_1;
		case SCANSAR_2:
			return DTOSensorMode.SCANSAR_2;								 
		case SPOTLIGHT_1A:
			return DTOSensorMode.SPOTLIGHT_1A;
		case SPOTLIGHT_1B:
			return DTOSensorMode.SPOTLIGHT_1B;
		//		case SPOTLIGHT_1S:
		//		return DTOSensorMode.SPOTLIGHT_1S;
		case SPOTLIGHT_2A:
			return DTOSensorMode.SPOTLIGHT_2A;
		case SPOTLIGHT_2B:
			return DTOSensorMode.SPOTLIGHT_2B;
		case SPOTLIGHT_2C:
			return DTOSensorMode.SPOTLIGHT_2C;
		case STRIPMAP:
			return DTOSensorMode.STRIPMAP;
		
		// Case NoSmolt	
		case SPOTLIGHT_1_EXP:
			return DTOSensorMode.SPOTLIGHT_1_EXP;
		case SPOTLIGHT_2_EXP:
			return DTOSensorMode.SPOTLIGHT_2_EXP;
		case SPOTLIGHT_2_MOS:
			return DTOSensorMode.SPOTLIGHT_2_MOS;	
			
		// Case DI2S
		case SPOTLIGHT_2_MSJN:
			return DTOSensorMode.SPOTLIGHT_2_MSJN;
		case SPOTLIGHT_1_MSOR:
			return DTOSensorMode.SPOTLIGHT_1_MSOR;
		case SPOTLIGHT_2_MSOS:
			return DTOSensorMode.SPOTLIGHT_2_MSOS;

	    // Case Quadpol
		case SPOTLIGHT_1_EQR:
			return DTOSensorMode.SPOTLIGHT_1_EQR; 
		case SPOTLIGHT_1_OQR:
			return DTOSensorMode.SPOTLIGHT_1_OQR; 
			case SPOTLIGHT_2_EQS:
			return DTOSensorMode.SPOTLIGHT_2_EQS; 
		case SPOTLIGHT_2_OQS:
			return DTOSensorMode.SPOTLIGHT_2_OQS;										  
		
		// Case Default
		default:													 
			return DTOSensorMode.SPOTLIGHT_1A;
		}
	}

	/**
	 * Parse from DataModel to BRM polarization // TODO: check polarization cases
	 * according to the ICD
	 */
	public static Polarization parseDMToBRMPolar(String polar) throws Exception {

		switch (polar) {

		case "HH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH;
		case "VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VV;
		case "HH+VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH;
		case "VV+HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VV;
		case "VV+HH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VH;
		case "VV+VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VH;
		case "HH+VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV;
		case "HH+HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV;
		case "HH/HH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.H_H;
		case "HH/VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.H_V;
		case "VV/HH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.V_H;
		case "VV/VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.V_V;
		case "HH/HH+VH/VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH_HH;
		case "VV/HV+VV/HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VV_VV;
		case "HH/VH+HH/VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH_HH;
		case "VV/VV+HV/HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VV_VV;
		case "HH/VV+HH/HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH_VV;
		case "HH/VV+HH/VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH_VV;
		case "HH/VH+HV/VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV_HV;
		case "VH/VH+VV/VV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV_HV;
		case "HH/HH+HV/HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV_HV;
		case "HH/VV+HV/VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HV_VH;
		case "VV/HH+VH/HV":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VH_HV;
		case "VV/VV+VH/VH":
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.VH_VH;				
		default:
			logger.warn("A default polarization is parsed.");
			return com.nais.spla.brm.library.main.ontology.enums.Polarization.HH;
		}
	}

	/**
	 * Parse the DI2SInfo from Data Model to BRM // TODO: finalize
	 *
	 * 
	 * @param pSId
	 * @param di2SInfo
	 * @param masterSchedDTOId
	 * @param slavePartnerId
	 * @return
	 * @throws Exception
	 */
	public static Di2sInfo parseDMToBRMDI2SInfo(Long pSId, DI2SInfo di2SInfo, String masterSchedDTOId, 
			String slavePartnerId) throws Exception {

		
		
		logger.trace("Parse DI2S Info for Slave DTO: " + parseDMToSchedDTOId(di2SInfo.getUgsId(), 
				di2SInfo.getProgrammingRequestId(), di2SInfo.getAcquisitionRequestId(), di2SInfo.getDtoId()));			

		/**
		 * The BRM DI2SInfo
		 */
		Di2sInfo brmDi2sInfo = new Di2sInfo(parseDMToSchedDTOId(di2SInfo.getUgsId(), 
				di2SInfo.getProgrammingRequestId(), di2SInfo.getAcquisitionRequestId(), di2SInfo.getDtoId()),
				masterSchedDTOId, di2SInfo.getUgsId());

		if (PRListProcessor.dtoSchedIdMap.get(pSId).containsKey((
				parseDMToSchedDTOId(di2SInfo.getUgsId(), di2SInfo.getProgrammingRequestId(),
						di2SInfo.getAcquisitionRequestId(), di2SInfo.getDtoId())))) {

			// Set slave DTO
			brmDi2sInfo.setSlaveDto(parseSchedToBRMDTO(pSId, 
					parseDMToSchedDTO(pSId, parseDMToSchedDTOId(di2SInfo.getUgsId(), 
							di2SInfo.getProgrammingRequestId(), di2SInfo.getAcquisitionRequestId(), di2SInfo.getDtoId()), 
							PRListProcessor.dtoSchedIdMap.get(pSId).get(
									parseDMToSchedDTOId(di2SInfo.getUgsId(), di2SInfo.getProgrammingRequestId(),
											di2SInfo.getAcquisitionRequestId(), di2SInfo.getDtoId())), false)));
		}

		brmDi2sInfo.setPartnerId(slavePartnerId); // TODO: check!

		return brmDi2sInfo;
	}

	/**
	 * Parse from BRM to DataModel polarization
	 * // TODO: manage ambiguous cases
	 * @param polar
	 * @return
	 * @throws Exception
	 */
	public static String parseBRMToDMPolar(com.nais.spla.brm.library.main.ontology.enums.Polarization polar)
			throws Exception {

		switch (polar) {

		case HH:
			return "HH";
		case VV:
			return "VV";
		case VH:
			return "VV+VH";
		case HV:
			return "HH+VV";
		case H_H:
			return "HH/HH";
		case H_V:
			return "HH/VV";
		case V_H:
			return "VV/HH";
		case V_V:
			return "VV/VV";
		case HH_HH:
			return "HH/HH+VH/VH";
		case VV_VV:
			return "VV/HV+VV/HV";
		case HH_VV:
			return "HH/VV+HH/HV";
		case HV_HV:
			return "HH/VH+HV/VV";
		case HV_VH:
			return "HH/VV+HV/VH";
		case VH_HV:
			return "VV/HH+VH/HV";
		case VH_VH:
			return "VV/VV+VH/VH";		
		default:			
			logger.warn("A default polarization is parsed!");			
			return "HH";
		}		
	}

	/**
	 * Parse Planning Session Status to session Status
	 *
	 * @param brmPRType
	 * @param ugsId
	 * @return
	 */
	public static PRType parseBRMToDMPRType(com.nais.spla.brm.library.main.ontology.enums.PRType brmPRType, 
			String ugsId) throws Exception {

		switch (brmPRType) {

		case HP:
			return PRType.Hp;
		case CIVILIAN_HP:
			return PRType.CIVILIAN_HP;
		case CRISIS:
			if (ugsId.startsWith("2")) {
				return PRType.CRISIS_HP;
			} else {
				return PRType.CRISIS_PP;		
			}
		case PP:
			return PRType.PP;
		case RANKED_ROUTINE:
			return PRType.RANKED_ROUTINE;
		case UNRANKED_ROUTINE:
			return PRType.UNRANKED_ROUTINE;
		case VU:
			if (ugsId.startsWith("2")) {
				return PRType.VU_HP;
			} else {
				return PRType.VU_PP;		
			}
		case LMP:
			if (ugsId.startsWith("2")) {
				return PRType.LMP_HP;
			} else {
				return PRType.LMP_PP;		
			}	
		default:
			logger.warn("A default PR type is reparsed.");
			return PRType.Hp;
		}

	}

	/**
	 * Parse Planning Session Status to session Status
	 *
	 * @param commonStatus
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.enums.PRType parseDMToBRMPRType(PRType pRType)
			throws Exception {

		switch (pRType) {

		case Hp:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.HP;
		case CIVILIAN_HP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.CIVILIAN_HP;
		case CIVILIAN_PP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.PP;
		case CRISIS_HP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.CRISIS;
		case CRISIS_PP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.CRISIS;
		case CRISIS_ROUTINE:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.CRISIS;
		case PP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.PP;
		case RANKED_ROUTINE:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.RANKED_ROUTINE;
		case UNRANKED_ROUTINE:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.UNRANKED_ROUTINE;
		case VU_HP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.VU;
		case VU_PP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.VU;
		case VU_ROUTINE:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.VU;
		case LMP_HP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.LMP;
		case LMP_PP:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.LMP;
		case LMP_ROUTINE:
			return com.nais.spla.brm.library.main.ontology.enums.PRType.LMP;
		default:
			logger.warn("A default PR type is parsed.");
			return com.nais.spla.brm.library.main.ontology.enums.PRType.HP;
		}


	}

	/**
	 * Parse scheduling to BRM equivalent DTO
	 * 
	 * @param pSId
	 * @param equivSchedAR
	 * @param equivDTO
	 * @param pRMode
	 * @param extraPitch
	 * @return
	 * @throws Exception
	 */
	public static com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO parseSchedToBRMEquivDTO(Long pSId,
			SchedAR equivSchedAR, EquivalentDTO equivDTO) throws Exception {

		/**
		 * The BRM equivalent DTO
		 */
		com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = null;

		try {

			logger.trace("Parse Equivalent DTO: " + equivDTO.getEquivalentDtoId());

			brmEquivDTO = new com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO(
					equivDTO.getEquivalentDtoId(), equivDTO.getStartTime(), equivDTO.getStopTime(), 
					equivSchedAR.getPitchExtraBic());

			brmEquivDTO.setPrMode(parseDMToBRMPRMode(equivSchedAR.getPRMode()));
			brmEquivDTO.setEquivType(parseDMToBRMPRMode(equivSchedAR.getPRMode()));
			brmEquivDTO.setAllDtoInEquivalentDto(parseSchedToBRMDTOList(pSId, equivSchedAR.getDtoList()));
			brmEquivDTO.setManAssociated(new ArrayList<>());	

			for (int i = 0; i < brmEquivDTO.getAllDtoInEquivalentDto().size(); i++) {

				brmEquivDTO.getAllDtoInEquivalentDto().get(i).
				setReferredEquivalentDto(brmEquivDTO.getEquivalentDtoId());

				brmEquivDTO.setSatelliteId(brmEquivDTO.getAllDtoInEquivalentDto().get(i).getSatelliteId());
			}

			// Set DI2S Info
			if (brmEquivDTO.getEquivType().equals(com.nais.spla.brm.library.main.ontology.enums.PRMode.DI2S)) {

				logger.trace("Handle DI2S PRMode.");

				// Update Master DTO BICs
				equivSchedAR.getDtoList().get(0).setBIC(updateDI2SBIC(pSId, equivSchedAR.getDtoList().get(0)));


				for (int i = 0; i < brmEquivDTO.getAllDtoInEquivalentDto().size(); i++) {

					brmEquivDTO.getAllDtoInEquivalentDto().get(i).setDi2s(true);
				}

				/**
				 * The slave DTO Id
				 */
				String slaveSchedDTOId = equivSchedAR.getDtoList().get(1).getDTOId();

				/**
				 * The DI2S info
				 */
				String[] slaveIds = slaveSchedDTOId.split(Configuration.splitChar);

				/**
				 * The PRList Id
				 */
				String pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
						.get(parseDMToSchedPRId(slaveIds[0], slaveIds[1])).get(0);

				/**
				 * The DI2s Info
				 */
				DI2SInfo di2sInfo = new DI2SInfo();
				di2sInfo.setUgsId(slaveIds[0]);
				di2sInfo.setProgrammingRequestListId(pRListId);
				di2sInfo.setProgrammingRequestId(slaveIds[1]);
				di2sInfo.setAcquisitionRequestId(slaveIds[2]);
				di2sInfo.setDtoId(slaveIds[3]);
				di2sInfo.setTaskId(BigDecimal.ZERO);

				// Set DI2S Info
				brmEquivDTO.setDi2sInfo(parseDMToBRMDI2SInfo(pSId, di2sInfo, 
						brmEquivDTO.getAllDtoInEquivalentDto().get(0).getDtoId(),
						getPRUserInfoList(
								pSId, parseDMToSchedPRId(slaveIds[0], slaveIds[1]))
						.get(0).getOwnerId()));

			} else {

				logger.trace("Handle theatre or experimental PRMode.");

				ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Maneuver> brmManList = new ArrayList<>();			

				// Set theatre/experimental maneuvers
				if (equivDTO.getTaskList() != null && !equivDTO.getTaskList().isEmpty()) {

					// The maneuver counter
					int i = 0;

					for (Task man : equivDTO.getTaskList()) {

						logger.debug("Following maneuver is retrieved for Equivalent DTO " 
						+ equivDTO.getEquivalentDtoId() + " of AR " + equivSchedAR.getARId() 
						+  " : " + man.toString());
						
						// Add maneuver
						brmManList.add((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) ObjectMapper
								.parseDMToBRMMan(pSId, man));					
						brmManList.get(i).setPreviousMh(false);

						i ++;
					}

					brmEquivDTO.setManAssociated(brmManList);

				} else {

					logger.trace("No maneuvers are associated to the Equivalent DTO.");
				}
			}
		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return brmEquivDTO;
	}

	/**
	 * Update the DI2S BIC 
	 * // TODO: finalize
	 * @param pSId
	 * @param schedDTO
	 * @return
	 * @throws Exception 
	 */
	private static double updateDI2SBIC(Long pSId, SchedDTO schedDTO) throws Exception {

		/**
		 * The left factor
		 */
		int leftFac = 0;

		if (schedDTO.getLookSide().equalsIgnoreCase("Left")) {
			
			leftFac = 1;
		}

		// Compute Data Volume as a function of size and polarization
		/**
		 * The data volume
		 */
		double dataVol = 0.0;

		if (Configuration.dataVolBICMap.containsKey(schedDTO.getSensorMode())) {

			Configuration.dataVolBICMap.get(schedDTO.getSensorMode()); 

		} else {

			logger.warn("Skip Data Volume BIC computation for DTO: " + schedDTO.getDTOId());
		}

		if (RequestChecker.isDualPolar(schedDTO.getPolarization())) {

			dataVol = dataVol * 2;
		}

		/**
		 * The DI2S DTO BIC
		 */
		double dtoBIC = Configuration.essFac * RulesPerformer.getESSRatio(pSId,
				ObjectMapper.parseSchedToBRMDTO(pSId, schedDTO))
				+ Configuration.dataVolFac * dataVol
				+ leftFac * Configuration.extraLeftCost;

		return dtoBIC;


	}

	/**
	 * Parse scheduling to BRM equivalent DTO
	 * 
	 * @param pSId
	 * @param schedDTOList
	 * @param equivDTO
	 * @param pRMode
	 * @param extraPitch
	 * @return
	 * @throws Exception
	 */
	public static com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO parseSchedToBRMEquivDTO(Long pSId,
			ArrayList<SchedDTO> schedDTOList, EquivalentDTO equivDTO, PRMode pRMode, double extraPitch) throws Exception {

		logger.trace("Parse Equivalent DTO: " + equivDTO.getEquivalentDtoId());

		/**
		 * The BRM equivalent DTO
		 */
		com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = new com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO(
				equivDTO.getEquivalentDtoId(), equivDTO.getStartTime(), equivDTO.getStopTime(), extraPitch);

		brmEquivDTO.setPrMode(parseDMToBRMPRMode(pRMode));
		brmEquivDTO.setEquivType(parseDMToBRMPRMode(pRMode));
		brmEquivDTO.setAllDtoInEquivalentDto(parseSchedToBRMDTOList(pSId, schedDTOList));
		brmEquivDTO.setManAssociated(new ArrayList<>());				

		for (int i = 0; i < brmEquivDTO.getAllDtoInEquivalentDto().size(); i++) {

			brmEquivDTO.getAllDtoInEquivalentDto().get(i).
			setReferredEquivalentDto(brmEquivDTO.getEquivalentDtoId());

			brmEquivDTO.setSatelliteId(brmEquivDTO.getAllDtoInEquivalentDto().get(i).getSatelliteId());
		}

		// Set DI2S Info
		if (brmEquivDTO.getEquivType().equals(com.nais.spla.brm.library.main.ontology.enums.PRMode.DI2S)) {

			logger.trace("Handle DI2S PRMode.");

			for (int i = 0; i < brmEquivDTO.getAllDtoInEquivalentDto().size(); i++) {

				brmEquivDTO.getAllDtoInEquivalentDto().get(i).setDi2s(true);
			}

			/**
			 * The slave scheduling DTO Id
			 */
			String slaveSchedDTOId = schedDTOList.get(1).getDTOId();

			/**
			 * The PRList Id
			 */
			String pRListId = null;

			if (PRListProcessor.pRToPRListIdMap.get(pSId).containsKey(getSchedPRId(slaveSchedDTOId))) {

				pRListId = PRListProcessor.pRToPRListIdMap.get(pSId)
						.get(getSchedPRId(slaveSchedDTOId)).get(0);
			} else {

				logger.warn("No PRList Id found for scheduling PR: " + getSchedPRId(slaveSchedDTOId));
			}

			/**
			 * The DI2S Info
			 */
			DI2SInfo di2sInfo = new DI2SInfo();
			di2sInfo.setUgsId(getUgsId(slaveSchedDTOId));
			di2sInfo.setProgrammingRequestListId(pRListId);
			di2sInfo.setProgrammingRequestId(getPRId(slaveSchedDTOId));
			di2sInfo.setAcquisitionRequestId(getARId(slaveSchedDTOId));
			di2sInfo.setDtoId(getDTOId(slaveSchedDTOId));
			di2sInfo.setTaskId(BigDecimal.ZERO);

			// Set DI2S Info
			brmEquivDTO.setDi2sInfo(parseDMToBRMDI2SInfo(pSId, di2sInfo, 
					brmEquivDTO.getAllDtoInEquivalentDto().get(0).getDtoId(),
					getPRUserInfoList(pSId, getSchedPRId(slaveSchedDTOId))
					.get(0).getOwnerId()));

		}
		else 
		{
			logger.trace("Handle theatre/experimental PRMode.");

			/**
			 * The list of BRM Maneuvers
			 */
			ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Maneuver> brmManList = new ArrayList<>();			

			for (int i = 0; i < brmEquivDTO.getAllDtoInEquivalentDto().size(); i++) {

				brmEquivDTO.getAllDtoInEquivalentDto().get(i).
				setReferredEquivalentDto(brmEquivDTO.getEquivalentDtoId());
			}

			// Set theatre/experimental maneuvers
			if (equivDTO.getTaskList() != null && !equivDTO.getTaskList().isEmpty()) {

				// The maneuver counter
				int i = 0;

				for (Task man : equivDTO.getTaskList()) {

					// Add maneuver
					brmManList.add((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) ObjectMapper
							.parseDMToBRMMan(pSId, man));					
					brmManList.get(i).setPreviousMh(false);

					i ++;
				}

				brmEquivDTO.setManAssociated(brmManList);


			} else {

				logger.trace("No maneuvers are associated to the Equivalent DTO.");
			}
		}

		return brmEquivDTO;
	}

	/**
	 * Parse DM to BRM equivalent DTO List
	 * // Updated on 13/07/2022 to consider only EquivalentDTO with maneuvers
	 * @param pSId
	 * @param equivDTOList
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO> parsePrevBRMEquivDTOList(Long pSId,
			ArrayList<EquivalentDTO> equivDTOList) throws Exception {

		logger.trace("Parse Equivalent DTO list.");

		/**
		 * The Equivalent DTO list
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO> brmEquivDTOList = new ArrayList<com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO>();

		for (EquivalentDTO equivDTO : equivDTOList) {

			if (equivDTO.getTaskList() != null)  {
			
				brmEquivDTOList.add(parsePrevBRMEquivDTO(pSId, equivDTO));
			}
		}

		return brmEquivDTOList;
	}

	/**
	 * Parse DM to BRM equivalent DTO
	 *
	 * @param pSId
	 * @param equivDTO
	 * @return
	 * @throws Exception
	 */
	public static com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO parsePrevBRMEquivDTO(Long pSId,
			EquivalentDTO equivDTO) throws Exception {

		logger.trace("Parse Equivalent DTO.");

		/**
		 * The Equivalent DTO list
		 */
		com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO brmEquivDTO = new com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO();

		/**
		 * The extra pitch
		 */
		double extraPitch = 0;

		/**
		 * The PRMode
		 */
		com.nais.spla.brm.library.main.ontology.enums.PRMode prMode = com.nais.spla.brm.library.main.ontology.enums.PRMode.Standard;

		/**
		 * The satellite Id
		 */
		String satId = "";

		if (PRListProcessor.equivIdSchedARIdMap.get(pSId).containsKey(equivDTO.getEquivalentDtoId())) {

			/**
			 * The scheduling PR Id
			 */
			String schedPRId = ObjectMapper.getSchedPRId(PRListProcessor.equivIdSchedARIdMap
					.get(pSId).get(String.valueOf(equivDTO.getEquivalentDtoId())));

			if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {

				extraPitch = PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getPitchExtraBIC();

				prMode = ObjectMapper.parseDMToBRMPRMode(
						PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getMode());

				satId = PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId)
						.getAcquisitionRequestList().get(0).getDtoList().get(0).getSatelliteId();
			}
		}

		/**
		 * The BRM equivalent DTO
		 */
		brmEquivDTO = new com.nais.spla.brm.library.main.ontology.tasks.EquivalentDTO(
				equivDTO.getEquivalentDtoId(), equivDTO.getStartTime(), equivDTO.getStopTime(), extraPitch);		
		brmEquivDTO.setPrMode(prMode);
		brmEquivDTO.setEquivType(prMode);
		brmEquivDTO.setSatelliteId(satId);

		/**
		 * The list of maneuvers
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.tasks.Maneuver> brmManList = new ArrayList<>();

		if (equivDTO.getTaskList() != null) {

			for (Task man : equivDTO.getTaskList()) {

				// Add maneuver
				brmManList.add((com.nais.spla.brm.library.main.ontology.tasks.Maneuver) ObjectMapper
						.parseDMToBRMMan(pSId, man));								
			}

			brmEquivDTO.setManAssociated(brmManList);	
		}

		brmEquivDTO.setManAssociated(brmManList);

		return brmEquivDTO;
	}

	/**
	 * Get BRM satellite from DM satellite
	 *
	 * @param pSId
	 * @param dmSat
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.resources.Satellite parseDMToBRMSat(Long pSId,
			Satellite dmSat) throws Exception {

		logger.trace("Parse attitude data.");

		String satAtt = "right";

		// TODO: check satellite attitude !!
		if (dmSat.getAttitude() != null) {

			if (!dmSat.getAttitude().isEmpty()) {

				if (dmSat.getAttitude().get(0) != null) {

					if (dmSat.getAttitude().get(0).getValue().doubleValue() > 0.0) {

						satAtt = "left";
					}
				}
			}
		}

		/**
		 * The output satellite
		 */
		com.nais.spla.brm.library.main.ontology.resources.Satellite sat = new com.nais.spla.brm.library.main.ontology.resources.Satellite(
				dmSat.getCatalogSatellite().getSatelliteId(), dmSat.getCatalogSatellite().getSatelliteName(),
				parseDMToBRMPDHT(pSId, dmSat.getPdht(), dmSat.getCatalogSatellite()),
				parseDMToBRMPAWList(dmSat.getPlatformActivityWindowList(),
						dmSat.getCatalogSatellite().getSatelliteId()),
				parseDMToBRMVisibilityList(pSId, dmSat.getVisibilityList(),
						dmSat.getCatalogSatellite().getSatelliteId()), satAtt);

		return sat;
	}

	/**
	 * Get BRM from satellite PDHT 
	 *
	 * @param pSId
	 * @param dmPDHT
	 * @param satId
	 */
	public static PDHT parseDMToBRMPDHT(Long pSId, Pdht dmPDHT, CatalogSatellite catSat) throws Exception {

		logger.trace("Parse satellite PDHT data for satellite: " + catSat.getSatelliteId());

		/**
		 * The output PDHT
		 */
		PDHT pdht = new PDHT();

		if ((dmPDHT.getMm1().get(0) != null) && !dmPDHT.getMm1().isEmpty()) {

			/**
			 * The PDHT List of Memory Modules
			 */
			ArrayList<MemoryModule> mmList = new ArrayList<>();

			/**
			 * The satellite Id
			 */
			String satId = catSat.getSatelliteId();

			/**
			 * The catalog Pdht
			 */
			CatalogPdht catPDHT = catSat.getPdht();

			/**
			 * The free memory value in sectors
			 */
			long freeMemory = 0;

			/**
			 * The memory modules values list in sectors
			 */
			ArrayList<Long> mmValueList = new ArrayList<>();

			MemoryModule mm1 = (new MemoryModule("mm1",
					(long) (dmPDHT.getMm1().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm1.setInitialSectors(catPDHT.getMm1().intValue());
			mmList.add(mm1);
			MemoryModule mm2 = (new MemoryModule("mm2",
					(long) (dmPDHT.getMm2().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm2.setInitialSectors(catPDHT.getMm2().intValue());
			mmList.add(mm2);
			MemoryModule mm3 = (new MemoryModule("mm3",
					(long) (dmPDHT.getMm3().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm3.setInitialSectors(catPDHT.getMm3().intValue());
			mmList.add(mm3);
			MemoryModule mm4 = (new MemoryModule("mm4",
					(long) (dmPDHT.getMm4().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm4.setInitialSectors(catPDHT.getMm4().intValue());
			mmList.add(mm4);
			MemoryModule mm5 = (new MemoryModule("mm5",
					(long) (dmPDHT.getMm5().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm5.setInitialSectors(catPDHT.getMm5().intValue());
			mmList.add(mm5);
			MemoryModule mm6 = (new MemoryModule("mm6",
					(long) (dmPDHT.getMm6().get(0).getValue().doubleValue() / ((4.0) * Math.pow(1024.0, 2)))));
			mm6.setInitialSectors(catPDHT.getMm6().intValue());
			mmList.add(mm6);

			for (int i = 0; i < mmList.size(); i++) {

				freeMemory += mmList.get(i).getFreeSectors();
				mmValueList.add(mmList.get(i).getFreeSectors());
			}

			/**
			 * The output PDHT
			 */
			pdht = new PDHT(satId + "::PDHT", satId + "::PDHT", satId, freeMemory, mmValueList);

			pdht.setMMList(mmList);

		} else {

			logger.warn("No PDHT resources are found in .");
			logger.warn("A default PDHT is built to support the scheduling process.");
			pdht = parseDMToBRMPDHT(pSId, DefaultDebugger.getDefaultPdht(
					SessionActivator.planSessionMap.get(pSId).getMissionHorizonStartTime().getTime()),
					catSat);
		}

		return pdht;
	}

	/**
	 * Get BRM Eclipse from satellite Eclipse
	 *
	 * @param pSId
	 * @param eclipse
	 * @param satId
	 */
	public static com.nais.spla.brm.library.main.ontology.resources.Eclipse parseDMToBRMEclipse(Long pSId,
			Eclipse dmEclipse, String satId) throws Exception {

		logger.trace("Parse eclipse data.");

		/**
		 * The output eclipse
		 */
		com.nais.spla.brm.library.main.ontology.resources.Eclipse eclipse = new com.nais.spla.brm.library.main.ontology.resources.Eclipse(
				dmEclipse.getEclipseStartTime(), dmEclipse.getEclipseStopTime(), satId);

		return eclipse;
	}

	/**
	 * Parse BRM partners from incoming list 
	 * // TODO: set owners data according to pSId
	 * // TODO: manage loan boolean // TODO:
	 * finalize NEO usage conditions // TODO: finalize additional session types //
	 * TODO: finalize transactions for multiple MH (rate > 1) // TODO: manage
	 * mission horizons within same day // TODO: TBD if given transactions from 
	 * are for the working session only! // TODO: TBD for next dev
	 * setCannotGiveCredit
	 *
	 * @param pSId
	 * @param pRType
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static ArrayList<Partner> parseDMToBRMPartners(Long pSId) throws Exception {

		logger.trace("Parse owners BIC data.");

		/**
		 * The list of BRM partners
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.resources.Partner> brmPartnerList = new ArrayList<>();

		try {

			for (int i = 0; i < SessionActivator.partnerListMap.get(pSId).size(); i++) {

				/**
				 * The owner
				 */
				Owner owner = SessionActivator.ownerListMap.get(pSId).get(i);

				logger.debug("Setup available BICs for owner " + owner.getCatalogOwner().getOwnerId());

				/**
				 * The available scheduling BICs
				 */
				double availSchedBIC = SessionActivator.partnerListMap.get(pSId).get(i).getPremBIC();

				/**
				 * The available NEO BICs
				 */
				double availNEOBIC = SessionActivator.partnerListMap.get(pSId).get(i).getNeoBIC();

				/**
				 * The list of transactions
				 */
				List<Transaction> transList = new ArrayList<>();

				if (SessionChecker.isRoutine(pSId)) {

					// TODO: Changed on 16/11/2021 for Unranked RTN BIC management
//					availSchedBIC = SessionActivator.partnerListMap.get(pSId).get(i).getRoutBIC();
					availSchedBIC = SessionActivator.partnerListMap.get(pSId).get(i).getMHRoutBIC();
					
					if (owner.getRoutineAccountChart() != null) {

						transList = owner.getRoutineAccountChart().getTransactions();
					}

				} else {

					if (owner.getPremiumAccountChart() != null) {

						transList = owner.getPremiumAccountChart().getTransactions();
					}
				}

				if (transList == null) {

					transList = new ArrayList<Transaction>();
				}

				/**
				 * The credit card list
				 */
				ArrayList<CreditCard> creditCardList = new ArrayList<>();

				/**
				 * The debit card list
				 */
				ArrayList<DebitCard> debitCardList = new ArrayList<>();

				logger.debug("Import transactions for owner " + owner.getCatalogOwner().getOwnerId());

				for (Transaction trans : transList) {

					if (!trans.getAccountFrom().equals(trans.getAccountTo())) {

						if (trans.getAccountFrom().equals(owner.getCatalogOwner().getOwnerId())) {

							creditCardList.add(new CreditCard(trans.getAccountFrom(), trans.getAmount(), ""));

						} else if (trans.getAccountTo().equals(owner.getCatalogOwner().getOwnerId())) {

							debitCardList.add(new DebitCard(trans.getAccountTo(), -trans.getAmount(), ""));
						}
					}
				}

				/**
				 * The list of AR Ids for Partner
				 */
				List<String> arIdForPartner = new ArrayList<>();

				if (!SessionActivator.ownerARIdMap.get(pSId).isEmpty()) {

					arIdForPartner = (ArrayList<String>) SessionActivator.ownerARIdMap.get(pSId)
							.get(owner.getCatalogOwner().getOwnerId()).clone();
				}

				/**
				 * The BRM Partner
				 */
				com.nais.spla.brm.library.main.ontology.resources.Partner brmPartner = new com.nais.spla.brm.library.main.ontology.resources.Partner(
						owner.getCatalogOwner().getOwnerId(), arIdForPartner, availSchedBIC,
						owner.getCatalogOwner().getPayOffs());
				brmPartner.setMaxNEOBicAvailable(availNEOBIC);
				brmPartner.setGivenLoan(creditCardList);
				brmPartner.setLoanList(debitCardList);

				if (!SessionChecker.isRoutine(pSId)) {

					brmPartner.setBorrowingBic(owner.getCatalogOwner().getPremiumBICBorrowingOwner());
					brmPartner.setMaxPercLoanBic(owner.getCatalogOwner().getPremiumBICLendingPercentage());

				} else {

					brmPartner.setBorrowingBic(owner.getCatalogOwner().getRoutineBICBorrowingOwner());
					brmPartner.setMaxPercLoanBic(owner.getCatalogOwner().getRoutineBICLendingPercentage());
				}

				//				logger.debug("Initial BIC amount available for partner " + owner.getCatalogOwner().getOwnerId() + ": "
				//						+ "Scheduling = " + availSchedBIC + ", NEO = " + availNEOBIC + ".");

				if (SessionActivator.firstSessionMap.get(pSId)) {

					brmPartner.setNewBicSetup(true);
				}

				// Set DLO erosion data
				brmPartner.setMaxPriorityDepthDloErosion(owner.getCatalogOwner().getpMaxriorityDepthDLOErosion());
				brmPartner.setDLOdailyTimeThreshold(owner.getCatalogOwner().getdLOErosionThreshold().intValue());

				brmPartnerList.add(brmPartner);
			}

		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getStackTrace()[0].toString() + ex.getMessage());
		}

		return brmPartnerList;
	}

	/**
	 * Parse BRM from Data Model PAW list // TODO: confirm Id
	 *
	 * @param satPAWList
	 * @param satId
	 * @return
	 */
	public static List<PAW> parseDMToBRMPAWList(List<PlatformActivityWindow> satPAWList, String satId)
			throws Exception {

		logger.trace("Parse PAW data.");

		List<PAW> pawList = new ArrayList<>();

		for (int i = 0; i < satPAWList.size(); i++) {

			PAW paw = new PAW(satPAWList.get(i).getActivityId().intValue(), satId, satPAWList.get(i).getStartTime(),
					satPAWList.get(i).getStopTime());

			paw.setType(parseDMToBRMPAWType(satPAWList.get(i).getActivityType()));

			pawList.add(paw);
		}

		return pawList;
	}

	/**
	 * Parse BRM to DataModel Task Id
	 *
	 * @param ugsId
	 * @param pRId
	 * @param aRId
	 * @param dtoId
	 */
	private static String parseDMToBRMTaskId(String ugsId, String pRId, String aRId, String dtoId) {

		String taskId = ugsId + Configuration.splitChar + pRId + Configuration.splitChar + aRId
				+ Configuration.splitChar + dtoId + Configuration.splitChar;

		return taskId;
	}

	/**
	 * Parse BRM from Data Model Visibility list // TODO: add visibilities GPS slots
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param satVisList
	 *            - the list of satellite visibilities
	 * @param satId
	 *            - the satellite Id
	 * @return the BRM list of visibilities
	 */
	public static List<com.nais.spla.brm.library.main.ontology.resourceData.Visibility> parseDMToBRMVisibilityList(
			Long pSId, List<Visibility> visList, String satId) throws Exception {

		logger.trace("Parse visibility data.");

		/**
		 * The map of visibilities
		 */
		HashMap<String, com.nais.spla.brm.library.main.ontology.resourceData.Visibility> brmVisMap = new HashMap<String, com.nais.spla.brm.library.main.ontology.resourceData.Visibility>();

		if (!visList.isEmpty()) {
			
			Collections.sort(visList, new VisTimeComparator());
		}

		/**
		 * The list of BRM visibilities
		 */
		List<com.nais.spla.brm.library.main.ontology.resourceData.Visibility> brmVisList = new ArrayList<>();

		for (Visibility vis : visList) {

			if (vis.isAllocated() && vis.isXbandFlag()) {

				// Get visibilities by owner
				setOwnerVisibilities(pSId, vis, satId, brmVisMap);
			}

		}

		for (String key: brmVisMap.keySet()) {

			brmVisList.add(brmVisMap.get(key));

			logger.debug("Computed BRM visibility: " +  brmVisMap.get(key));
		}

		return brmVisList;
	}

	/**
	 * Parse BRM from Data Model Visibility
	 *
	 * @param pSId
	 *            - the Planning Session Id
	 * @param vis
	 *            - the satellite visibility
	 * @param satId
	 *            - the satellite Id
	 * @return the BRM list of visibilities
	 */
	private static void setOwnerVisibilities(Long pSId, Visibility vis, String satId, 
			HashMap<String, com.nais.spla.brm.library.main.ontology.resourceData.Visibility> brmVisMap) throws Exception {

		logger.trace("Parse acquisition stations data for satellite: " + satId);

		/**
		 * The owner Id
		 */
		String ownerId = null;

		/**
		 * The map of owner acquisition stations list
		 */
		Iterator<Map.Entry<String, ArrayList<AcquisitionStation>>> it = SessionActivator.ownerAcqStationListMap
				.get(pSId).entrySet().iterator();

		while (it.hasNext()) {

			/**
			 * The acquisition station entry
			 */
			Map.Entry<String, ArrayList<AcquisitionStation>> acqStationListMap = it.next();

			for (AcquisitionStation acqStation : acqStationListMap.getValue()) {

				if (acqStation.getCatalogAcquisitionStation() != null) {

					if (acqStation.getCatalogAcquisitionStation().getAcquisitionStationId()
							.equals(vis.getAcquisitionStationId())) {

						ownerId = acqStationListMap.getKey();

						logger.trace("Found owner " + ownerId + " associated to visibility "
								+ " from " + vis.getVisibilityStartTime() + " to " + vis.getVisibilityStopTime() 
								+ " of the acquisition station " + vis.getAcquisitionStationId() 
								+ " for contact counter " + vis.getContactCounter());

						if (acqStation.getCatalogAcquisitionStation().isExternalStationFlag()) {

							ownerId = null;
						}

						/**
						 * The univocal visibility Id
						 */
						String uniVisId = vis.getContactCounter() + Configuration.splitChar 
								+ satId + Configuration.splitChar 
								+ vis.getAcquisitionStationId() + Configuration.splitChar;

						/**
						 * The attitude map
						 */
						if (!brmVisMap.containsKey(uniVisId)) {

							logger.trace("Add new visibility for owner " + ownerId + " associated to visibility "
									+ " from " + vis.getVisibilityStartTime() + " to " + vis.getVisibilityStopTime() 
									+ " of the acquisition station " + vis.getAcquisitionStationId() 
									+ " for contact counter " + vis.getContactCounter()
									+ " for satellite: " + satId);

							/**
							 * The BRM visibility
							 */
							com.nais.spla.brm.library.main.ontology.resourceData.Visibility brmVis = new com.nais.spla.brm.library.main.ontology.resourceData.Visibility(
									vis.getContactCounter(), satId, vis.getAcquisitionStationId(), ownerId,
									vis.getVisibilityStartTime(), vis.getVisibilityStopTime());

							brmVisMap.put(uniVisId, brmVis);

						}

						/**
						 * The map of BRM attitudes
						 */
						HashMap<Boolean, Attitude> brmAttMap = getAttitudeMap(vis);

						for (boolean key : brmAttMap.keySet()) {

							logger.debug("Set Attitude Map for attitude (right=true, left=false): " + key
									+ " of the acquisition station " + vis.getAcquisitionStationId() 
									+ " for contact counter " + vis.getContactCounter()
									+ " for satellite: " + satId);

							brmVisMap.get(uniVisId).getAttitudes().put(key, brmAttMap.get(key));
						}

						// External flag
						if (acqStation.getCatalogAcquisitionStation().isExternalStationFlag().booleanValue()) {

							brmVisMap.get(uniVisId).setExternal(true);
						}

						break;
					}
				}
			}
		}

		//		for (String key : brmVisMap.keySet()) {
		//			
		//			logger.debug("Add univoque visibility " + key + " having attitudes: " 
		//			+ brmVisMap.get(key).getAttitudes().keySet().toString() + " and  external flag " 
		//					+ brmVisMap.get(key).isExternal());  
		//			
		//			brmVisList.add(brmVisMap.get(key));
		//
		//		}
		//
		//		return brmVisMap;
	}

	/**
	 * Get the BRM attitude map
	 * @param vis - the input visibility
	 * @return
	 */
	private static HashMap<Boolean, Attitude> getAttitudeMap(Visibility vis) {

		/**
		 * The BRM list of pitch intervals
		 */
		ArrayList<com.nais.spla.brm.library.main.ontology.utils.PitchIntervals> pitchIntList = new ArrayList<>();

		for (ElevationMask elMask : vis.getElevationMaskList()) {

			pitchIntList.add(new com.nais.spla.brm.library.main.ontology.utils.PitchIntervals(
					elMask.getStartTime(), elMask.getStopTime(), elMask.getElevationAngle()));
		}

		/**
		 * The visibility attitude map
		 */
		HashMap<Boolean, Attitude> attMap = new HashMap<Boolean, Attitude>();

		logger.debug("Visibility: " + vis.getAcquisitionStationId() + " for the contact counter " + vis.getContactCounter() 
		+ " has look side: " + vis.getLookSide().toString() + " with Pitch Intervals size: " + pitchIntList.size());

		// true for left attitude, false for right attitude
		if (vis.getLookSide().equals(LookSide.Both)) {

			attMap.put(true, new Attitude(vis.getVisibilityStartTime(), vis.getVisibilityStopTime(), true, pitchIntList));
			attMap.put(false, new Attitude(vis.getVisibilityStartTime(), vis.getVisibilityStopTime(), false, pitchIntList));

		} else if (vis.getLookSide().equals(LookSide.Left)) {

			attMap.put(false, new Attitude(vis.getVisibilityStartTime(), vis.getVisibilityStopTime(), false, pitchIntList));

		} else {

			attMap.put(true, new Attitude(vis.getVisibilityStartTime(), vis.getVisibilityStopTime(), true, pitchIntList));
		}

		return attMap;
	}

	/**
	 * Get BRM from Data Model satellite state list
	 * data
	 */
	public static List<SatelliteState> parseDMToBRMStateList(List<ResourceStatus> satResList, 
			List<Satellite> satList)
					throws Exception {

		logger.trace("Parse satellite status data.");

		List<SatelliteState> statusList = new ArrayList<>();

		for (int i = 0; i < satList.size(); i++) {

			for (int j = 0; j < satResList.size(); j++) {

				/**
				 * The reference dates
				 */
				Date unavStartTime = satResList.get(j).getReferenceStartTime();
				Date unavStopTime = satResList.get(j).getReferenceStopTime();

				if (unavStopTime == null) {

					unavStopTime = new Date((long) Double.POSITIVE_INFINITY);
				}

				SatelliteState status = new SatelliteState(satList.get(i).getCatalogSatellite().getSatelliteId(),
						unavStartTime, unavStopTime);

				statusList.add(status);
			}
		}

		return statusList;
	}

	/**
	 * Parse Data Model PDHT from BRM PDHT // TODO: check PDHT config
	 *
	 * @param brmPdht
	 * @param refDate
	 */
	@SuppressWarnings("unchecked")
	public static Pdht parseBRMToDMPDHT(PDHT brmPdht, Date refDate) throws Exception {

		logger.trace("Reparse satellite PDHT data.");

		/**
		 * New Data Model PDHT
		 */
		Pdht dmPdht = new Pdht();

		ArrayList<ResourceValue> resValList = new ArrayList<>();

		// Add resource values to each pdht MM // TODO: get PDHT functions
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(0).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm1((List<ResourceValue>) resValList.clone());
		resValList.clear();
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(1).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm2((List<ResourceValue>) resValList.clone());
		resValList.clear();
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(2).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm3((List<ResourceValue>) resValList.clone());
		resValList.clear();
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(3).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm4((List<ResourceValue>) resValList.clone());
		resValList.clear();
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(4).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm5((List<ResourceValue>) resValList.clone());
		resValList.clear();
		resValList.add(new ResourceValue(refDate,
				BigDecimal.valueOf((brmPdht.getMMList().get(5).getFreeSectors() * 4.0 * Math.pow(1024.0, 2)))));
		dmPdht.setMm6((List<ResourceValue>) resValList.clone());
		resValList.clear();

		return dmPdht;
	}

	/**
	 * Parse Data Model PDHT from BRM PDHT // TODO: check PDHT config
	 *
	 * @param brmAtt
	 */
	public static BigDecimal parseBRMToDMAttitude(boolean rightToLeft) throws Exception {

		BigDecimal satAtt = BigDecimal.valueOf(0.0);

		if (rightToLeft == true) {

			satAtt = BigDecimal.valueOf(1.0);
		}

		return satAtt;
	}

	/**
	 * Get data of packet associated to the store
	 *
	 * @param pRId
	 * @param aRId
	 * @param dtoId
	 * @param storeList
	 * @return
	 */
	private static ArrayList<PacketStore> getPacketData(String ugsId, String pRId, String aRId, String dtoId,
			ArrayList<Store> storeList) {

		/**
		 * The list of stored Ids
		 */
		ArrayList<PacketStore> packetList = new ArrayList<>();

		for (Store store : storeList) {

			if (ugsId.equals(store.getUgsId()) && pRId.equals(store.getProgrammingRequestId()) 
					&& aRId.equals(store.getAcquisitionRequestId()) && dtoId.equals(store.getDtoId())) {

				if (store.getPacketStoreIdH() != null) {
					/**
					 * The packet store H
					 */
					PacketStore packetStoreH = new PacketStore(store.getPacketStoreIdH().toString());

					packetStoreH.setPolarization(Polarization.HH);

					/**
					 * The planned On MM map
					 */
					HashMap<MemoryModule, Long> plannedOnMemModule = new HashMap<>();

					if (store.getMemoryModules() != null && ! store.getMemoryModules().isEmpty()) {

						if (store.getMemoryModules().size() != 6) {

							logger.warn("Incoherent MMs size for Packet Store of DTO: " 
									+ ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId));
						} else {
							logger.info("MMs for Packet Store of DTO: " 
									+ ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId) 
									+ " " + store.getMemoryModules().toString()); 						
						}

						for (int i = 0; i < store.getMemoryModules().size(); i++) {

							/**
							 * The MM
							 */	
							MemoryModule mm = new MemoryModule("mm" + (i + 1), store.getMemoryModules().get(i).longValue());

							plannedOnMemModule.put(mm, store.getMemoryModules().get(i).longValue());

							packetStoreH.setPlannedOnMemModule(plannedOnMemModule);
						}
					}

					// Add packet Store H
					packetList.add(packetStoreH);
				}

				if (store.getPacketStoreIdV() != null) {
					/**
					 * The packet store V
					 */
					PacketStore packetStoreV = new PacketStore(store.getPacketStoreIdV().toString());

					packetStoreV.setPolarization(Polarization.VV);

					/**
					 * The planned On MM map
					 */
					HashMap<MemoryModule, Long> plannedOnMemModule = new HashMap<>();

					if (store.getMemoryModules() != null && ! store.getMemoryModules().isEmpty()) {

						if (store.getMemoryModules().size() != 6) {

							logger.warn("Incoherent MMs size for Packet Store of DTO: " 
									+ ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId));

						} else {

							logger.info("MMs for Packet Store of DTO: " 
									+ ObjectMapper.parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId) 
									+ " " + store.getMemoryModules().toString()); 
						}

						for (int i = 0; i < store.getMemoryModules().size(); i++) {

							/**
							 * The MM
							 */	
							MemoryModule mm = new MemoryModule("mm" + (i + 1), store.getMemoryModules().get(i).longValue());

							plannedOnMemModule.put(mm, store.getMemoryModules().get(i).longValue());

							packetStoreV.setPlannedOnMemModule(plannedOnMemModule);
						}
					}
					// Add packet Store V
					packetList.add(packetStoreV);
				}
			}
		}

		if (packetList.isEmpty()) {

			logger.warn("No store Ids related to Download of DTO Id: " 
					+ parseDMToSchedDTOId(ugsId, pRId, aRId, dtoId) + " found.");

		} else if (packetList.size() > 1) {

			/**
			 * The planned On MM map
			 */
			HashMap<MemoryModule, Long> plannedOnMemModule = new HashMap<>();

			for (int i = 0; i < 6; i ++) {

				/**
				 * The MM
				 */	
				MemoryModule mm = new MemoryModule("mm" + (i + 1));

				plannedOnMemModule.put(mm, 0L);

				packetList.get(1).setPlannedOnMemModule(plannedOnMemModule);
			}
		}

		return packetList;
	}

	/**
	 * Get the the UserInfo (owner, subscribers) relevant to the given
	 * PR Id for the working Planning Session
	 *  
	 * @param pSId
	 * @param schedPRId
	 * @param ugsOwnerList
	 * @return the list of owner and subscribers
	 */
	public static List<UserInfo> getPRUserInfoList(Long pSId, String schedPRId) {

		/**
		 * The output list of user info
		 */
		List<UserInfo> userInfoList = new ArrayList<>();

		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {

			userInfoList = PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getUserList();

		} else {

			logger.warn("No user info relevant to PR " + schedPRId + " are found!");
		}

		return userInfoList;
	}

	/**
	 * Get the the UserInfo (owner, subscribers) relevant to the given
	 * acquisition Task for the working Planning Session
	 *  
	 * @param pSId
	 * @param schedPRId
	 * @param ugsOwnerList
	 * @return the list of owner and subscribers
	 */
	public static List<UserInfo> getAcqUserInfoList(Long pSId, String schedPRId, List<UgsOwner> ugsOwnerList) {

		/**
		 * The output list of user info
		 */
		List<UserInfo> userInfoList = new ArrayList<>();

		if (PRListProcessor.pRSchedIdMap.get(pSId).containsKey(schedPRId)) {

			userInfoList = PRListProcessor.pRSchedIdMap.get(pSId).get(schedPRId).getUserList();

		} 

		if (ugsOwnerList != null) {

			for (UgsOwner ugsOwner : ugsOwnerList) {

				userInfoList.add(new UserInfo(ugsOwner.getOwnerId(), ugsOwner.getUgsId(), true));
			}
		}

		return userInfoList;
	}

	/**
	 * Parse the scheduling to BRM UserInfo
	 *
	 * @param userInfoList
	 */
	private static List<com.nais.spla.brm.library.main.ontology.resourceData.UserInfo> parseDMToBRMUserInfo(
			List<UserInfo> userInfoList) {

		/**
		 * The list of UserInfo 
		 */
		List<com.nais.spla.brm.library.main.ontology.resourceData.UserInfo> brmUserInfoList = new ArrayList<>();

		for (UserInfo userInfo : userInfoList) {

			brmUserInfoList
			.add(new com.nais.spla.brm.library.main.ontology.resourceData.UserInfo(userInfo.getAcquisitionStationIdList(),
					userInfo.isSubscriber(), userInfo.getOwnerId(), userInfo.getUgsId()));
		}

		return brmUserInfoList;
	}

	/**
	 * Parse the scheduling to BRM UserInfo
	 *
	 * @param userInfoList
	 */
	public static List<UgsOwner> parseBRMToDMUserInfo(
			List<com.nais.spla.brm.library.main.ontology.resourceData.UserInfo> brmUserInfoList) {

		/**
		 * The list of UserInfo 
		 */
		List<UgsOwner> ugsOwnerList = new ArrayList<UgsOwner>();

		for (int i = 0; i < brmUserInfoList.size(); i++) {

			com.nais.spla.brm.library.main.ontology.resourceData.UserInfo brmUserInfo = brmUserInfoList.get(i);

			if (i > 1) {

				ugsOwnerList.add(new UgsOwner(brmUserInfo.getUgsId(), brmUserInfo.getOwnerId()));
			}
		}

		return ugsOwnerList;
	}

	/**
	 * Parse the scheduling to BRM UgsOwner list
	 *
	 * @param userInfoList
	 */
	private static ArrayList<String> parseDMToBRMUgsOwnerList(List<UgsOwner> ugsOwnerList) {

		/**
		 * The list of ugs Owner Ids
		 */
		ArrayList<String> brmUgsOwnerList = new ArrayList<>();

		for (UgsOwner userInfo : ugsOwnerList) {

			brmUgsOwnerList.add(userInfo.getOwnerId());
		}

		return brmUgsOwnerList;
	}

	/**
	 * Parse the DataModel to BRM PAWType
	 *
	 * @param pawType
	 * @return
	 */
	public static com.nais.spla.brm.library.main.ontology.enums.PAWType parseDMToBRMPAWType(PAWType pawType) {

		switch (pawType) {
		case CAL:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.CAL;
		case CMS:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.CMS;
		case GENERIC:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.GENERIC;
		case KCR:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.KCR;
		case MAN:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.MAN;
		case STTOCCULTATION:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.STTOCCULTATION;
		case SWM:
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.SWM;
		default:
			logger.warn("A default PAW type is parsed.");
			return com.nais.spla.brm.library.main.ontology.enums.PAWType.GENERIC;
		}
	}

	/**
	 *
	 * @param pRMode
	 */
	private static com.nais.spla.brm.library.main.ontology.enums.PRMode parseDMToBRMPRMode(PRMode pRMode) {

		switch (pRMode) {

		case Standard:
			return com.nais.spla.brm.library.main.ontology.enums.PRMode.Standard;
		case Theatre:
			return com.nais.spla.brm.library.main.ontology.enums.PRMode.Theatre;
		case DI2S:
			return com.nais.spla.brm.library.main.ontology.enums.PRMode.DI2S;
		case Experimental:
			return com.nais.spla.brm.library.main.ontology.enums.PRMode.Exp;
		default:
			logger.warn("A default PR Mode is parsed.");
			return com.nais.spla.brm.library.main.ontology.enums.PRMode.Standard;
		}
	}

	/**
	 * Parse from BRM to DM Di2sInfo
	 * 
	 * @param pSId
	 * @param brmDi2sInfo
	 * @param taskId
	 * @param masterSchedDTOId
	 * @return
	 */
	public static DI2SInfo parseBRMToDMDi2sInfo(Long pSId,
			com.nais.spla.brm.library.main.ontology.resourceData.Di2sInfo brmDi2sInfo, Integer taskId,
			String masterSchedDTOId) {

		/**
		 * The Di2SInfo
		 */
		DI2SInfo di2sInfo = null;

		// The ids
		String[] ids = null;

		if (brmDi2sInfo != null) {

			logger.trace("Parse DI2S Info for Master scheduling DTO: " + masterSchedDTOId);			

			ids = brmDi2sInfo.getRelativeSlaveId().split(Configuration.splitChar);

			logger.trace("Parse DI2S Info for Slave scheduling DTO: " + brmDi2sInfo.getRelativeSlaveId());

			EquivDTOHandler.di2sLinkedIdsMap.get(pSId).put(masterSchedDTOId, brmDi2sInfo.getRelativeSlaveId());

		} else {

			logger.trace("No DI2S Info found from BRM for scheduling DTO: " + masterSchedDTOId);

			// TODO: String[] ids = EquivDTOHandler.IdsMap(); only if ugs is the same ?
			if (EquivDTOHandler.di2sLinkedIdsMap.get(pSId).containsKey(masterSchedDTOId)) {

				ids = EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(masterSchedDTOId)
						.split(Configuration.splitChar);
			}
		}

		if (EquivDTOHandler.di2sLinkedIdsMap.get(pSId).containsKey(masterSchedDTOId) 
				&& PRListProcessor.pRToPRListIdMap.get(pSId).containsKey(getSchedPRId(
						EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(masterSchedDTOId)))) {

			logger.trace("Set DI2S Info for Master scheduling DTO: " + masterSchedDTOId +
					" with Slave scheduling DTO: " + EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(masterSchedDTOId));

			/**
			 * The DI2SInfo
			 */
			di2sInfo = new DI2SInfo();			
			di2sInfo.setAcquisitionRequestId(ids[2]);
			di2sInfo.setDtoId(ids[3]);
			di2sInfo.setProgrammingRequestId(ids[1]);
			di2sInfo.setProgrammingRequestListId(PRListProcessor.pRToPRListIdMap.get(pSId).get(
					getSchedPRId(EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(masterSchedDTOId))).get(0));
			di2sInfo.setTaskId(BigDecimal.valueOf(taskId));
			di2sInfo.setUgsId(ids[0]);

			logger.trace("Slave scheduling DTO is inserted: " 
					+ EquivDTOHandler.di2sLinkedIdsMap.get(pSId).get(masterSchedDTOId));			

		} else {

			logger.trace(" No associated PR to scheduling DTO " + masterSchedDTOId + " found. ");
		}

		return di2sInfo;
	}	

	/**
	 * Compute download polarization total parts relevant to a reference acquisition 
	 * // Changed on 07/09/2022 to manage multi-Partner downloads for DI2S requests
	 * @param pSId
	 * @param schedDTOId
	 * @param polar
	 * @return
	 */
	public static HashMap<String, Integer> computeDwlTotalParts(Long pSId, String schedDTOId, Polarization polar) {

		/**
		 * The Packet Store total parts number
		 */
		HashMap<String, Integer> totalPartsMap = new HashMap<>();

		try {

			/**
			 * The list of downloads associated to DTO (no GPS)
			 */
			if (! schedDTOId.contains("null")) {

				/**
				 * The list of Downloads
				 */
				for (Task dmTask : PersistPerformer.refTaskListMap.get(pSId)) {

					if (dmTask.getTaskType().equals(TaskType.DWL)) {

						if (((Download) dmTask).getProgrammingRequestId() != null
								&& schedDTOId.equals(ObjectMapper.parseDMToSchedDTOId(dmTask.getUgsId(), 
										dmTask.getProgrammingRequestId(), dmTask.getProgrammingRequestId(), 
										dmTask.getDtoId()))) {

							// The dwl polarization
							Polarization dwlPolar = Polarization.HH;

							if (((Download) dmTask).getSourcePacketNumberH() == null 
									|| ((Download) dmTask).getSourcePacketNumberH().equals(BigDecimal.ZERO)) {	
								
								dwlPolar = Polarization.VV;							
							}

							if (polar.equals(dwlPolar)) {

								logger.debug("Set Packet Store for DTO: " + schedDTOId + " in total parts " 
										+ ((Download) dmTask).getPacketStoreTotalParts().intValue()
										+ " relevant to Partner " + SessionActivator.ugsOwnerIdMap.get(pSId).get(dmTask.getUgsId()));
								
								totalPartsMap.put(SessionActivator.ugsOwnerIdMap.get(pSId).get(dmTask.getUgsId()),
										((Download) dmTask).getPacketStoreTotalParts().intValue());
							}
						}
					}
				}
			}


		} catch (Exception ex) {

			logger.error("Exception raised: " + ex.getMessage());
		}

		return totalPartsMap;
	}
}

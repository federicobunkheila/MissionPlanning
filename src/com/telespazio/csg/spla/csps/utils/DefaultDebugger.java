package com.telespazio.csg.spla.csps.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.telespazio.csg.spla.csps.core.server.Environment;
import com.telespazio.csg.spla.csps.model.impl.Partner;

import it.sistematica.spla.datamodel.core.enums.Category;
import it.sistematica.spla.datamodel.core.enums.LookSide;
import it.sistematica.spla.datamodel.core.enums.PlanningSessionType;
import it.sistematica.spla.datamodel.core.exception.InputException;
import it.sistematica.spla.datamodel.core.model.PlanningSession;
import it.sistematica.spla.datamodel.core.model.resource.AcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.CMGA;
import it.sistematica.spla.datamodel.core.model.resource.Owner;
import it.sistematica.spla.datamodel.core.model.resource.Pdht;
import it.sistematica.spla.datamodel.core.model.resource.Sar;
import it.sistematica.spla.datamodel.core.model.resource.Satellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogAcquisitionStation;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogCMGA;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogHPLimitation;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogOwner;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogPdht;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogSar;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogSatellite;
import it.sistematica.spla.datamodel.core.model.resource.catalog.CatalogUgs;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.AccountChart;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ElevationMask;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.PlatformActivityWindow;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceStatus;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.ResourceValue;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Transaction;
import it.sistematica.spla.datamodel.core.model.resource.resourceData.Visibility;

public class DefaultDebugger {

    static String[] pawsFilenames =
    {
            // Environment.SPLA_HOME + "//CSPS//DATA//FILES//stub-files"
            // + "//PAW_01-Generic_early.xml",
            Environment.SPLA_HOME + "//CSPS//DATA//FILES//stub-files" + "//PAW_02-Generic_final.xml",
            // Environment.SPLA_HOME + "//CSPS//DATA//FILES//stub-files"
            // + "//PAW_03-Occultation_early.xml",
            Environment.SPLA_HOME + "//CSPS//DATA//FILES//stub-files" + "//PAW_04-Occultation_final.xml",
            // Environment.SPLA_HOME + "//CSPS//DATA//FILES//stub-files"
            // + "//PAW_05-OccultationAndGeneric.xml",
    };
	
	/**
	 * Get default Acquisition Stations
	 *
	 * @throws IOException
	 * @throws InvalidProtocolBufferException
	 * @throws XMLException
	 */
	public static ArrayList<AcquisitionStation> getDefaultAcqStationList(Long sessionId)
	        throws InvalidProtocolBufferException, IOException, Exception
	{
		/**
		 * The list of acquisition stations
		 */	
	    ArrayList<AcquisitionStation> acqStationList = new ArrayList<>();
	
		/**
		 * The list of acquisition station catalogs
		 */	
	    ArrayList<CatalogAcquisitionStation> catAcqStationList = new ArrayList<CatalogAcquisitionStation>();
	    catAcqStationList.add(new CatalogAcquisitionStation("IDACQ", "IDACQ", false));        
	    catAcqStationList.add(new CatalogAcquisitionStation("ICACQ", "ICACQ", false));
	
	    for (CatalogAcquisitionStation catAcqStation: catAcqStationList) {
	
	    	AcquisitionStation acqSt = new AcquisitionStation(catAcqStation, new ArrayList<ResourceStatus>());
	    	
	    	acqStationList.add(acqSt);
	    }       
	    
	    return acqStationList;
	}

	public static CMGA getDefaultCMGAs()
	{
		/**
		 * The CMGA
		 */			
	    CMGA cmga = new CMGA(new CatalogCMGA("CMGA1", true, 180.0, 180.0,25.0, 25.0, 15.0),
	            new CatalogCMGA("CMGA2", true, 180.0, 180.0,25.0, 25.0, 15.0),
	            new CatalogCMGA("CMGA3", true, 180.0, 180.0,25.0, 25.0, 15.0));
	
	    return cmga;
	}

	/**
	 *
	 * @param pS
	 * @return
	 */
	public static List<Owner> getDefaultOwners(PlanningSession pS)
	{
		/**
		 * The list of ugs catalogs
		 */		
	    ArrayList<CatalogUgs> ugsList = new ArrayList<>();
		/**
		 * The list of NEO BICs
		 */		    
		List<ResourceValue> neoBICList = new ArrayList<>();
	    neoBICList.add(
	            new ResourceValue(pS.getMissionHorizonStartTime(), BigDecimal.valueOf(99999.0)));
	    /**
		 * The list of premium BICs
		 */	
		List<ResourceValue> premiumBICList = new ArrayList<>();
	    premiumBICList.add(
	            new ResourceValue(pS.getMissionHorizonStartTime(), BigDecimal.valueOf(30000.0)));
		/**
		 * The list of routine BICs
		 */		    
		List<ResourceValue> routineBICList = new ArrayList<>();
	    routineBICList.add(
	            new ResourceValue(pS.getMissionHorizonStartTime(), BigDecimal.valueOf(70000.0)));
		/**
		 * The account chart
		 */		
	    AccountChart accChart = new AccountChart();
	
		/**
		 * The list of acquisition station catalogs
		 */		    
		ArrayList<CatalogAcquisitionStation> catAcqStation = new ArrayList<CatalogAcquisitionStation>();
	    catAcqStation.add(new CatalogAcquisitionStation("IDACQ", "IDACQ", true));        
	    catAcqStation.add(new CatalogAcquisitionStation("ICACQ", "ICACQ", true));
	    
		/**
		 * The list of ugs catalogs
		 */		    
		 CatalogUgs[] catUgs =
	    { 		new CatalogUgs("100", "100", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("110", "110", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("120", "120", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("130", "130", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("140", "140", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("150", "150", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("200", "200", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("210", "210", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("220", "220", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("230", "230", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("240", "240", true, null, catAcqStation, false, catAcqStation, false, false),
	            new CatalogUgs("250", "250", true, null, catAcqStation, false, catAcqStation, false, false)
	            };
	
	    ugsList.addAll(Arrays.asList(catUgs));
	
		/**
		 * The owner catalogs
		 */		    
	    CatalogOwner[] catOwners =
	    { 		new CatalogOwner("1000", Category.Defence, 30000.0, 70000.0, 99999.0, false, true, 
	    				2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("1100", Category.Defence, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("1200", Category.Defence, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("1300", Category.Defence, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("1400", Category.Defence, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("1500", Category.Defence, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2000", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2100", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2200", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2300", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2400", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList),
	            new CatalogOwner("2500", Category.Civilian, 30000.0, 70000.0, 99999.0, false, true,
	                    2, new ArrayList<String>(), 30.0, new ArrayList<String>(), 30.0, 30.0, 100, ugsList) 
	            };
	
		/**
		 * The default owners
		 */	
	    Owner[] defOwners =
	    { new Owner(catOwners[0], neoBICList, accChart, accChart, premiumBICList, routineBICList),
	            new Owner(catOwners[1], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[2], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[3], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[4], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[5], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[6], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[7], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[8], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[9], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[10], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList),
	            new Owner(catOwners[11], neoBICList, accChart, accChart, premiumBICList,
	                    routineBICList) };
	
	    // Update lists
	    return Arrays.asList(defOwners);
	}

	/**
	 * @param pSId
	 * @param ownerList
	 * @return
	 */
	public static ArrayList<Partner> getDefaultPartners(Long pSId, List<Owner> ownerList)
	{
		/**
		 * The list of partners
		 */		
	    ArrayList<Partner> partnerList = new ArrayList<>();
	
	    for (int i = 0; i < ownerList.size(); i++)
	    {
			/**
			 * The available Premium BICs
			 */		
	        double premiumAvailBIC = ownerList.get(i).getCatalogOwner().getPremiumBIC();
	        
			/**
			 * The available Routine BICs
			 */		
			double routineAvailBIC = ownerList.get(i).getCatalogOwner().getRoutineBIC();
	
	        // Check Premium Transactions
	        for (Transaction trans : ownerList.get(i).getPremiumAccountChart().getTransactions())
	        {
	
	            if (trans.getTransactionDescription().equals("premiumAccountChart")
	                    && (trans.getAccountFrom() == trans.getAccountTo()))
	            {
					// Update Premium BIC
	                premiumAvailBIC -= trans.getAmount();
	            }
	        }	        
			// Check Routine Transactions
	        for (Transaction trans : ownerList.get(i).getRoutineAccountChart().getTransactions())
	        {
	
	            if (trans.getTransactionDescription().equals("routineAccountChart")
	                    && (trans.getAccountFrom() == trans.getAccountTo()))
	            {
					// Update Routine BIC
	                routineAvailBIC -= trans.getAmount();
	            }
	        }
	
	        /**
	         * The map of the consumed BIC resource
	         */
	        HashMap<PlanningSessionType, Double> pSBICMap = new HashMap<>();
	
	        pSBICMap.put(PlanningSessionType.AllPartnersCheckConflict, premiumAvailBIC); // TODO:
	                                                                                     // TBC
	        pSBICMap.put(PlanningSessionType.AllPartnersLimitedCheckConflict, 0.0);
	        pSBICMap.put(PlanningSessionType.PartnerOnlyCheckConflict, 0.0);
	        pSBICMap.put(PlanningSessionType.InterCategoryRankedRoutine, routineAvailBIC); // TODO:
	                                                                                       // TBC
	        pSBICMap.put(PlanningSessionType.IntraCategoryRankedRoutine, 0.0);
	        pSBICMap.put(PlanningSessionType.Negotiation, 0.0);
	        pSBICMap.put(PlanningSessionType.UnrankedRoutine, routineAvailBIC); // TODO:
	                                                                            // TBC
	        pSBICMap.put(PlanningSessionType.Poll, 0.0);
	        pSBICMap.put(PlanningSessionType.LastMinutePlanning, 0.0);
	        pSBICMap.put(PlanningSessionType.VeryUrgent, 0.0);
	
		    /**
	         * The default Partner
	         */
	        Partner partner = new Partner(ownerList.get(i).getCatalogOwner().getOwnerId(),
	                ownerList.get(i).getCatalogOwner().getUgsList().get(0).getUgsId(),
	                premiumAvailBIC, routineAvailBIC, ownerList.get(i).getCatalogOwner().getNeoBIC(), 
	                premiumAvailBIC, routineAvailBIC, ownerList.get(i).getCatalogOwner().getNeoBIC(), false);
			
			// Add partner
	        partnerList.add(partner);
	    }
	
	    return partnerList;
	}

	/**
	 *
	 * @param refTime
	 * @return
	 */
	public static Pdht getDefaultPdht(long refTime)
	{
		/**
		 * The default PDHT
		 */	
	    Pdht pdht = new Pdht();
	
		/**
		 * The list of PDHT resources 
		 */	
	    List<ResourceValue> resPdhtList = new ArrayList<>();
	    resPdhtList.add(
	            new ResourceValue(new Date(refTime), BigDecimal.valueOf(192000000000.0 / 6.0)));
	    pdht.setMm1(resPdhtList);
	    pdht.setMm2(resPdhtList);
	    pdht.setMm3(resPdhtList);
	    pdht.setMm4(resPdhtList);
	    pdht.setMm5(resPdhtList);
	    pdht.setMm6(resPdhtList);
	
	    return pdht;
	}

	/**
	 *
	 * @param pS
	 * @param refTime
	 * @param satId
	 * @return
	 * @throws InputException
	 * @throws IOException
	 * @throws XMLException
	 * @throws InvalidProtocolBufferException
	 */
	public static Satellite getDefaultSatellite(PlanningSession pS, long refTime, String satId)
	        throws InputException, InvalidProtocolBufferException, Exception, IOException
	{
	
		/**
		 * The default Satellite
		 */	
	    Satellite sat = new Satellite(new CatalogSatellite(satId, satId, false, 
	    		new CatalogHPLimitation("", new ArrayList<>()), BigDecimal.ZERO, 
	    		BigDecimal.ZERO, new CatalogSar(satId, satId, null), 
	    		new CatalogPdht(satId, satId, null, null, null, null, null, null, null, null)));
	    sat.setPdht(getDefaultPdht(refTime));
	
		/**
		 * The list of attitude resources
		 */	
	    List<ResourceValue> resAttList = new ArrayList<>();
	    resAttList.add(new ResourceValue(new Date(refTime), BigDecimal.valueOf(0.0)));
	    sat.setAttitude(resAttList);
	    List<PlatformActivityWindow> pawList = new ArrayList<>();
	
//	    // Upload test PAWs
//	    pawList = uploadPAWs(pS.getPlanningSessionId());
	
	    sat.setPlatformActivityWindowList(pawList);
		
		/**
		 * The list of visibilities
		 */			
	    List<Visibility> visList = new ArrayList<>();
		
	    ElevationMask elMask = new ElevationMask(0.0, new Date(), new Date());
	    
	    ArrayList<ElevationMask> elMaskList = new ArrayList<ElevationMask>();
	    
	    elMaskList.add(elMask);
	    
		/**
		 * The default visibility
		 */	
	    Visibility vis = new Visibility(LookSide.Right, 0L, "1100",
	            new Date((long) ((refTime - 86400000.0 - 3600000.0))),
	            new Date((long) (refTime - 86400000.0  + 3600000.0)), true, false, elMaskList);
	    visList.add(vis);
	    sat.setVisibilityList(visList);
		
		/**
		 * The default SAR
		 */	
	    Sar sar = new Sar();
	    sat.setSar(sar);
	
	    sat.setCmga(getDefaultCMGAs());
	
	    return sat;
	}
	
//	/**
//     * Upload PAWs from file
//     *
//     * @throws IOException
//     * @throws InvalidProtocolBufferException
//     * @throws XMLException
//     */
//    public static ArrayList<PlatformActivityWindow> uploadPAWs(Long sessionId)
//            throws InvalidProtocolBufferException, IOException, Exception
//    {
//
//		/**
//		 * The PAW Reader
//		 */		
//        PlatformActivityWindowReader pawReader = new PlatformActivityWindowReader(sessionId);
//
//		/**
//		 * The list of PAWs
//		 */	
//        ArrayList<PlatformActivityWindow> pawList = new ArrayList<>();
//
//        for (int i = 0; i < pawsFilenames.length; i++)
//        {
//			/**
//			 * The PAWs
//			 */	
//            PlatformActivityWindows paws = pawReader.readPAW(pawsFilenames[i]);
//
//            pawList.addAll(paws.getListPAW());
//        }
//
//        return pawList;
//
//    }

}

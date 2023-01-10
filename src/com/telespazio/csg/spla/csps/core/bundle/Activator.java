/**
*
* MODULE FILE NAME: Activator.java
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

package com.telespazio.csg.spla.csps.core.bundle;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import com.nais.spla.msl.main.core.MessageServiceLayer;
import com.telespazio.csg.spla.csps.core.server.CSPS;
import com.telespazio.csg.spla.csps.core.server.Environment;

import it.sistematica.spla.dpl.core.util.HibernateUtil;

/**
 * The Activator class of the CSPS bundle.
 */
public class Activator implements BundleActivator {

	private static BundleContext context;

	private static CSPS csps;

	/**
	 * Get bundle context
	 * 
	 * @return
	 */
	static BundleContext getContext() {
		return context;
	}

	/**
	 * The bundle activator
	 */
	public Activator() {
	}

	/**
	 * Start bundle
	 *
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.
	 *      BundleContext)
	 */
	@Override
	public void start(BundleContext context) throws Exception {
		// Getting an MSL
		// reference
		ServiceReference serviceReference = context.getServiceReference(MessageServiceLayer.class.getName());
		MessageServiceLayer msl = (MessageServiceLayer) context.getService(serviceReference);
		Activator.context = context;
		
		// Init configuration
		Environment.initConfiguration();
		HibernateUtil.init();
		
		// Instance CSPS
		csps = new CSPS(Environment.getConfiguration().getModuleName(), msl);
		
		// Start CSPS
		System.out.println("Starting CSPS Activator...");	
		csps.start();
	}

	/**
	 * Stop bundle
	 *
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	@Override
	public void stop(BundleContext context) throws Exception {
		
		// Stop CSPS
		System.out.println("CSPS shutdown ...");
		csps.stopServer();
		
		// CSPS halted
		System.out.println("CSPS halted");
		Activator.context = null;
	}
}
/**
*
* MODULE FILE NAME: AoICalculator.java
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

import com.telespazio.csg.spla.csps.core.server.Configuration;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * The AoI Calculator
 *
 * @author bunkheila
 *
 */
public class AoICalculator {

	/**
	 * Return the intersection boolean between 2 AoIs, true if the first AoI polygon
	 * is covered from the second one for the given coverage fraction at least. //
	 * Hp: AoIs are polygons
	 *
	 * @param aoI1
	 *            - the first AoI in [lat(°), lon(°)] coordinates
	 * @param aoI2
	 *            - the first AoI in [lat(°), lon(°)] coordinates
	 * @return true if AoIs intersect, false otherwise
	 */
	public boolean isIntersectedAoI(String aoI1, String aoI2) {

		/**
		 * The output boolean
		 */
		boolean isIntersected = false;
		
		/**
		 * Precision Model
		 */
		PrecisionModel precModel = new PrecisionModel();

		/**
		 * The Geometry Factory
		 */
		GeometryFactory geomFactory = new GeometryFactory(precModel, 2);

		if ((aoI1 != null) && (aoI2 != null)) {

			String[] aoI1Data = aoI1.split(" ");
			String[] aoI2Data = aoI2.split(" ");

			Coordinate[] coords1 = new Coordinate[(int) (aoI1Data.length / 2.0)];
			Coordinate[] coords2 = new Coordinate[(int) (aoI2Data.length / 2.0)];

			for (int i = 0; i < coords1.length; i++) {

				coords1[i] = new Coordinate(Double.parseDouble(aoI1Data[2 * i]),
						Double.parseDouble(aoI1Data[(2 * i) + 1]));
			}

			for (int i = 0; i < coords2.length; i++) {

				coords2[i] = new Coordinate(Double.parseDouble(aoI2Data[2 * i]),
						Double.parseDouble(aoI2Data[(2 * i) + 1]));
			}

			// Check geometry intersection
			Geometry geom1 = geomFactory.createPolygon(coords1);
			Geometry geom2 = geomFactory.createPolygon(coords2);

			Geometry intGeom = getAoIIntersection(geom1, geom2);
			
			double aoICov = (intGeom.getArea() / geom1.getArea());
			
			if (aoICov > Configuration.aoICovFrac) {

				isIntersected = true;
			}
		}

		return isIntersected;
	}

	/**
	 * Get the intersection geometry between 2 input geometry
	 *
	 * @param geom1
	 * @param geom2
	 * @return
	 */
	private static Geometry getAoIIntersection(Geometry geom1, Geometry geom2) {

		Geometry intGeom = geom1.intersection(geom2);

		return intGeom;

	}
}

/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.siddi.util;


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class MaxKStore {

    //Holds the Max K readings
    private Map<Double, Long> maxKUnits;
    //No of data to be held in the Map: The value of K
    private int dataToHold = 0;

    public MaxKStore(int count) {
        maxKUnits = new LinkedHashMap<Double, Long>();
        dataToHold = count;
    }

    /*
     * Callculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public Map<Double, Long> getMaxK(double value, long date) {

        // Holds pressure readings that are sorted in descending order according to the key (pressure value).
        Map<Double, Long> currentMax = new TreeMap<Double, Long>(maxKUnits).descendingMap();

        if (currentMax.size() < dataToHold) {

            currentMax.put(value, date);

        } else if (currentMax.size() == dataToHold) {

            double minkey = Collections.min(currentMax.keySet());

            if (minkey < value) {
                Object firstEvent = currentMax.remove(minkey);
                currentMax.put(value, date);
            }else if (minkey == value) {
                // if same pressure reading exist in the Map, updates the new value with updated timestamp.
                currentMax.put(value, date);
            }

        }

        maxKUnits = currentMax;
        return currentMax;
    }
}

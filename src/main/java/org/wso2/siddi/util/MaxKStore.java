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
    private Map<Double, Long> maxKUnits = new TreeMap<Double, Long>();
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
    public synchronized Map<Double, Long> getMaxK(double value, long date) {

        if (maxKUnits.size() < dataToHold) {

            maxKUnits.put(value, date);

        } else if (maxKUnits.size() == dataToHold) {

            double minKey = Collections.min(maxKUnits.keySet());

            if (minKey < value) {
                Object firstEvent = maxKUnits.remove(minKey);
                maxKUnits.put(value, date);
            }else if (minKey == value) {
                // if same pressure reading exist in the Map, updates the new value with updated timestamp.
                maxKUnits.put(value, date);
            }

        }

        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
        return new TreeMap<Double, Long>(maxKUnits).descendingMap();
    }
}

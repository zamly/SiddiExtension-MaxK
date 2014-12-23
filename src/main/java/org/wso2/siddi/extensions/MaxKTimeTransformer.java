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
package org.wso2.siddi.extensions;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;
import org.wso2.siddi.util.MaxKStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@SiddhiExtension(namespace = "MaxK", function = "getMaxK")
public class MaxKTimeTransformer extends TransformProcessor {

    private Map<String, Integer> paramPositions = new HashMap<String, Integer>();

    private static final Logger LOGGER = Logger.getLogger(MaxKTimeTransformer.class);
    private boolean debugEnabled = false;

    private String value = "";
    private String date = "";
    //The desired attribute position of value in input stream
    private int valuePosition = 0;
    private int datePosition = 0;
    //The K value
    private int capacity = 0;
    //The time in milliseconds for resetting.
    private int resetTimeRate = -1;
    //An array of Objects to manipulate output stream elements
    private Object[] data = null;

    private static int count = 0;
    private MaxKStore maxKStore = null;

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (debugEnabled) {
            LOGGER.debug("Processing a new Event for TopK Determination, Event : " + inEvent);
        }
        processEventForMaxK(inEvent);
        return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), data);
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InListEvent transformedListEvent = new InListEvent();
        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                transformedListEvent.addEvent((Event) processEvent((InEvent) event));
            }
        }
        return transformedListEvent;
    }

    @Override
    protected Object[] currentState() {
        return new Object[]{value,valuePosition,date,datePosition,capacity,resetTimeRate,maxKStore};
    }

    @Override
    protected void restoreState(Object[] objects) {
        if ((objects.length == 7) &&
                (objects[0] instanceof String) && (objects[1] instanceof Integer) &&
                (objects[2] instanceof String) && (objects[3] instanceof Integer) &&
                (objects[4] instanceof Integer) &&
                (objects[5] instanceof Integer) &&
                (objects[6] instanceof MaxKStore) ) {

            this.value = (String) objects[0];
            this.valuePosition = (Integer) objects[1];
            this.date = (String) objects[2];
            this.datePosition = (Integer) objects[3];
            this.capacity = (Integer) objects[4];
            this.resetTimeRate = (Integer) objects[5];
            this.maxKStore = (MaxKStore) objects[6];

        } else {
            LOGGER.error("Failed in restoring the Max-K Transformer.");
        }
    }

    @Override
    protected void init(Expression[] expressions,
                        List<ExpressionExecutor> expressionExecutors,
                        StreamDefinition inStreamDefinition,
                        StreamDefinition outStreamDefinition,
                        String elementId,
                        SiddhiContext siddhiContext) {

        debugEnabled = LOGGER.isDebugEnabled();

        if (expressions.length != 4) {
            LOGGER.error("Required Parameters : Four");
            throw new QueryCreationException("Mismatching Parameter count.");
        }

        //Getting all the parameters and assign those to instance variables
        value = ((Variable) expressions[0]).getAttributeName();
        date = ((Variable) expressions[1]).getAttributeName();
        capacity = ((IntConstant) expressions[2]).getValue();
        resetTimeRate = ((IntConstant) expressions[3]).getValue();

        valuePosition = inStreamDefinition.getAttributePosition(value);
        datePosition = inStreamDefinition.getAttributePosition(date);

        this.outStreamDefinition = new StreamDefinition().name("MaxKStream");
        for (int i = 1; i <= capacity; i++) {
            this.outStreamDefinition.attribute("max" + i , Attribute.Type.DOUBLE);
            this.outStreamDefinition.attribute("date" + i , Attribute.Type.LONG);
        }

        //Initiate the data object array that is to be sent with output stream
        data = new Object[2 * capacity];
        maxKStore = new MaxKStore(capacity);

        //If the reset time is grater than zero, then starting the ScheduledExecutorService instance that will schedule resetting stream-lib connector.
        if (resetTimeRate > 0) {
            siddhiContext.getScheduledExecutorService().scheduleAtFixedRate(executorTask, 0, resetTimeRate, TimeUnit.SECONDS);
            //ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            //scheduler.scheduleAtFixedRate(executorTask, 0, resetTimeRate, TimeUnit.SECONDS);
        }

    }

    @Override
    public void destroy() {

    }

    private void processEventForMaxK(InEvent event) {

        Object eventKeyValue = event.getData(valuePosition);
        Object eventKeyDate = event.getData(datePosition);

        Map<Double, Long> currentTopK = new HashMap<Double, Long>(count);

        currentTopK = maxKStore.getMaxK((Double)eventKeyValue, (Long)eventKeyDate);

        int currentTopKSize = currentTopK.size();

        int position = 0;

        //populating the 'data' object array with the latest Max-K
        for (Map.Entry<Double, Long> entry : currentTopK.entrySet()) {
            data[position++] = entry.getKey();
            data[position++] = entry.getValue();
        }

        //Populating remaing elements for the payload of the stream.
        if (currentTopKSize < capacity) {
            while (position <= (2 * capacity - 2)) {
                data[position++] = -1;
                data[position++] = -1;
            }
        }
        if (debugEnabled) {
            LOGGER.debug("Latest Top-K elements with frequency" + data);
        }

    }

    /**
     * Runnable instance resents the container that holds Max-k values.
     */
    private Runnable executorTask = new Runnable() {
        @Override
        public void run() {

            maxKStore = new MaxKStore(capacity);
        }
    };
}

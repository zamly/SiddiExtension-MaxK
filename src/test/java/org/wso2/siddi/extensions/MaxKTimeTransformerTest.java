/*
*  Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;

public class MaxKTimeTransformerTest {

    private static Logger logger = Logger.getLogger(MaxKTimeTransformerTest.class);
    protected static SiddhiManager siddhiManager;
    private static List<Object[]> data;
    protected long start;
    protected long end;

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
        logger.info("Shutting down Siddhi");
        siddhiManager.shutdown();
    }

    @Test
    public void test() throws Exception {
        logger.info("Testing");

        start = System.currentTimeMillis();

        String eventFuseExecutionPlan =
                "from pressureStream#transform.MaxK:getMaxK(value, date, 10, 30) \n" +
                        "select * output last every 30 sec\n" +
                        "insert into topKStream;";


        String eventFuseQueryReference = siddhiManager.addQuery(eventFuseExecutionPlan);

        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        siddhiManager.addCallback(eventFuseQueryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                Object results = null;
                String result = "";
                for (Event event : inEvents) {

                    result = "";

                    for (int t = 0; t < 20; t++) {
                        results = event.getData(t);
                        result = result + " | " + results.toString();
                    }

                    System.out.println(result);
                    logger.info("Result is" + result);
                }

            }
        });
        generateEvents();
    }

    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("pressureStream");
        int count = 0;
        for (Object[] dataLine : data) {
            count++;
//            if (count == 6 || count == 11) {
//                Thread.sleep(10000);
//            }
            Thread.sleep(1000);
            inputHandler.send(new Object[]{dataLine[0], dataLine[1], dataLine[2] });
        }
    }

    @Before
    public void setUp() throws Exception {
        logger.info("Initializing Siddhi setUp");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>();
        extensions.add(MaxKTimeTransformer.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        siddhiManager = new SiddhiManager(siddhiConfiguration);
        logger.info("calling setUpChild");
        siddhiManager.defineStream("define stream pressureStream (name string, value double, date long)");

        data = new ArrayList<Object[]>();

        data.add(new Object[]{"sensorZ", 159.8945, 4562947L});
        data.add(new Object[]{"sensorZ", 191.8945, 4562948L});
        data.add(new Object[]{"sensorZ", 167.8945, 4562949L});
        data.add(new Object[]{"sensorZ", 198.8945, 4562957L});
        data.add(new Object[]{"sensorZ", 165.8945, 4562948L});
        data.add(new Object[]{"sensorZ", 167.8945, 4562000L});
        data.add(new Object[]{"sensorZ", 179.8945, 4562960L});
        data.add(new Object[]{"sensorZ", 184.8945, 4562961L});
        data.add(new Object[]{"sensorZ", 151.8945, 4562963L});
        data.add(new Object[]{"sensorZ", 165.8945, 4562964L});
        data.add(new Object[]{"sensorZ", 182.8945, 4562967L});
        data.add(new Object[]{"sensorZ", 193.8945, 4562970L});
        data.add(new Object[]{"sensorZ", 194.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 190.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 196.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 184.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 182.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 181.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 188.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 187.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 185.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 156.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 179.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 177.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 166.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 168.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 169.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 159.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 199.9999, 4562989L});
        data.add(new Object[]{"sensorZ", 195.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 184.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 182.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 190.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 175.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 162.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 161.9945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 198.22222, 4562000L});
        data.add(new Object[]{"sensorZ", 198.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 199.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4561000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562000L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4562989L});
        data.add(new Object[]{"sensorZ", 154.23945, 4563000L});
    }
}

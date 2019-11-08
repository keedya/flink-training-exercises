package com.dataartisans.flinktraining.examples.datastream_java.connectors;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.admin.StreamManager;



public class RideCleansingToPravega {

    private static final String PRAVEGA_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    public static final String CLEANSE_PRAVEGA_STREAM_NAME = "cleansedRides";
    public static final String PRAVEGA_SCOPE = "workshop-samples";

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

        // initialize the parameter utility tool in order to retrieve input parameters
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(PRAVEGA_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = createStream(pravegaConfig,
                CLEANSE_PRAVEGA_STREAM_NAME,
                StreamConfiguration.builder().build());

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // create the Pravega sink to write a stream of taxi rides
        FlinkPravegaWriter<TaxiRide> writer = FlinkPravegaWriter.<TaxiRide>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withEventRouter(new EventRouter())
                .withSerializationSchema(new TaxiRideSchema())
                .build();
        // write the filtered data to a pravega sink
        filteredRides.addSink(writer).name("Pravega Stream");

        //print the output
        filteredRides.print().name("stdout");

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    /**
     * Creates a Pravega stream with a given configuration.
     *
     * @param pravegaConfig the Pravega configuration.
     * @param streamName the stream name (qualified or unqualified).
     * @param streamConfig the stream configuration (scaling policy, retention policy).
     */
    public static Stream createStream(PravegaConfig pravegaConfig, String streamName, StreamConfiguration streamConfig) {
        // resolve the qualified name of the stream
        Stream stream = pravegaConfig.resolve(streamName);

        try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
            // create the requested scope (if necessary)
            //streamManager.createScope(stream.getScope());

            // create the requested stream based on the given stream configuration
            streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        }

        return stream;
    }


    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {

            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<TaxiRide> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(TaxiRide event) {
            return "SameRoutingKey";
        }
    }

}

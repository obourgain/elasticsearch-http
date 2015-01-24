package org.elasticsearch.action.admin.cluster.health;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.io.stream.DataOutputStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class ClusterHealthResponseAccessor {

    public static ClusterHealthResponse create(String clusterName, ClusterHealthStatus status, Boolean timedOut, int numberOfNodes, int numberOfDataNodes, int activePrimaryShards, int activeShards, int relocatingShards, int initializingShards, int unassignedShards, List<String> validationFailures, Map<String, ClusterIndexHealth> indices) {
        ClusterHealthResponse response = new ClusterHealthResponse();

        // TODO can not rely on the constructor because it has a lot of logic in it and does not depends on the data I have, so let's hack it
        // in a byte array and use its unmarshalling
        ByteArrayDataOutput byteArrayDataOutput = ByteStreams.newDataOutput();
        DataOutputStreamOutput out = new DataOutputStreamOutput(byteArrayDataOutput);
        // copied from org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse.writeTo
        try {
            // ClusterHealthResponse's superclass org.elasticsearch.transport.TransportResponse have optional headers
            out.writeBoolean(false);

            out.writeString(clusterName);
            out.writeVInt(activePrimaryShards);
            out.writeVInt(activeShards);
            out.writeVInt(relocatingShards);
            out.writeVInt(initializingShards);
            out.writeVInt(unassignedShards);
            out.writeVInt(numberOfNodes);
            out.writeVInt(numberOfDataNodes);
            out.writeByte(status.value());
            out.writeVInt(indices.size());
            for (ClusterIndexHealth indexHealth : indices.values()) {
                indexHealth.writeTo(out);
            }
            out.writeBoolean(timedOut);

            out.writeVInt(validationFailures.size());
            for (String failure : validationFailures) {
                out.writeString(failure);
            }

            StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(byteArrayDataOutput.toByteArray()));

            response.readFrom(in);
            return response;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

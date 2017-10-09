package server;

import edu.umass.cs.nio.*;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

public class ReplicatedServer extends SingleServer {
    public static final String SERVER_PREFIX = "server.";

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    public ReplicatedServer(NodeConfig<String> nodeConfig, String myID) throws
            IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID)));
        this.myID = myID;
        this.serverMessenger = new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);
        log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this
                .myID, this.serverMessenger.getListeningSocketAddress()});
    }

    // TODO: process bytes received from clients here
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // echo to client
        super.handleMessageFromClient(bytes, header);

        // relay to other servers
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
            if (!node.equals(myID))
                try {
                    this.serverMessenger.send(node, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        log.log(Level.INFO, "{0} received relayed message from {1}",
                new Object[]{this.myID, header.sndr}); // simply log
    }

    /**
     *
     * @param args The first argument is the properties file and the rest are
     *            names of servers to be started.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if(args.length>1)
            for(int i=1; i<args.length; i++)
                new ReplicatedServer(NodeConfigUtils
                        .getNodeConfigFromFile(args[0], SERVER_PREFIX), args[i]
                        .trim());
        else log.info("Incorrect number of arguments; not starting any server");
    }
}
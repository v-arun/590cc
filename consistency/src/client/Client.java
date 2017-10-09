package client;

import server.SingleServer;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

public class Client {
    protected final MessageNIOTransport<String, byte[]> nio;

    public Client() throws IOException {
        this.nio = new
                MessageNIOTransport<String, byte[]>(
                new
                        AbstractBytePacketDemultiplexer() {

                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {

                                handleMessageClient(bytes, nioHeader);
                                return true;
                            }
                        });
    }

    protected void handleMessageClient(byte[] bytes, NIOHeader header) {
        // expect echo reply here
        try {
            System.out.println(new String(bytes, SingleServer.DEFAULT_ENCODING));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        InetSocketAddress isa = SingleServer.getSocketAddress(args);

        Client client = new Client();
        client.nio.send(isa, "hello world".getBytes(SingleServer.DEFAULT_ENCODING));
    }
}
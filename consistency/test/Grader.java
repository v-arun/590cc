import client.Client;
import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.DefaultTest;
import org.junit.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import server.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class Grader extends DefaultTest{

    private static final String DEFAULT_KEYSPACE = "demo";
    private static final InetSocketAddress DEFAULT_SADDR = new InetSocketAddress
            ("localhost", 1999);
    private static final InetSocketAddress DEFAULT_DB_ADDR = new
            InetSocketAddress("localhost", 9042);
    private static final int NUM_SERVERS = 3;
    private static final int SLEEP = 1000;
    private static final String CONFIG_FILE = System.getProperty("config")
            !=null ? System.getProperty("config") : "conf/servers.properties";

    private static Client client = null;
    private static SingleServer singleServer = null;
    private static SingleServer[] replicatedServers=null;
    private static Map<String, InetSocketAddress> serverMap = null;
    private static String[] servers = null;
    private static Cluster cluster;
    private static Session session = (cluster=Cluster.builder().addContactPoint
            (DEFAULT_SADDR
                    .getHostName()).build()).connect(DEFAULT_KEYSPACE);

    private static final boolean GRADING_MODE = false;

    @BeforeClass
    public static void setup() throws IOException {
        // setup single server
        singleServer = GRADING_MODE ? new MyDBSingleServer(DEFAULT_SADDR,
                DEFAULT_DB_ADDR, DEFAULT_KEYSPACE) :
                new MyDBSingleServer(DEFAULT_SADDR, DEFAULT_DB_ADDR,
                        DEFAULT_KEYSPACE);
        NodeConfig<String> nodeConfigServer = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET);

        // setup client
        NodeConfig<String> nodeConfigClient = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX);
        client = GRADING_MODE ? new MyDBClient(nodeConfigClient) : new
                MyDBClient(nodeConfigClient);

        // setup replicated servers
        replicatedServers = new SingleServer[nodeConfigServer.getNodeIDs().size()];
        int i=0;
        for(String node : nodeConfigServer.getNodeIDs())
            replicatedServers[i++] = GRADING_MODE ? new MyDBReplicatedServer
                    (nodeConfigServer, node, DEFAULT_DB_ADDR) : new
                    MyDBReplicatedServer
                    (nodeConfigServer, node, DEFAULT_DB_ADDR);

        // setup frequently used information
        i=0;
        servers = new String[nodeConfigServer.getNodeIDs().size()];
        for(String node : nodeConfigServer.getNodeIDs())
            servers[i++] = node;
        serverMap = new HashMap<String, InetSocketAddress>();
        for(String node : nodeConfigClient.getNodeIDs())
            serverMap.put(node, new InetSocketAddress(nodeConfigClient
                    .getNodeAddress
                            (node), nodeConfigClient.getNodePort(node)));

    }

    /**
     * This test tests a simple default DB command expected to always succeed.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test01_DefaultAsync() throws IOException,
            InterruptedException {
        client.send(DEFAULT_SADDR, "select table_name from system_schema" +
                ".tables");
    }

    /**
     * Tests that a table is indeed being created successfully.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test02_Single_CreateTable_Async() throws IOException,
            InterruptedException {
        testCreateTable(true, true);
    }


    @Test
    public void test03_InsertRecords_Async() throws IOException {
        for(int i=0; i<10; i++) {
            send("insert into " + TABLE + " (ssn, firstname, lastname) " +
                    "values (" + (int)(Math.random()*Integer.MAX_VALUE) + ", '" +
                    "John"+i + "', '" + "Smith"+i +"')", true);
        }
    }

    @Test
    public void test04_DeleteRecords_Async() throws IOException, InterruptedException {
        send("truncate users", true);
        Thread.sleep(SLEEP);
        ResultSet resultSet = session.execute("select count(*) from " + TABLE);
        Assert.assertTrue(!resultSet.isExhausted());
        Assert.assertEquals(0, resultSet.one().getLong(0));
    }

    @Test
    public void test05_CreateTable_Sync() throws IOException, InterruptedException {
        testCreateTable(true, false);
    }

    @Test
    public void test99_closeSession() {
        session.close();;
    }

    private void testCreateTable(boolean single, boolean sleep) throws
            IOException, InterruptedException {
        if(sleep) testCreateTableSleep(single);
        else testCreateTableBlocking(single);
        verifyTableExists(TABLE);

    }
    private void verifyTableExists(String table) {
        ResultSet resultSet = session.execute("select table_name from " +
                "system_schema.tables where keyspace_name='"+DEFAULT_KEYSPACE+"'" );
        Assert.assertTrue(!resultSet.isExhausted());
        boolean match = false;
        for(Row row : resultSet)
            match = match || row.getString("table_name").equals(table);
        Assert.assertTrue(match);
    }

    private void testCreateTableSleep(boolean single) throws
            InterruptedException, IOException {
        send(getDropTableCmd(TABLE), single);
        Thread.sleep(SLEEP);
        send(getCreateTableCmd(TABLE), single);
        Thread.sleep(SLEEP);
    }


    ConcurrentHashMap<Long, String> outstanding = new ConcurrentHashMap<Long, String>();

    private void testCreateTableBlocking(boolean single) throws
            InterruptedException, IOException {
        waitResponse(callbackSend(DEFAULT_SADDR, getDropTableCmd(TABLE)));
        waitResponse(callbackSend(DEFAULT_SADDR, getCreateTableCmd(TABLE)));
    }

    private Long callbackSend(InetSocketAddress isa, String request) throws
            IOException {
        Long id = enqueueRequest(request);
        client.callbackSend(isa, request, new WaitCallback(id));
        return id;
    }

    private class WaitCallback implements Client.Callback{
        Long monitor; // both id and monitor
        WaitCallback(Long monitor) {
            this.monitor = monitor;
        }

        @Override
        public void handleResponse(byte[] bytes, NIOHeader header) {
            synchronized (this.monitor) {
                outstanding.remove(monitor);
                this.monitor.notify();
            }
        }
    }

    private long reqnum = 0;
    private long enqueue() {
        synchronized (outstanding) {
            return reqnum++;
        }
    }
    private long enqueueRequest(String request) {
        long id = enqueue();
        outstanding.put(id, request);
        return id;
    }

    private void waitResponse(Long id) {
        synchronized (id) {
            while(outstanding.containsKey(id))
                try {
                    id.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }


    private static final void send(String cmd, boolean single) throws
            IOException {
        client.send(single ? DEFAULT_SADDR :
                        serverMap.get(servers[(int) (Math.random() * servers.length)]),
                cmd);
    }

    private final void send(String cmd) throws IOException {
        send(cmd, false);
    }


    private static final String TABLE = "users";
    private static String getCreateTableCmd(String table) {
        return "create table if not exists " + table + " (age int, firstname " +
                "text, lastname text, ssn int, address text, hash bigint, " +
                "primary key (ssn))";
    }
    private static String getDropTableCmd(String table) {
        return "drop table if exists " + table;
    }

    @AfterClass
    public static void teardown() {
        if(replicatedServers!=null)
            for(SingleServer s : replicatedServers)
                s.close();
        if(client!=null) client.close();
        if(singleServer!=null) singleServer.close();
        session.close();
        cluster.close();
    }


    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(Grader.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }

}

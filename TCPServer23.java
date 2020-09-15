import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class TCPServer23 {

    public static void main(String[] args) {
        final int SERVER_PORT = 5023;

        ServerSocket serverSocket = null;
        ArrayList<msgEPartition> subscriptions = new ArrayList<>();
        ArrayList<msgEPartition> publications = new ArrayList<>();
        SubspaceAllocator subspaceAllocator = new SubspaceAllocator();
        AttributeOrderSorter attributeOrderSorter = new AttributeOrderSorter(new AttributeOrder());

        int brokerID = 23;
        ArrayList<RoutingTable> routingTable = new ArrayList<>();
        routingTable.add(new RoutingTable(27, 5027));
        routingTable.add(new RoutingTable(27, 5027));
        routingTable.add(new RoutingTable(27, 5027));
        routingTable.add(new RoutingTable(31, 5031));
        routingTable.add(new RoutingTable(7, 5007));

        try {
            serverSocket = new ServerSocket();

            String localHostAddress = InetAddress.getLocalHost().getHostAddress();
            serverSocket.bind(new InetSocketAddress(localHostAddress, SERVER_PORT));
            //System.out.println("[server] binding! \naddress:" + localHostAddress + ", port:" + SERVER_PORT);

            while (true) {
                Socket socket = serverSocket.accept();
                new ProcessThread(socket, brokerID, routingTable, subspaceAllocator, attributeOrderSorter, subscriptions, publications).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
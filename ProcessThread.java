import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class ProcessThread extends Thread {

    private Socket socket = null;
    private int brokerID = 0;
    private ArrayList<RoutingTable> routingTable;
    private ArrayList<msgEPartition> subscriptions = null;
    private ArrayList<msgEPartition> publications = null;
    private SubspaceAllocator subspaceAllocator = null;
    private AttributeOrderSorter attributeOrderSorter = null;

    public ProcessThread(Socket socket, int brokerID, ArrayList<RoutingTable> routingTable, SubspaceAllocator subspaceAllocator, AttributeOrderSorter attributeOrderSorter, ArrayList<msgEPartition> subscriptions, ArrayList<msgEPartition> publications) {

        this.socket = socket;
        this.brokerID = brokerID;
        this.routingTable = routingTable;
        this.subspaceAllocator = subspaceAllocator;
        this.attributeOrderSorter = attributeOrderSorter;
        this.subscriptions = subscriptions;
        this.publications = publications;
    }

    @Override
    public void run() {

        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
        int remoteHostPort = remoteSocketAddress.getPort();
//        System.out.println("[server] connected! \nconnected socket address:" + remoteHostName
//                + ", port:" + remoteHostPort);
        // print message(s)

        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

            msgEPartition message = msgEPartition.parseFrom(dataInputStream);

            //System.out.println(message.getMsgType());
            String out = "";
            int targetKey;
            int targetIndex;
            int tempKey;
            int tempTargetKey;
            msgEPartition.Builder[] messageBuilders;
            msgEPartition[] messages;
            ArrayList<Integer> preventDuplicate = new ArrayList<>();

            if (message.getMsgType().equals("Subscription")) {
                if (message.getSubspaceForward() == "") {

                    message = attributeOrderSorter.sortAttributeOrder(message);
                    message = subspaceAllocator.allocateSubspace(message);

                    messageBuilders = new msgEPartition.Builder[message.getSubspaceList().size()];
                    messages = new msgEPartition[message.getSubspaceList().size()];

                    for (int i = 0; i < message.getSubspaceList().size(); i++) {

                        if (preventDuplicate.contains(RoutingTable.getSuccessor(simpleNet.getHashCode(message.getSubspace(i))))) // 이벤트 처리할 때도 같은 브로커 안에 있는 중복된 subscription 에 대해 이미 보낸거 안 보내야 함.
                            continue;
                        else
                            preventDuplicate.add(RoutingTable.getSuccessor(simpleNet.getHashCode(message.getSubspace(i))));
                        messageBuilders[i] = msgEPartition.newBuilder();
                        messageBuilders[i].mergeFrom(message);
                        messageBuilders[i].setSubspaceForward(message.getSubspace(i));
                        messages[i] = messageBuilders[i].build();
                    }

                    for (msgEPartition msg : messages) {

                        if (msg == null)
                            continue;

                        targetIndex = -1;
                        targetKey = RoutingTable.getSuccessor(simpleNet.getHashCode(msg.getSubspaceForward()));

                        if (brokerID != targetKey) {
//                            System.out.println("key = " + targetKey);

                            for (int i = 0; i < 5; i++) {

                                if (i == 0) {

                                    if (brokerID > routingTable.get(i).brokerID) {
                                        tempKey = routingTable.get(i).brokerID + 32;
                                        if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
                                            tempTargetKey = targetKey + 32;
                                        else
                                            tempTargetKey = targetKey;
                                    } else {
                                        tempKey = routingTable.get(i).brokerID;
                                        tempTargetKey = targetKey;
                                    }

                                    if (tempTargetKey > brokerID && tempTargetKey <= tempKey)
                                        targetIndex = i;

                                } else {
                                    if (routingTable.get(i - 1).brokerID == routingTable.get(i).brokerID) {
                                        if (i == 4)
                                            break;
                                        else
                                            continue;
                                    }

                                    if (routingTable.get(i - 1).brokerID > routingTable.get(i).brokerID) {
                                        tempKey = routingTable.get(i).brokerID + 32;
                                        if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
                                            tempTargetKey = targetKey + 32;
                                        else
                                            tempTargetKey = targetKey;
                                    } else {
                                        tempKey = routingTable.get(i).brokerID;
                                        tempTargetKey = targetKey;
                                    }

                                    if (tempTargetKey > routingTable.get(i - 1).brokerID && tempTargetKey < tempKey)
                                        targetIndex = i - 1;
                                    if (tempTargetKey > routingTable.get(i - 1).brokerID && tempTargetKey == tempKey)
                                        targetIndex = i;
                                }
                            }
                            if (targetIndex == -1)
                                targetIndex = 4;
//                            System.out.println("index = " + targetIndex);

                            new RouteThread(msg, routingTable.get(targetIndex).port).start();

                        } else {
                            synchronized (subscriptions) {
                                subscriptions.add(msg);
                                System.out.println("size = " + +subscriptions.size());
                            }
                        }
                    }
                } else {
                    targetIndex = -1;
                    targetKey = RoutingTable.getSuccessor(simpleNet.getHashCode(message.getSubspaceForward()));

                    if (brokerID != targetKey) {

                        for (int i = 0; i < 5; i++) {

                            if (i == 0) {

                                if (brokerID > routingTable.get(i).brokerID) {
                                    tempKey = routingTable.get(i).brokerID + 32;
                                    if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
                                        tempTargetKey = targetKey + 32;
                                    else
                                        tempTargetKey = targetKey;
                                } else {
                                    tempKey = routingTable.get(i).brokerID;
                                    tempTargetKey = targetKey;
                                }

                                if (tempTargetKey > brokerID && tempTargetKey <= tempKey)
                                    targetIndex = i;

                            } else {
                                if (routingTable.get(i - 1).brokerID == routingTable.get(i).brokerID) {
                                    if (i == 4)
                                        break;
                                    else
                                        continue;
                                }

                                if (routingTable.get(i - 1).brokerID > routingTable.get(i).brokerID) {
                                    tempKey = routingTable.get(i).brokerID + 32;
                                    if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
                                        tempTargetKey = targetKey + 32;
                                    else
                                        tempTargetKey = targetKey;
                                } else {
                                    tempKey = routingTable.get(i).brokerID;
                                    tempTargetKey = targetKey;
                                }

                                if (tempTargetKey > routingTable.get(i - 1).brokerID && tempTargetKey < tempKey)
                                    targetIndex = i - 1;
                                if (tempTargetKey > routingTable.get(i - 1).brokerID && tempTargetKey == tempKey)
                                    targetIndex = i;
                            }
                        }
                        if (targetIndex == -1)
                            targetIndex = 4;

//                        System.out.println(routingTable.get(targetIndex).port);
                        new RouteThread(message, routingTable.get(targetIndex).port).start();

                    } else {
                        synchronized (subscriptions) {
                            subscriptions.add(message);
                            System.out.println("(routed) size = " + +subscriptions.size());
                        }
                    }
                }
            }

//
//            if (message.getMsgType().equals("Subscription")) {
//
//                for(String subspace : message.getSubspaceList())
//                    System.out.println(subspace);
//
//                for (String subspace : message.getSubspaceList()) {
//
//                    targetIndex = -1;
//                    targetKey = RoutingTable.getSuccessor(simpleNet.getHashCode(subspace));
//                    if (brokerID != targetKey) {
//
//                        for (int i = 0; i < 5; i++) {
//
//                            if (i == 0) {
//
//                                if (brokerID > routingTable.get(i).brokerID) {
//                                    tempKey = routingTable.get(i).brokerID + 32;
//                                    if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
//                                        tempTargetKey = targetKey + 32;
//                                    else
//                                        tempTargetKey = targetKey;
//                                } else {
//                                    tempKey = routingTable.get(i).brokerID;
//                                    tempTargetKey = targetKey;
//                                }
//
//                                if (tempTargetKey > brokerID && tempTargetKey <= tempKey)
//                                    targetIndex = i;
//
//                            } else {
//                                if (routingTable.get(i - 1).brokerID == routingTable.get(i).brokerID) {
//                                    if (i == 4)
//                                        break;
//                                    else
//                                        continue;
//                                }
//
//                                if (routingTable.get(i - 1).brokerID > routingTable.get(i).brokerID) {
//                                    tempKey = routingTable.get(i).brokerID + 32;
//                                    if (targetKey >= 0 && targetKey <= routingTable.get(i).brokerID)
//                                        tempTargetKey = targetKey + 32;
//                                    else
//                                        tempTargetKey = targetKey;
//                                } else {
//                                    tempKey = routingTable.get(i).brokerID;
//                                    tempTargetKey = targetKey;
//
//                                    if (tempTargetKey > routingTable.get(i - 1).brokerID && tempTargetKey <= tempKey)
//                                        targetIndex = i;
//                                }
//                            }
//                        }
//                        if (targetIndex == -1)
//                            targetIndex = 4;
//
//                        new RouteThread(message, routingTable.get(targetIndex).port).start();
//
//                    } else {
//                        synchronized (subscriptions) {
//                            subscriptions.add(message);
//                            System.out.println(subscriptions.size());
//                        }
//                    }
//                }
//            } else if (message.getMsgType().equals("Publication")) {
//
//                synchronized (publications) {
//
//                    publications.add(message);
//
////                            out = out + message.getIPAddress() + " " + message.getPayload() + " ";
////                            for (String subspace : message.getSubspaceList())
////                                out = out + subspace + " ";
////                            for (String attribute : message.getAttributeList())
////                                out = out + attribute + " ";
////                            for (double singlePoint : message.getPub().getSinglePointList())
////                                out = out + singlePoint + " ";
//                }
//            } else if (message.getMsgType().equals("Unsubscription")) {
//
//                /*
//                To be implemented in the future.
//                 */
//
//                out = message.getIPAddress() + " " + message.getPayload() + " ";
//                for (double lowerbound : message.getUnsub().getLowerBoundList())
//                    out = out + lowerbound + " ";
//                for (double upperbound : message.getUnsub().getUpperBoundList())
//                    out = out + upperbound + " ";
//            }

//            System.out.println(out);
            dataInputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
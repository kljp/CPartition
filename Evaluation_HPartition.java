import com.EPartition.EPartitionMessageSchema.msgEPartition;
import java.util.ArrayList;

public class Evaluation_HPartition {

    public static void main(String[] args) {

        SubspaceAllocator subspaceAllocator = new SubspaceAllocator();
        AttributeOrderSorter attributeOrderSorter = new AttributeOrderSorter(new AttributeOrder());

        String msgType = "Subscription";

        ArrayList<msgEPartition> subscriptionList = SubscriptionReader.readSubscriptions();
        MessageWrapper messageWrapper;
        msgEPartition message;

        ArrayList<ArrayList<msgEPartition>> brokers;
        ArrayList<msgEPartition> tempMsgs;

        int numRepeat = 1000;
        int numMsg = 10000;
        int interval = 1000;
        int numPhase = numMsg / interval;
        double[] accumulated = new double[numPhase];

        for (int i = 0; i < numPhase; i++)
            accumulated[i] = 0.0;

        double before = System.currentTimeMillis();

        for (int a = 0; a < numRepeat; a++) {

            brokers = new ArrayList<ArrayList<msgEPartition>>();
            tempMsgs = new ArrayList<msgEPartition>();

            for (int i = 0; i < GlobalState.NumberOfBrokers; i++)
                brokers.add(new ArrayList<msgEPartition>());

            for (int i = 0; i < numMsg; i++) {

                if(tempMsgs.size() / interval >= numPhase)
                    break;

                msgEPartition.Builder[] messageBuilders;
                msgEPartition[] messages;
                ArrayList<Integer> preventDuplicate = new ArrayList<>();

                if(msgType.equals("Publication")){

                    messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomValueGenerator());
                    message = messageWrapper.buildMsgEPartition();
                }

                else{
                    messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomRangeGenerator());
                    message = messageWrapper.buildMsgEPartition();
                }

                if (message.getMsgType().equals("Subscription")) {

                    message = attributeOrderSorter.sortAttributeOrder(message);
                    message = subspaceAllocator.allocateSubspace(message);

                    messageBuilders = new msgEPartition.Builder[message.getSubspaceList().size()];
                    messages = new msgEPartition[message.getSubspaceList().size()];

                    for (int j = 0; j < message.getSubspaceList().size(); j++) {

                        if (preventDuplicate.contains(Math.abs(MurmurHash.hash32(message.getSubspace(j))) % GlobalState.NumberOfBrokers)) // 이벤트 처리할 때도 같은 브로커 안에 있는 중복된 subscription 에 대해 이미 보낸거 안 보내야 함.
                            continue;
                        else
                            preventDuplicate.add(Math.abs(MurmurHash.hash32(message.getSubspace(j))) % GlobalState.NumberOfBrokers);
                        messageBuilders[j] = msgEPartition.newBuilder();
                        messageBuilders[j].mergeFrom(message);
                        messageBuilders[j].setSubspaceForward(message.getSubspace(j));
                        messages[j] = messageBuilders[j].build();

                    }

                    int identifier;
                    double[] data = new double[GlobalState.NumberOfBrokers];

                    for (msgEPartition msg : messages) {

                        if (msg == null)
                            continue;

                        identifier = Math.abs(MurmurHash.hash32(msg.getSubspaceForward())) % GlobalState.NumberOfBrokers;

                        brokers.get(identifier).add(msg);
                        tempMsgs.add(msg);

                        if(tempMsgs.size() % interval == 0){

                            // calculate normalized standard deviation
                            for (int j = 0; j < GlobalState.NumberOfBrokers; j++)
                                data[j] = brokers.get(j).size();
                            accumulated[tempMsgs.size() / interval - 1] += normStandardDeviation(data, 1);
                        }


                    }
                }
            }
        }

        for (int i = 0; i < numPhase; i++) {

            accumulated[i] /= numRepeat;
            System.out.println(accumulated[i]);
        }

        double after = System.currentTimeMillis();
        double elapsed = ((after - before) / 1000.0);
        System.out.println("elapsed: " + elapsed / numRepeat);
    }

    public static double mean(double[] array) {
        double sum = 0.0;

        for (int i = 0; i < array.length; i++)
            sum += array[i];

        return sum / array.length;
    }


    public static double normStandardDeviation(double[] array, int option) {
        if (array.length < 2) return Double.NaN;

        double sum = 0.0;
        double sd = 0.0;
        double diff;
        double meanValue = mean(array);

        for (int i = 0; i < array.length; i++) {
            diff = array[i] - meanValue;
            sum += diff * diff;
        }
        sd = Math.sqrt(sum / (array.length - option));
        sd = sd / meanValue;

        return sd;
    }


}

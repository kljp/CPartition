import com.EPartition.EPartitionMessageSchema.msgEPartition;
import java.util.ArrayList;
import java.util.Collections;

public class Evaluation_CPartition {

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

                        if(tempMsgs.size() % interval == 0 && tempMsgs.size() >= interval){

                            // calculate normalized standard deviation
                            for (int j = 0; j < GlobalState.NumberOfBrokers; j++)
                                data[j] = brokers.get(j).size();
                            accumulated[tempMsgs.size() / interval - 1] += normStandardDeviation(data, 1);

                            // do CPartition
                            if(tempMsgs.size() < numPhase - 1)
                                CPartition(tempMsgs);
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

    public static void CPartition(ArrayList<msgEPartition> tempMsgs) {

        int numAttr = GlobalState.NumberOfDimensions;
        double[][] ccs = new double[numAttr][numAttr];
        ArrayList<Integer> attrs = new ArrayList<Integer>();

        for (int i = 0; i < numAttr; i++)
            attrs.add(i);

        ArrayList<Double>[] tempAt = new ArrayList[numAttr];
        double[] sums = new double[numAttr];

        for (int i = 0; i < numAttr; i++){
            tempAt[i] = new ArrayList<Double>();
        }

        for (int i = 0; i < numAttr; i++) {
            for (int j = 0; j < tempMsgs.size(); j++) {
                tempAt[i].add((tempMsgs.get(j).getSub().getLowerBound(i) + tempMsgs.get(j).getSub().getLowerBound(i)) / 2.0);
            }
        }

        for (int i = 0; i < numAttr; i++) {
            sums[i] = 0.0;
        }

        for (int i = 0; i < numAttr; i++) {
            for (int j = 0; j < tempAt[i].size(); j++) {
                sums[i] += tempAt[i].get(j);
            }
        }

        double tempCalc1;
        double tempCalc2;
        double tempCalc3;

        for (int i = 0; i < numAttr; i++) {
            for (int j = 0; j < numAttr; j++) {
                if(i < j){
                    tempCalc1 = 0.0;
                    tempCalc2 = 0.0;
                    tempCalc3 = 0.0;
//                    ccs[i][j] = (double) ((int) (Math.random() * 10000)) / 10000.0;
                    for (int k = 0; k < tempMsgs.size(); k++) {
                        tempCalc1 += ((tempAt[i].get(k) - sums[i]) * (tempAt[j].get(k) - sums[j]));
                        tempCalc2 += ((tempAt[i].get(k) - sums[i]) * (tempAt[i].get(k) - sums[i]));
                        tempCalc3 += ((tempAt[j].get(k) - sums[j]) * (tempAt[j].get(k) - sums[j]));
                    }

                    tempCalc2 = Math.sqrt(tempCalc2);
                    tempCalc3 = Math.sqrt(tempCalc3);

                    ccs[i][j] = tempCalc1 / (tempCalc2 * tempCalc3);
                }

                else
                    ccs[i][j] = 2.0;
            }
        }

        for (int i = 0; i < numAttr; i++) {
            for (int j = 0; j < numAttr; j++) {
                if(i > j){
                    ccs[i][j] = ccs[j][i];
                }
            }
        }



//        for (int i = 0; i < 16; i++) {
//            for (int j = 0; j < 16; j++) {
//                System.out.print(ccs[i][j] + " ");
//            }
//            System.out.println();
//        }

        double temp;
        double ccss = 0.0;

        double[][] tempccs;


        temp = popt(ccs, attrs);

    }

    public static double popt(double[][] ccs, ArrayList<Integer> attrs){

        try {
            ArrayList<Double> sums = new ArrayList<Double>();

            ArrayList<Integer> temp1;
            ArrayList<Integer> temp2;
            ArrayList<Integer> temp3;
            ArrayList<Integer> temp4 = new ArrayList<Integer>();
            temp4.addAll(attrs);

            int a1 = -1, a2 = -1, a3 = -1, a4 = -1;
            int a5 = -1, a6 = -1, a7 = -1, a8 = -1;
            int a9 = -1, a10 = -1, a11 = -1, a12 = -1;

            double min;

            min= 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i == 0 && j == 0)
                        min = ccs[i][j];
                    if(i < j){
                        if(ccs[i][j] < min){
                            min = ccs[i][j];
                            a1 = i;
                            a2 = j;
                        }
                    }
                }
            }

            temp4.remove((Integer) a1);
            temp4.remove((Integer) a2);

            min = 1.0;
            int a3temp1 = 0;
            double tempSums1 = 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i < j){
                        if((a1 == i) || (a1 == j)){
//                        if(i == 0 && j == 0)
//                            min = ccs[i][j];
                            if(ccs[i][j] < min){
                                if(a1 == i && a2 != j)
                                    a3temp1 = j;
                                if(a1 == j && a2 != i)
                                    a3temp1 = i;
                                if((a1 == i && a2 != j) || (a1 == j && a2 != i))
                                    min = ccs[i][j];
                            }
                        }
                    }
                }
            }
            tempSums1 = min + ccs[a2][a3temp1];

            min = 1.0;
            int a3temp2 = 0;
            double tempSums2 = 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i < j){
                        if((a2 == i) || (a2 == j)){
//                        if(i == 0 && j == 0)
//                            min = ccs[i][j];
                            if(ccs[i][j] < min){
                                if(a2 == i && a1 != j)
                                    a3temp2 = j;
                                if(a2 == j && a1 != i)
                                    a3temp2 = i;
                                if((a2 == i && a1 != j) || (a2 == j && a1 != i))
                                    min = ccs[i][j];
                            }
                        }
                    }
                }
            }
            tempSums2 = min + ccs[a1][a3temp2];

            if(tempSums1 < tempSums2)
                a3 = a3temp1;
            if(tempSums1 > tempSums2)
                a3 = a3temp2;

            temp4.remove((Integer) a3);

            min = 1.0;
            int a4temp1 = 0;
            double temp2Sums1 = 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i < j){
                        if((a1 == i) || (a1 == j)){
//                        if(i == 0 && j == 0)
//                            min = ccs[i][j];
                            if(ccs[i][j] < min){
                                if(a1 == i && a2 != j && a3 != j)
                                    a4temp1 = j;
                                if(a1 == j && a2 != i && a3 != i)
                                    a4temp1 = i;
                                if((a1 == i && a2 != j && a3 != j) || (a1 == j && a2 != i && a3 != i))
                                    min = ccs[i][j];
                            }
                        }
                    }
                }
            }
            temp2Sums1 = min + ccs[a2][a4temp1] + ccs[a3][a4temp1];

            min = 1.0;
            int a4temp2 = 0;
            double temp2Sums2 = 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i < j){
                        if((a1 == i) || (a1 == j)){
//                        if(i == 0 && j == 0)
//                            min = ccs[i][j];
                            if(ccs[i][j] < min){
                                if(a2 == i && a1 != j && a3 != j)
                                    a4temp2 = j;
                                if(a2 == j && a1 != i && a3 != i)
                                    a4temp2 = i;
                                if((a2 == i && a1 != j && a3 != j) || (a2 == j && a1 != i && a3 != i))
                                    min = ccs[i][j];
                            }
                        }
                    }
                }
            }
            temp2Sums2 = min + ccs[a1][a4temp2] + ccs[a3][a4temp2];

            min = 1.0;
            int a4temp3 = 0;
            double temp2Sums3 = 0.0;
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    if(i < j){
                        if((a1 == i) || (a1 == j)){
//                        if(i == 0 && j == 0)
//                            min = ccs[i][j];
                            if(ccs[i][j] < min){
                                if(a3 == i && a1 != j && a2 != j)
                                    a4temp3 = j;
                                if(a3 == j && a1 != i && a2 != i)
                                    a4temp3 = i;
                                if((a3 == i && a1 != j && a2 != j) || (a3 == j && a1 != i && a2 != i))
                                    min = ccs[i][j];
                            }
                        }
                    }
                }
            }
            temp2Sums3 = min + ccs[a1][a4temp3] + ccs[a2][a4temp3];

            if(temp2Sums1 < temp2Sums2 && temp2Sums1 < temp2Sums3)
                a4 = a4temp1;
            if(temp2Sums2 < temp2Sums1 && temp2Sums2 < temp2Sums3)
                a4 = a4temp2;
            if(temp2Sums3 < temp2Sums1 && temp2Sums3 < temp2Sums2)
                a4 = a4temp3;

            temp4.remove((Integer) a4);

            temp1 = new ArrayList<Integer>();
            temp1.add(a1);
            temp1.add(a2);
            temp1.add(a3);
            temp1.add(a4);

            sums.add(ccs[a1][a2] + ccs[a1][a3] + ccs[a1][a4] + ccs[a2][a3] + ccs[a2][a4] + ccs[a3][a4]);
            ccs[a1][a2] = 2.0; ccs[a2][a1] = 2.0;
            ccs[a1][a3] = 2.0; ccs[a3][a1] = 2.0;
            ccs[a1][a4] = 2.0; ccs[a4][a1] = 2.0;
            ccs[a2][a3] = 2.0; ccs[a3][a2] = 2.0;
            ccs[a2][a4] = 2.0; ccs[a4][a2] = 2.0;
            ccs[a3][a4] = 2.0; ccs[a4][a3] = 2.0;





            sums.add(ccs[temp4.get(0)][temp4.get(1)] + ccs[temp4.get(0)][temp4.get(2)] + ccs[temp4.get(0)][temp4.get(3)]
                    + ccs[temp4.get(1)][temp4.get(2)] + ccs[temp4.get(1)][temp4.get(3)] + ccs[temp4.get(2)][temp4.get(3)]);

            double sumss = 0.0;
            for (int i = 0; i < sums.size(); i++)
                sumss += sums.get(i);

//            System.out.println(temp1.get(0) + " " + temp1.get(1) + " " + temp1.get(2) + " " + temp1.get(3));
//            System.out.println(temp4.get(0) + " " + temp4.get(1) + " " + temp4.get(2) + " " + temp4.get(3));

            GlobalState.attributes2[0] = "attr" + Integer.toString(temp1.get(0));
            GlobalState.attributes2[1] = "attr" + Integer.toString(temp1.get(1));
            GlobalState.attributes2[2] = "attr" + Integer.toString(temp1.get(2));
            GlobalState.attributes2[3] = "attr" + Integer.toString(temp1.get(3));
            GlobalState.attributes2[4] = "attr" + Integer.toString(temp4.get(0));
            GlobalState.attributes2[5] = "attr" + Integer.toString(temp4.get(1));
            GlobalState.attributes2[6] = "attr" + Integer.toString(temp4.get(2));
            GlobalState.attributes2[7] = "attr" + Integer.toString(temp4.get(3));

            return sumss;

        } catch (ArrayIndexOutOfBoundsException e){

            return -1.0;
        }
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

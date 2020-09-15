import com.EPartition.EPartitionMessageSchema.msgEPartition;

public class asd {

    public static void main(String[] args) {

        GlobalState.attributes2[0] = "attr_9";

        String msgType = "Subscription";
        MessageWrapper messageWrapper;
        msgEPartition message;

        messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomRangeGenerator());
        message = messageWrapper.buildMsgEPartition();

        System.out.println(message);
    }
}

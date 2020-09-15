import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class TCPClient6 {
    // 1. TCP 서버의 IP와 PORT를 상수로 할당
    // 실제로는 서버의 IP보다는 도메인을 작성하는 것이 좋다.
    private static final String SERVER_IP = "210.107.197.172";
    private static final int SERVER_PORT = 5027;

    public static void main(String[] args) {

        String msgType = "Subscription";

        ArrayList<msgEPartition> subscriptionList = SubscriptionReader.readSubscriptions();
        MessageWrapper messageWrapper;
        msgEPartition message;

        Socket socket;

//        System.out.println();
//        String sgc = subscription.getClass().toString().split("\\$")[1];
//        System.out.println(sgc);

        for (int i = 0; i < 100; i++) {

            if(msgType.equals("Publication")){

                messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomValueGenerator());
                message = messageWrapper.buildMsgEPartition();
            }

            else{
                message = subscriptionList.get((int) (Math.random() * GlobalState.NumberOfSubscriptions) % GlobalState.NumberOfSubscriptions);
            }

            System.out.println(i + "th trial");
            socket = null;

            try{
                // 2. 서버와 연결을 위한 소켓을 생성
                socket = new Socket();

                // 3. 생성한 소켓을 서버의 소켓과 연결(connect)

                socket.connect(new InetSocketAddress(SERVER_IP, SERVER_PORT));
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                message.writeTo(dataOutputStream);
                dataOutputStream.flush();
                dataOutputStream.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally{
                try{
                    if( socket != null && !socket.isClosed()){
                        socket.close();
                    }
                }
                catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
}
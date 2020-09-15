import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class RouteThread extends Thread {

    private static final String SERVER_IP = "210.107.197.172";
    private int SERVER_PORT;
    private msgEPartition message;

    public RouteThread(msgEPartition message, int SERVER_PORT){

        this.SERVER_PORT = SERVER_PORT;
        this.message = message;
    }

    @Override
    public void run(){

        Socket socket = null;

        try{

            socket = new Socket();

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

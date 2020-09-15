public class RoutingTable {

    int brokerID;
    int port;

    public RoutingTable(int brokerID, int port) {

        this.brokerID = brokerID;
        this.port = port;
    }

    public static int getSuccessor(int hashed) {

        if (hashed > 3 && hashed <= 7)
            return 7;
        else if (hashed > 7 && hashed <= 11)
            return 11;
        else if (hashed > 11 && hashed <= 15)
            return 15;
        else if (hashed > 15 && hashed <= 19)
            return 19;
        else if (hashed > 19 && hashed <= 23)
            return 23;
        else if (hashed > 23 && hashed <= 27)
            return 27;
        else if (hashed > 27 && hashed <= 31)
            return 31;
        else
            return 3;
    }
}
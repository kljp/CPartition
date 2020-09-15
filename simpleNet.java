import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class simpleNet {

    public static void main(String[] args) {

        System.out.println(getHashCode("10000000"));
        System.out.println(getHashCode("01000000"));
        System.out.println(getHashCode("00100000"));
        System.out.println(getHashCode("00010000"));
        System.out.println(getHashCode("00001000"));
        System.out.println(getHashCode("00000100"));
        System.out.println(getHashCode("00000010"));
        System.out.println(getHashCode("00000001"));

        System.out.println("-----------");

        System.out.println(getHashCode("00003300"));
        System.out.println(getHashCode("00004300"));
        System.out.println(getHashCode("00000043"));

    }

    public static int getHashCode(String str){ // This function is available under 9-segments.

//        int hashcode = 0;
//
//        for(char c : str.toCharArray())
//            hashcode += c;
//
//        hashcode %= GlobalState.NumberOfChordNode;
//
//
//        return hashcode;

        int temp = MurmurHash.hash32(str);
//        if(temp < 0)
//            temp *= -1;

        while(temp < 0)
            temp = MurmurHash.hash32(Integer.toString(temp));

        return  temp % GlobalState.NumberOfChordNode;
    }
}

public class GlobalState {

    public static final int NumberOfDimensions = 8;
    public static String[] attributes = new String[]{"attr_0", "attr_1", "attr_2", "attr_3", "attr_4", "attr_5", "attr_6", "attr_7", "attr_8"}; // This is the order of the attributes.
    public static String[] attributes2 = new String[]{"attr_0", "attr_1", "attr_2", "attr_3", "attr_4", "attr_5", "attr_6", "attr_7", "attr_8"}; // This is the order of the attributes for test.
    public static final double[] minimumBounds = new double[]{0, 0, 0, 0, 0, 0, 0, 0};
    public static final double[] maximumBounds = new double[]{180, 180, 180, 180, 180, 180, 180, 180};
    public static int NumberOfSegmentsPerDimension = 4;
    public static final String[] segmentIdentifier = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"}; // Currently, we support totally 15 identifiers except zero.
    public static int NumberOfDimensionGroups = 2;
    public static int NumberOfDimensionsPerGroup = NumberOfDimensions / NumberOfDimensionGroups;
    public static final boolean[] skewedSubscriptionDist = new boolean[]{true, true, true, true, false, false, false, false, true, true, true, true, false, false, false, false};
    public static int NumberOfSubscriptions = 1000;
    public static final String subscriptionListDir = "./src/messages/subscriptions/subscription ";
    public static int NumberOfBrokers = 8;
    public static int NumberOfChordNode = 32;
}
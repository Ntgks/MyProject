package ergasia;

import java.io.Serializable;
import java.util.ArrayList;

public class ValuePasser implements Serializable {

    private int client;
    Object passingObject;

    /*
    * action variable informs the receiver of manager/renter specific actions
    * 0: Worker and Client Connection Tests
    * 1: Manager wants to add room
    * 2: Manager wants to add Date
    * 3: Manager wants to retrieve Bookings for his Rooms
     */
    private int action;

    public ValuePasser(Object passingObject, int action, int client) {

        this.action = action;
        this.passingObject = passingObject;
        this.client = client;
    }

    public Object getObjectValues() {
        return this.passingObject;
    }
    public int getAction() { return this.action; }

    public int getClient() {return this.client; }


}

package ergasia;

import java.io.Serializable;
import java.time.LocalDate;

public class Dates implements Serializable {
    private LocalDate startDate, endDate;
    private String room_name;

    private String room_area;

    private int clientID;
    public Dates(LocalDate startDate, LocalDate endDate, String room_name, int clientID, String room_area) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.room_name = room_name;
        this.clientID = clientID;
        this.room_area = room_area;
    }

    public LocalDate getStartDate() {return this.startDate;}
    public LocalDate getEndDate() {return this.endDate;}

    public String getRoomName() {return this.room_name;}

    public String getRoomArea() {return this.room_area;}

    public void setClientID(int clientID) {this.clientID = clientID;}

    public int getClientID() {return this.clientID;}


    // Method to perform logic calculations
    public boolean isAvailableOn(LocalDate date, LocalDate availableFrom, LocalDate availableTo) {
        boolean available = !date.isBefore(availableFrom) && !date.isAfter(availableTo);
        if (available) {
            System.out.println("Date: " + date + " is between: " + availableFrom + " and " + availableTo);
        } else {
            System.out.println("Date: " + date + " is not between: " + availableFrom + " and " + availableTo);
        }
        return available;
    } //auth tha xreiastei gia otan psaxnei me filter


    public static void main(String args[]) {

    }


}

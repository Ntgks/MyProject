package ergasia;

import java.io.*;
import java.net.*;
import java.util.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDate;


public class Client extends Thread {
    int typeOfClient;
    String clientAction;

    ArrayList<String[]> Katalymata = new ArrayList<String[]>(); //kanei store katalymata
    ArrayList<String[]> toAddDates = new ArrayList<String[]>();
    ArrayList<Dates> dates = new ArrayList<Dates>();

    String[] jsonPaths = {
            "C:\\Users\\nickr\\IdeaProjects\\KatanemTest\\src\\ergasia\\Katalymata\\Katalyma_1.json",
            "C:\\Users\\nickr\\IdeaProjects\\KatanemTest\\src\\ergasia\\Katalymata\\basicroom.json"
    };
    public Client() {

    }

 
    public void run() {
        ObjectOutputStream out= null ;
        ObjectInputStream in = null ;
        Socket requestSocket= null ;
 
 
        try {
            String host = "localhost";
            /* Create socket for contacting the server on port 4321*/
            requestSocket = new Socket(host, 4321);
 
            /* Create the streams to send and receive data from server */
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            for (String i : jsonPaths) {
                jsonIterator(i);
            }

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter 1 for Manager, Enter 2 for Enoikiasths: ");
            typeOfClient = sc.nextInt();
            if (typeOfClient == 1) {
                manager(sc, out, in);
            } else {
                renter(sc, out, in);
            }



            //Test t = new Test(typeOfClient);


 
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        /*} catch (ClassNotFoundException e) {
            throw new RuntimeException(e);*/
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close(); out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void manager(Scanner sc, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
        while(true) {
            System.out.println("Enter Number 1 - 3 for the selected action");
            System.out.println("1: Add a new Residence");
            System.out.println("2: Add Rent Dates for an existing Residence");
            System.out.println("3: See your Bookings");
            System.out.println("4: See your Residences based on Date Input");
            System.out.println("5: Quit");
            int choice = sc.nextInt();

            if (choice == 5)
                break;

            switch (choice) {
                case 1:
                    System.out.println("Selected 1...");
                    System.out.println("Waiting for jsonParser...");

                    int roomcounter = 1;
                    System.out.println("You have these Rooms to be added");
                    for (String[] i : Katalymata) {
                        System.out.println(roomcounter + ": " + i[0]);
                        roomcounter++;
                    }
                    choice = sc.nextInt();
                    while(choice < 1 || choice >= roomcounter) {
                        System.out.println("Please select an existing number");
                        choice = sc.nextInt();
                    }


                    ValuePasser roomDetails = new ValuePasser(Katalymata.get(choice - 1), 1,0);
                    toAddDates.add(Katalymata.remove(choice - 1));
                    //System.out.println(toAddDates.get(0)[0]);
                    out.writeObject(roomDetails);
                    out.flush();


                    break;
                case 2:
                    System.out.println("Selected 2...");
                    if(!checkHouse()) {
                        break;
                    }
                    ValuePasser dateSend = new ValuePasser(dates.remove(0), 2,0);
                    out.writeObject(dateSend);
                    out.flush();
                    break;
                case 3:
                    System.out.println("Selected 3...");
                    seeBookings(out);

                    ValuePasser receiver = (ValuePasser) in.readObject(); //to pairnei epityxws

                    ArrayList<String[]> katalymata = (ArrayList<String[]>) receiver.getObjectValues();
                    System.out.println("Client> These are your Bookings");
                    int counter = 1;
                    for (String[] i : katalymata) {
                        System.out.println(counter + ": " + i[0] + " from Manager " + i[7]);
                        counter++;
                    }

                    break;
                case 4:
                    System.out.println("Selected 4...");
                    LocalDate[] dateFilter = addDates(3);
                    System.out.println("The dates you've selected are: " + dateFilter[0] + " , " + dateFilter[1]);

                    ValuePasser filter_by_date = new ValuePasser(dateFilter, 7, 0);
                    out.writeObject(filter_by_date);
                    out.flush();

                    receiver = (ValuePasser) in.readObject(); //to pairnei epityxws
                    HashMap<String, Integer> areas = (HashMap<String, Integer>) receiver.getObjectValues();
                    System.out.println("Output: ");
                    for (String area : areas.keySet()) {
                        System.out.println(area + ": " + areas.get(area));
                    }


                // more cases can be added as needed
                default:
                    // code block to be executed if expression doesn't match any case
            }
        }
    }

    private void seeBookings(ObjectOutputStream out) throws IOException {

        ValuePasser dateSend = new ValuePasser(null, 6,0);
        out.writeObject(dateSend);
        out.flush();

    }

    private void renter(Scanner sc, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object[] filters = null;
        LocalDate[] bookingDates = new LocalDate[2];
        while(true) {
            System.out.println("Enter Number 1 - 3 for the selected action");
            System.out.println("1: Filter Rooms");
            System.out.println("2: See Available Rooms");
            System.out.println("3: Rate a Residence");
            System.out.println("4: Quit");
            int choice = sc.nextInt();


            if (choice == 4)
                break;

            switch (choice) {
                case 1:
                    filters = createFilter(sc);
                    bookingDates[0] = (LocalDate) filters[1];
                    bookingDates[1] = (LocalDate) filters[2];
                    break;
                case 2:
                    boolean filterYN = (filters != null);
                    if (filterYN) {
                        System.out.print("Do you want to see Rooms based on filter? (Y/N): ");
                        if (Objects.equals(sc.next(), "Y")) {
                            System.out.println("Filter Selected, showing filtered rooms...");
                            search(filters, out);
                        } else {
                            System.out.println("No Filter Selected, showing all available rooms...");
                            filterYN = false;
                            search(null, out);
                        }
                    }







                    ValuePasser receiver = (ValuePasser) in.readObject(); //to pairnei epityxws

                    ArrayList<String[]> katalymata = (ArrayList<String[]>) receiver.getObjectValues();
                    System.out.println("Client> I got Rooms, Select Rooms");
                    int counter = 1;
                    for (String[] i : katalymata) {
                        System.out.println(counter + ": " + i[0] + " from Manager " + i[7]);
                        counter++;
                    }
                    //System.out.println("counter is: " + counter);
                    choice = sc.nextInt();

                    
                    while(choice < 1 || choice >= counter) {
                        System.out.println("Please select an existing number");
                        choice = sc.nextInt();
                    }
                    String[] katalyma = katalymata.get(choice - 1);
                    boolean exit = false;
                    int option = 0;
                    while(!exit) {
                        System.out.println("What would you like to do with room: " + katalymata.get(choice - 1)[0]);
                        System.out.println("1: Make a Booking");
                        System.out.println("2: Make a Rating");
                        System.out.println("3: See Details");
                        option = sc.nextInt();

                        while(option < 1 || option > 3) {
                            System.out.println("Please select an existing number");
                            choice = sc.nextInt();
                        }

                        if (option != 3) {
                            exit = true;
                        } else {
                            System.out.println("Room Name: " + katalyma[0]);
                            System.out.println("Number Of Persons: " + katalyma[1]);
                            System.out.println("Area: " + katalyma[2]);
                            System.out.println("Stars: " + katalyma[3]);
                            System.out.println("Number Of Reviews: " + katalyma[4]);
                            System.out.println("Price: " + katalyma[5]);
                            System.out.println("Room Image: " + katalyma[6]);
                        }
                    }







                    if (option == 2) {
                        addStars(katalyma, out);
                    } else if (option == 1) {
                        book(katalyma, out, filterYN, bookingDates);
                        int receiver1 = in.readInt();
                        if (receiver1 == 1) {
                            System.out.println("Booking Success");
                        } else {
                            System.out.println("There is already a booking with these dates");
                        }
                        /*ValuePasser receiver1 = (ValuePasser) in.readObject();
                        if ((int) receiver1.getObjectValues() == 1) {
                            System.out.println("Booking Success");

                        } else {
                            System.out.println("There is already a booking with these dates");
                        }*/

                    }



                    // TODO




                    break;
                case 3:
                    System.out.println("Selected 3...");
                    break;
                // more cases can be added as needed
                default:
                    // code block to be executed if expression doesn't match any case
            }


        }
    }

    private void search(Object[] filters, ObjectOutputStream out) throws IOException {

        ValuePasser request = new ValuePasser(filters, 3,0);
        out.writeObject(request);
        out.flush();
    }


    private void book(String[] katalyma, ObjectOutputStream out, boolean filterExists, LocalDate[] bookingDates) throws IOException {

        Dates katal;
        if (filterExists) {
            katal = new Dates(bookingDates[0], bookingDates[1], katalyma[0], Integer.parseInt(katalyma[7]), katalyma[2]);
        } else {
            LocalDate[] bookDates = addDates(3);
            katal = new Dates(bookDates[0], bookDates[1], katalyma[0], Integer.parseInt(katalyma[7]), katalyma[2]);
        }

        /*LocalDate[] bookDates = addDates(3);
        Dates katal = new Dates(bookDates[0], bookDates[1], katalyma[0], Integer.parseInt(katalyma[7]), katalyma[2]);*/

        //Dates katal = new Dates()
        ValuePasser request = new ValuePasser(katal, 5,0);
        out.writeObject(request);
        out.flush();





    }

    //Makes a Rating and then sends the room with the new rating into the stream
    private void addStars(String[] katalyma, ObjectOutputStream out) throws IOException {
        Scanner sc = new Scanner(System.in);
        System.out.println("Select 1-5 Stars:");
        int choice = sc.nextInt();
        while(choice < 1 || choice > 5) {
            System.out.println("Please select an existing number");
            choice = sc.nextInt();
        }
        //System.out.println("You rated " + choice + " Stars for residence: " + katalyma[0]);
        katalyma[8] = String.valueOf(choice);
        System.out.println("You rated " + katalyma[8] + " Stars for residence: " + katalyma[0]);


        ValuePasser sender = new ValuePasser(katalyma, 4, Integer.parseInt(katalyma[7]));
        out.writeObject(sender);
        out.flush();

    }

    /*
     * filter[0] = Area
     * filter[1] = Start Date
     * filter[2] = End Date
     * filter[3] = AmountOfPeople
     * filter[4] = Min Price
     * filter[5] = Max Price
     * filter[6] = Stars
     *
     */
    private Object[] createFilter(Scanner sc) {
        Object[] filter = new Object[7];
        System.out.println("Select Room Filters. Typing 0 means NO filter");
        System.out.print("Select Area: ");
        filter[0] = sc.next();
        System.out.println("Select Dates");
        LocalDate[] dates = addDates(2);
        filter[1] = dates[0];
        filter[2] = dates[1];
        System.out.println("Select Amount of People (single or double digit): ");
        filter[3] = sc.next();
        System.out.println("Select Price Range");
        System.out.println("Min Price: ");
        filter[4] = sc.nextInt();
        System.out.println("Max Price: ");
        filter[5] = sc.nextInt();
        System.out.println("Select Stars (1-5): ");
        filter[6] = sc.nextInt();


        return filter;
    }
    //Adding the Dates of existing Rooms
    private LocalDate[] addDates(int actionType) {
        Scanner sc = new Scanner(System.in);
        int choice = 0;
        LocalDate[] datesForRent = new LocalDate[2];
        if (actionType == 1) {
            System.out.println("For which residence would you like to make changes?");
            for (int i = 0; i < toAddDates.size(); i++) {
                System.out.println((i + 1) + ": " + toAddDates.get(i)[0]);
            }
            System.out.println("Select a Number");
            choice = sc.nextInt();
            while (choice > toAddDates.size() || choice <= 0) {
                System.out.println("Choose a correct Number");
                choice = sc.nextInt();
            }
            System.out.println("Selected room: " + toAddDates.get(choice - 1)[0]);

        }


        System.out.println("Date Input must be in form of: dd/mm/yyyy");
        System.out.println("Enter first Date: ");
        String inputString = sc.next();

        while (isDateValid(inputString) == null) {
            inputString = sc.next();
        }

        LocalDate firstDate = isDateValid(inputString);
        System.out.println("Correct First Date: " + inputString);
        if (actionType != 1) {
            datesForRent[0] = firstDate;
        }

        System.out.println("Enter second Date: ");
        inputString = sc.next();
        LocalDate checker = isDateValid(inputString);
        //System.out.println(firstDate.isBefore(isDateValid(inputString)));
        while (checker == null || !firstDate.isBefore(checker)) {
            if (checker == null) {
                System.out.println("Enter Second Date: ");
                inputString = sc.next();
                checker = isDateValid(inputString);
            } else {

                if (checker == null) {
                    break;
                }

                if (!(firstDate.isBefore(checker))) {
                    System.out.println("Second Date before or same as First Date...");
                    System.out.println("Enter Second Date: ");
                }
                inputString = sc.next();
                checker = isDateValid(inputString);
            }

        }

        LocalDate secondDate = isDateValid(inputString);
        System.out.println("Correct Second Date: " + inputString);
        if (actionType != 1) {
            datesForRent[1] = secondDate;
        }

        /*dates.add(new Dates(firstDate, secondDate,
                    toAddDates.get(choice - 1)[0],
                    Integer.parseInt(toAddDates.get(choice - 1)[7],
                            Integer.parseInt(toAddDates.get(choice - 1)[2]))));

         */

        if (actionType == 1) {
            dates.add(new Dates(firstDate, secondDate, toAddDates.get(choice - 1)[0],
                    Integer.parseInt(toAddDates.get(choice - 1)[7]), toAddDates.get(choice - 1)[2]));
        } else {
            return datesForRent;
        }


        return null;




    }

    //Splits json file into key - value and sends the file in the socket
    private void jsonIterator(String fileName) {
        //fileName = "C:\\Users\\nickr\\IdeaProjects\\KatanemTest\\src\\ergasia\\Katalymata\\Katalyma_1.json"; // Change this to your file path
        String[] values = new String[9]; // Array to store the extracted values

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains(":")) { // Check if the line contains a key-value pair
                    String[] parts = line.split(":", 2); // Limit split to only 2 parts
                    if (parts.length > 1) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        if (value.startsWith("\"") && value.endsWith("\"")) {
                            value = value.substring(1, value.length() - 1); // Remove quotes
                        }
                        if (key.equals("\"roomImage\"")) {
                            values[index++] = value;
                        } else {
                            values[index++] = value;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        values[0] = values[0].replaceAll("\"", "");
        //System.out.println(values[0]); // Output will be without "" marks

        values[2] = values[2].replaceAll("\"", "");
        //System.out.println(values[2]); // Output will be without "" marks

        values[7] = "0"; //Client ID
        values[8] = "0"; //Stars to pass

        for (int i = 0; i < 6; i++) {
            values[i] = values[i].substring(0, values[i].length() - 1); //removes the comma at the end of the string
        }

        // Print the extracted values
        /*for (String value : values) {
            System.out.println(value);
        }*/

       /*       "roomName": "Basic Room",
                "noOfPersons": 1,
                "area": "Area1",
                "stars": 1,
                "noOfReviews": 5,   x = roomValue[3] * roomValue[4], roomValue[3] = (x + roomValue[8]) / (roomValue[4] + 1)
                "price": 100,
                "roomImage":*/

        Katalymata.add(values);
        //System.out.println("Residence with Room Name: " + values[0] + " has been added successfully!!!");


    }
    //Sends the room details into the socket

    //Checks if there are Available Rooms to add dates
    private boolean checkHouse() {
        if (toAddDates.isEmpty()) {
            System.out.println("You don't have any available Residencies to add Dates");
            return false;
        } else {
            addDates(1);
            return true;
        }
    }

    //Checks if the Date is of correct input (mm/dd/yyyy - mm/dd/yyyy)
    private LocalDate isDateValid(String userInput) {
        // Regular expression pattern to match "dd/mm/yyyy" format
        String regex = "\\s*(\\d{1,2})/(\\d{1,2})/(\\d{4}\\s*)";

        // Create a Pattern object
        Pattern pattern = Pattern.compile(regex);

        // Create a Matcher object
        Matcher matcher = pattern.matcher(userInput);

        // Check if the input matches the pattern
        if(matcher.matches()) {
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            String group3 = matcher.group(3);
            if (Objects.equals(group1, "08")) {
                group1 = "8";
            } else if (Objects.equals(group1, "09")) {
                group1 = "9";
            }
            int day = Integer.parseInt(group1);
            int month = Integer.parseInt(group2);
            int year = Integer.parseInt(group3);
            //int counter = 0; //Checks 2 times when adding dates

            if (year < 2024 || year > 9999) {
                System.out.println("Incorrect Input. Input a Correct Year...");
                return null;
            }

            // Check if the month is valid (1 to 12)
            if (month < 1 || month > 12) {
                System.out.println("Incorrect Input. Input a Correct Month...");
                return null;
            }

            if (day < 1 || day > 31) {
                System.out.println("Incorrect Input. Input a Correct Day...");
                return null;
            }

            return dayMatchesYear(LocalDate.of(year,month,day));

        }
        System.out.println("Incorrect Input. Date Input must be in form of: dd/mm/yyyy");
        return null;

    }

    //Check if the last day of the month is 31,30 or 28 depending on the year
    private LocalDate dayMatchesYear(LocalDate date) {
        int day = date.getDayOfMonth();


        // Check if the day is valid for the given month and year
        if (day > date.lengthOfMonth()) {
            System.out.println("Incorrect Input. Input a Correct Day...");
            return null;
        }

        return date;
    }

    private void mapReduce(Object key, Object value) {

    }

    public static void main(String [] args) {

        Client th = new Client();
        th.start(); //San orisma tha pairnei an einai manager h enoikiasths
        //new Client(0).start();
    }
}
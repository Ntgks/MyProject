package ergasia;

import java.io.*;
import java.net.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;

//Worker Thread pou tha xeirizontai dedomena
public class Worker extends Thread {

    private ArrayList<Thread> threadlist;
    private int fatherID;

    protected ObjectInputStream in;
    private int id;

    protected ObjectOutputStream out;


    /* katalymata[i][x] [i] - >
    * [i] -> [1...length] = to katalyma (String[])
    * [x] -> [0] = roomName, [1] = noOfPersons, [2] = area, [3] = stars, [4] = NoOfreviews, [5] = roomImage
     */
    protected ArrayList<String[]> katalymata;
    protected ArrayList<Dates> dates;

    protected ValuePasser valueObject;
    private ArrayList<String[]> bookings;
    private ArrayList<Dates> bookingDates;



    public Worker(int id, ObjectOutputStream out, int fatherID, ArrayList<Dates> dates,
                  ArrayList<String[]> katalymata, ValuePasser valueObject,
                  ArrayList<String[]> bookings, ArrayList<Thread> threadlist, ArrayList<Dates> bookingDates) {
        this.id = id;
        this.out = out;
        this.fatherID = fatherID;
        if (id <= 100) {
            this.katalymata = new ArrayList<String[]>();
            this.dates = new ArrayList<Dates>();
            this.bookings = new ArrayList<String[]>();
            this.threadlist = new ArrayList<Thread>();
            this.bookingDates = new ArrayList<Dates>();

        } else {
            this.dates = dates;
            this.katalymata = katalymata;
            this.valueObject = valueObject;
            this.bookings = bookings;
            this.threadlist = threadlist;
            this.bookingDates = bookingDates;
        }


    }

    public int getTID() {
        return this.id;
    }

    public void setTID(int id) {
        this.id = id;
    }

    public int getFID() {
        return this.fatherID;
    }

    public void setFID(int fatherID) {
        this.fatherID = fatherID;
    }


    public void run() {

        if (id == -1) {

            while(true && !threadlist.isEmpty()) {
                if (!threadlist.get(0).isAlive()) {
                    System.out.println("Thread has died");
                }
            }

        }

        else if (id > 100) { //Thread that handles alla pragmata
            //ValuePasser receiver;
            System.out.println("Worker "+ getTID() +"> Thread Started");
            if (valueObject.getAction() == 1) {
                String[] roomValues = (String[]) valueObject.getObjectValues();
                System.out.println("Worker "+ getTID() +"> I got room: " + roomValues[0]);
                katalymata.add(roomValues);



            } else if (valueObject.getAction() == 2) {
                Dates date = (Dates) valueObject.getObjectValues();
                System.out.println("Worker "+ getTID() +"> I got Dates for Room: " + date.getRoomName() + " and manager " + date.getClientID());
                dates.add(date);
                // TODO check if clientID orizetai swsta


            } else if (valueObject.getAction() == 3) {


                ArrayList<String[]> newkatal = new ArrayList<String[]>();
                System.out.println("Worker "+ getTID() +"> Got request for rooms, sending rooms");
                if (valueObject.getObjectValues() == null) {
                    System.out.println("Worker "+ getTID() +"> We dont got filters...");
                    System.out.println("Worker "+ getTID() +"> Im gonna send: " + katalymata.size() + " rooms");
                    System.out.println("Worker "+ getTID() +"> I got rooms: " + katalymata);
                    newkatal.addAll(katalymata);
                } else {
                    System.out.println("Worker "+ getTID() +"> We got filters!!!");
                    ArrayList<String[]> rooms = filter((Object[]) valueObject.getObjectValues());
                    newkatal.addAll(rooms);
                }


                ValuePasser katal = new ValuePasser(newkatal, 3, getTID());
                System.out.println("Worker "+ getTID() +"> " + katal.getObjectValues());
                try {
                    out.writeObject(katal);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }



            } else if (valueObject.getAction() == 4) {
                String[] roomValues = (String[]) valueObject.getObjectValues();
                for (String[] i : katalymata) {
                    if (Objects.equals(i[0], roomValues[0]) && Objects.equals(i[7], roomValues[7])) {


                        float totalStars = (Float.parseFloat(i[3]) * Float.parseFloat(i[4]));
                        i[4] = String.valueOf(Float.parseFloat(i[4]) + 1);


                        System.out.println("Worker "+ getTID() +"> " + totalStars);
                        i[3] = String.valueOf((totalStars + Float.parseFloat(roomValues[8])) / Float.parseFloat(i[4]));


                        float x = Float.parseFloat(i[3]);
                        float roundedX = Math.round(x * 10.0f) / 10.0f;
                        i[3] = String.format("%.1f", roundedX);
                        //System.out.println(i[3]); // Output will be 1.7


                        System.out.println("Worker "+ getTID() +"> new Rating for room " + i[0] + " is: " + i[3]);
                    }
                }
            } else if (valueObject.getAction() == 5) {
                Dates roomValues = (Dates) valueObject.getObjectValues();
                //System.out.println("Worker "+ getTID() +"> Room sent: " + roomValues.getRoomName() + ", Room here: " + katalymata.get(0)[0]);
                //System.out.println("Worker "+ getTID() +"> Client sent: " + roomValues.getClientID() + ", Client here: " + katalymata.get(0)[7]);
                for (String[] i : katalymata) {
                    if (Objects.equals(i[0], roomValues.getRoomName()) && Integer.parseInt(i[7]) == roomValues.getClientID()) {
                        System.out.println("Worker "+ getTID() +"> New Booking for room " + i[0] + " on manager " + i[7]);
                        System.out.println("Worker "+ getTID() +"> On dates: " + roomValues.getStartDate() + ", " + roomValues.getEndDate());
                        bookings.add(i);
                        bookingDates.add(roomValues);
                    }
                }

            } else if (valueObject.getAction() == 6) {
                int manager = valueObject.getClient();
                ArrayList<String[]> bookingsReturn = new ArrayList<String[]>();

                for (String[] i : bookings) {
                    if (Objects.equals(i[7], String.valueOf(manager))) {
                        bookingsReturn.add(i);

                    }
                }

                ValuePasser returner = new ValuePasser(bookingsReturn, 3, getTID());
                System.out.println("Worker "+ getTID() +"> " + returner.getObjectValues());
                try {
                    out.writeObject(returner);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            } else if (valueObject.getAction() == 7) {

                int manager = valueObject.getClient();
                LocalDate[] dates2 = (LocalDate[]) valueObject.getObjectValues();
                ArrayList<String> toreturn_areas = new ArrayList<String>();
                for (Dates i : dates) {
                    if (i.isAvailableOn(i.getStartDate(), dates2[0], dates2[1])
                            && i.isAvailableOn(i.getEndDate(), dates2[0], dates2[1]) && i.getClientID() == manager) {
                                System.out.println(i.getRoomName());
                                toreturn_areas.add(i.getRoomArea());
                    }
                }
                System.out.println("Worker "+ getTID() +"> Printing Areas from Filter");
                System.out.println(toreturn_areas);
                ValuePasser returner = new ValuePasser(toreturn_areas, 7, getTID());
                try {
                    out.writeObject(returner);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            }
        }
        //System.out.println("Worker " + getFID() + ">" + valueObject.getAction());
        else {
            ObjectOutputStream out= null ;
            ObjectInputStream in = null ;
            Socket requestSocket= null ;

            //System.out.println("Thread: " + getId() + " is up");
            //System.out.println("Worker Thread: " + getTID() + " is up");
            String host = "localhost";

            try {
                /* Create socket for contacting the server on port 4321*/
                requestSocket = new Socket(host, 4322);



                /* Create the streams to send and receive data from server */
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                in = new ObjectInputStream(requestSocket.getInputStream());
                // Create an object to send
                //Test testObject = new Test(10);

                // Send the object to ActionsForWorkers
                //out.writeObject(testObject);
                //out.flush();


                ValuePasser receiver = (ValuePasser) in.readObject();
                //Action must be 0, no test needed
                String answer = (String) receiver.getObjectValues();
                //System.out.println(answer);
                if (Objects.equals(answer, "Close")) {
                    System.out.println("Worker Connection Limit has been reached. Can't connect...");
                } else {
                    ValuePasser workerID = new ValuePasser(this.getTID(),0, 0);
                    out.writeObject(workerID);
                    out.flush();


                    /*for (int i = 0; i < 4; i++) {

                        Worker workthread = new Worker(i + 101, out, in, fatherID, dates, katalymata, valueObject);
                        threadslist.add(workthread);
                        workthread.start();

                    }*/
                    Worker th1 = new Worker(-1, null, getFID(), null, null, null, null, threadlist, null);
                    th1.start();
                    int counter = 1;
                    while(true) {

                        if (!threadlist.isEmpty()) {
                            Worker th = (Worker) threadlist.get(0);
                            th.join();

                            threadlist.remove(0);
                            System.out.println("Worker "+ getTID() +"> " + th.getTID() + " Thread has been removed");
                        }
                        System.out.println("Worker "+ getTID() +"> Im waiting for object");
                        receiver = (ValuePasser) in.readObject(); //to pairnei epityxws
                        Worker th = new Worker(100 + counter, out, fatherID, dates, katalymata, receiver, bookings, null, bookingDates);
                        threadlist.add(th);
                        th.start();
                        counter++;



                        // TO start thread, then do action then send stuff back then send to reducer.

                    }
                }





            /*while(true) {
                ValuePasser receiver = (ValuePasser) in.readObject(); //to pairnei epityxws
                String[] roomValues = (String[]) receiver.getObjectValues();
                System.out.println("Room with name: " + roomValues[0] + " has been successfully " +
                        "been passed to Worker");
                katalymata.add(roomValues);
                for (String[] i : katalymata) {
                    if (katalymata.size() > 1) {
                        System.out.println(i[0]);
                    }
                }
            }*/

            } catch (UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch (IOException ioException) {
                ioException.printStackTrace();
        /*} catch (ClassNotFoundException e) {
            throw new RuntimeException(e);*/
            } catch (ClassNotFoundException | InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                    if (out != null) {
                        out.close();
                    }
                    if (requestSocket != null) {
                        requestSocket.close();
                    }

                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }

    }

    private ArrayList<String[]> filter(Object[] objectValues) {
        ArrayList<String[]> rooms = new ArrayList<String[]>();
        for (String[] i : katalymata) {
            int filterLevel = 0;
            //checks for area, numOfPeople, stars
            /*System.out.println((String) objectValues[0]);
            System.out.println(i[2]);
            System.out.println((String) objectValues[3]);*/
            if (Objects.equals((String) objectValues[0], i[2]) && Objects.equals((String) objectValues[3], i[1])
                    && (int) objectValues[6] == Integer.parseInt(i[3])) {
                    System.out.println("Worker "+ getTID() +"> Room: " + i[0] + " matches filter for Area, NumOfPeople and Stars");
                    filterLevel++;
            } else {
                continue;
            }


            //checks for min, max prise
            if (Integer.parseInt(i[5]) >= (int) objectValues[4] && Integer.parseInt(i[5]) <= (int) objectValues[5]) {
                System.out.println("Worker "+ getTID() +"> Room: " + i[0] + " matches filter for Price");
                filterLevel++;
            } else {
                continue;
            }

            //gia veltistopoihsh kane thn dates hashmap
            for (Dates j : dates) {
                if (Objects.equals(j.getRoomName(), i[0])) {
                    if (j.isAvailableOn(((LocalDate) objectValues[1]), j.getStartDate(), j.getEndDate())
                            && j.isAvailableOn(((LocalDate) objectValues[2]), j.getStartDate(), j.getEndDate())) {


                        System.out.println("Worker "+ getTID() +"> Room: " + i[0] + " matches filter for Dates");
                        filterLevel++;


                    }
                }
            }

            if (filterLevel == 3) {
                rooms.add(i);
            }


        }
        return rooms;
    }


    public static void main(String[] args) {

        Random random = new Random();
        int randomNumber = random.nextInt(100);
        new Worker(randomNumber, null, randomNumber, null, null, null, null, null, null).start();



    }
}

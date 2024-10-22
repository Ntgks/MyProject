package ergasia;

import com.sun.jdi.Value;

import java.io.*;
import java.net.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

//Actions For Clients = Polynhmatikos Master ws pros Clients. Dhladh Syndeontai osoi Clients theloun

 
public class ActionsForClients extends Thread {
ObjectInputStream in;
ObjectOutputStream out;

ArrayList<Object> queue;
//ArrayList<String[]> queue2;
ArrayList<ActionsForClients> clients;

ArrayList<ActionsForWorkers> workers;

int numOfWorkers;

int afc_ID;

String[] hashPasser = new String[2]; //krataei to nodeID pou prepei na xrhsimopoieithei apo worker kai to idio to ID tou client



    public ActionsForClients(Socket connection, ArrayList<Object> queue, ArrayList<ActionsForWorkers> workers, int numOfWorkers,ArrayList<ActionsForClients> clients) {
        this.queue = queue;
        this.workers = workers;
        this.numOfWorkers = numOfWorkers;
        this.clients = clients;
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());




        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {

        //System.out.println("Thread with id: "+getId()+ " started!");
        System.out.println("ActionsForClients> new ActionsForClients Thread Started");
        clients.add(this);
        afc_ID = clients.size();
        System.out.println("ActionsForClients> A Client communicates with ClientThreadID:  " + afc_ID);

        try {

            while(true) {

                System.out.println("ActionsForClients> I am waiting...");
                ValuePasser receiver = (ValuePasser) in.readObject(); //to pairnei epityxws
                System.out.println("ActionsForClients> I got Object");
                if (receiver.getAction() == 1) { //Sets the action to: Manager Room ADD
                    String[] roomValues = (String[]) receiver.getObjectValues();
                    //int nodeID = hashFunction(receiver.getRoomValues()[0]);
                    //System.out.println(nodeID);
                    int nodeID = hashFunction(roomValues[0]);
                    //int nodeID = hashFunction2(afc_ID);
                    synchronized (queue) {
                        queue.set(0, 1); //Sets the action to: Manager Room ADD
                        queue.set(5, afc_ID); //Sets the client ID
                        roomValues[7] = String.valueOf(afc_ID);
                        System.out.println("ActionsForClients> " + roomValues[7] + " is the clientID");
                        queue.set(1, roomValues); //puts room into the queue
                        queue.set(2, nodeID); //puts Worker ID into the queue
                        System.out.println("ActionsForClients> " + roomValues[0] + " added to queue!");
                        queue.notifyAll();
                    }
                } else if (receiver.getAction() == 2) { //Action: Manager Date for room ADD
                    Dates date = (Dates) receiver.getObjectValues();
                    int nodeID = hashFunction(date.getRoomName());

                    synchronized (queue) {
                        queue.set(0, 2); //Sets the appropriate Action
                        queue.set(5, afc_ID); //Sets the Client ID
                        date.setClientID(afc_ID); //Sets Client ID
                        queue.set(3, date); //Adds the Date
                        queue.set(4, nodeID);
                        System.out.println("ActionsForClients> Dates for room:  " + date.getRoomName()  + " have been added to queue!");
                        queue.notifyAll();
                        try {
                            queue.wait(); //<-- here we wait for the consumer to consume the number first!
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                } else if(receiver.getAction() == 3) {
                    clients.remove(this);
                    synchronized (queue) {
                        queue.set(0, 3);
                        queue.set(8, receiver.getObjectValues());
                        System.out.println("ActionsForClients> Request from Renter to show rooms");
                        //int waitForWorkers = workers.size();
                        //ArrayList<String[]> rooms;
                        queue.notifyAll();
                        try {
                            queue.wait(); //<-- here we wait for the consumer to consume the number first!
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                        //while (!workers.isEmpty()) {}

                        ArrayList<String[]> rooms = (ArrayList<String[]>) queue.get(6);
                        System.out.println("ActionsForClients> I got " + rooms + " from the request");
                        queue.set(6, null);



                        ValuePasser tosend = new ValuePasser(rooms, 3, 0);
                        out.writeObject(tosend);
                        out.flush();

                    }



                } else if (receiver.getAction() == 4) {
                    String[] roomValues = (String[]) receiver.getObjectValues();
                    System.out.println(roomValues[0]);
                    int nodeID = hashFunction(roomValues[0]); //Worker Node ID
                    System.out.println("Worker ID is: " + nodeID + "and rating is: " + roomValues[8]);

                    synchronized (queue) {
                        queue.set(0, 4);
                        System.out.println("ActionsForClients> Sending Star Rating to residence");
                        queue.set(1, roomValues); //Sends the katalyma
                        queue.set(2, nodeID); //Sends the Worker ID
                        //queue.set(5, afc_ID); //Sends the client ID
                        queue.notifyAll();

                    }
                } else if (receiver.getAction() == 5) {
                    Dates roomValues = (Dates) receiver.getObjectValues();
                    int nodeID = hashFunction(roomValues.getRoomName()); //Worker Node ID
                    synchronized (queue) {
                        queue.set(0, 5);
                        System.out.println("ActionsForClients> Sending -Book Room- Request to residence");
                        queue.set(1, roomValues); //Sends the katalyma
                        queue.set(2, nodeID); //Sends the Worker ID
                        int bookStatus = 0;
                        if (queue.get(7) == null && queue.get(9) == null) {

                            queue.set(7, roomValues.getStartDate());
                            queue.set(9, roomValues.getEndDate());
                            System.out.println("ActionsForClients> Im going to sleep");
                            bookStatus = 1;
                            //sleep(5000);
                            System.out.println("ActionsForClients> I woke up from sleep");
                            queue.notifyAll();

                        } else if (queue.get(7) == roomValues.getStartDate() && queue.get(9) == roomValues.getEndDate()) {
                            bookStatus = 2;
                            System.out.println("ActionsForClients> There is already a booking with these dates");

                        }

                        /*ValuePasser sender = new ValuePasser(bookStatus, 5, 0);
                        out.writeObject(sender);
                        out.flush();*/

                        out.writeInt(bookStatus);
                        out.flush();




                        //queue.set(5, afc_ID); //Sends the client ID

                    }
                } else if (receiver.getAction() == 6) {
                    synchronized (queue) {
                        queue.set(0, 6);
                        queue.set(5, afc_ID);
                        System.out.println("ActionsForClients> Sending -See Bookings- Request to Worker");
                        queue.notifyAll();
                        try {
                            queue.wait(); //<-- here we wait for the consumer to consume the number first!
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                        //while (!workers.isEmpty()) {}

                        ArrayList<String[]> rooms = (ArrayList<String[]>) queue.get(6);
                        System.out.println("ActionsForClients> I got " + rooms + " from the request");
                        queue.set(6, null);



                        ValuePasser tosend = new ValuePasser(rooms, 6, 0);
                        out.writeObject(tosend);
                        out.flush();
                    }

                } else if (receiver.getAction() == 7) {
                    synchronized (queue) {
                        queue.set(0, 7);
                        queue.set(8, receiver.getObjectValues());
                        queue.set(5, afc_ID);
                        System.out.println("ActionsForClients> Request from Manager to get areas on Date");
                        //int waitForWorkers = workers.size();
                        //ArrayList<String[]> rooms;
                        queue.notifyAll();
                        try {
                            queue.wait(); //<-- here we wait for the consumer to consume the number first!
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                        //while (!workers.isEmpty()) {}

                        ArrayList<String> areas = (ArrayList<String>) queue.get(6);

                        HashMap<String, Integer> areaCounts = new HashMap<>();

                        // Count occurrences of each area
                        for (String area : areas) {
                            areaCounts.put(area, areaCounts.getOrDefault(area, 0) + 1);
                        }


                        // Print the counts
                        for (String area : areaCounts.keySet()) {
                            System.out.println(area + ": " + areaCounts.get(area));
                        }

                        System.out.println("ActionsForClients> I got " + areaCounts + " from the request");
                        queue.set(6, null);


                        ValuePasser tosend = new ValuePasser(areaCounts, 7, 0);
                        out.writeObject(tosend);
                        out.flush();

                    }
                }



            }







        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);// need it in readObject 
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private int hashFunction(String roomName) {
        int hash = 0;
        for (char c : roomName.toCharArray()) {
            hash += (int) c;
        }

        // Calculate the node ID using modulo operation
        return (hash % workers.size()) + 1;
        //workers.size() gia dynamikh xrhsh, eksartatai apo ton arithmo ton workers pou exoun syndethei
        //numOfWorkers metavlhth gia to epitrepomeno orio opws orizetai apo ekfwnhsh
    }

    private int hashFunction2(int afc_ID) {
        return 1;
    }


}



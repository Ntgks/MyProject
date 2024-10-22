package ergasia;

import java.io.*;
import java.net.*;
import java.util.ArrayDeque;
import java.util.ArrayList;

public class ActionsForWorkers extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;

    //This queue is used for the communication between the server threads
    ArrayList<Object> queue;

    ArrayList<ActionsForWorkers> workers;

    //Number of Worker Threads to be created (it may be useless)
    int numOfWorkers;

    int afw_ID;

    boolean canOpen;

    public ActionsForWorkers(Socket connection, int numOfWorkers,  ArrayList<Object> queue, ArrayList<ActionsForWorkers> workers, boolean canOpen) {
        this.numOfWorkers = numOfWorkers;
        this.queue = queue;
        this.workers = workers;
        this.canOpen = canOpen;
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {



        if (!canOpen) {
            try {
                ValuePasser sender = new ValuePasser("Close", 0, 0);
                out.writeObject(sender);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else {

            System.out.println("ActionsForWorkers> new ActionsForWorkers Thread Started");

            //System.out.println("Thread with id: "+getId()+ " started!");
            //System.out.println("WORKER MACHINE CONNECTED");


            try {

                ValuePasser sender = new ValuePasser("Open", 0, 0);
                out.writeObject(sender);
                out.flush();

                ValuePasser receiver = (ValuePasser) in.readObject(); //to pairnei epityxws
                int workerID = (Integer) receiver.getObjectValues();
                System.out.println("ActionsForWorkers> A Worker has connected with initialID: " + workerID);
                workers.add(this);
                afw_ID = workers.size();
                System.out.println("ActionsForWorkers> A Worker communicates with WorkerThreadID:  " + afw_ID);



                while(true) {
                    synchronized(queue) {
                        int activeWorkers = workers.size();
                        System.out.println("ActionsForWorkers Thread " + afw_ID + "> is waiting");
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                        System.out.println("ActionsForWorkers Thread " + afw_ID + "> Action is " + queue.get(0));
                        if ((int) queue.get(0) == 1){
                            String[] roomDetails = (String[]) queue.get(1);
                            int nodeID = (int) queue.get(2);
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> Room must be passed to ActionsForWorkers Thread " + nodeID);
                            if (nodeID == this.afw_ID) {
                                queue.set(2, 0);
                                //String[] hashPasser = queue.pop();
                                //int nodeID = Integer.parseInt(hashPasser[0]);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Successfully transferred roomDetails for room: " + roomDetails[0]);
                                //queue.notifyAll();

                                ValuePasser topass_roomDetails = new ValuePasser(roomDetails, 1, Integer.parseInt(roomDetails[7]));
                                out.writeObject(topass_roomDetails);
                                out.flush();
                            }
                        } else if ((int) queue.get(0) == 2) {
                            Dates date = (Dates) queue.get(3);
                            int nodeID = (int) queue.get(4);
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> Dates must be passed to ActionsForWorkers Thread " + nodeID);
                            if (nodeID == this.afw_ID) {
                                queue.set(4, 0);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Successfully transferred Room Dates for room: " + date.getRoomName());
                                queue.notifyAll();

                                ValuePasser topass_date = new ValuePasser(date, 2, date.getClientID());
                                out.writeObject(topass_date);
                                out.flush();

                            }
                        } else if ((int) queue.get(0) == 3) {

                            ValuePasser request = new ValuePasser(queue.get(8), 3, 0);
                            out.writeObject(request);
                            out.flush();

                            ValuePasser receiver123 = (ValuePasser) in.readObject();
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> I got rooms: " + receiver123.getObjectValues() + "From Worker " + receiver123.getClient());

                            //synchronized (queue) {
                                if (queue.get(6) == null) {
                                    //queue.set(0, 0);
                                    queue.set(6, receiver123.getObjectValues());
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> First thread here");
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                    workers.remove(0);
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());

                                } else {
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> After First thread ");
                                    ArrayList<String[]> rooms = (ArrayList<String[]>) queue.get(6);
                                    rooms.addAll((ArrayList<String[]>) receiver123.getObjectValues());
                                    queue.set(6, rooms);
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                    workers.remove(0);
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());
                                    //think of code for more actionsforworker threads ginetai ths poutanas me ta casts
                                }

                                if (workers.isEmpty()) {
                                    System.out.println("ActionsForWorkers Thread " + afw_ID + "> mphke edw");
                                    //workers.add(this);
                                    //workers.add(this);
                                    for (int i = 0; i < activeWorkers; i++) {
                                        workers.add(this);
                                    }
                                    queue.set(0, 0);

                                    queue.notifyAll();
                                }


                                //queue.notifyAll();

                            //}


                        } else if( (int) queue.get(0) == 4) {
                            String[] roomDetails = (String[]) queue.get(1);
                            int nodeID = (int) queue.get(2);
                            if (nodeID == this.afw_ID) {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Rating is: " + roomDetails[8]);
                                ValuePasser rating = new ValuePasser(roomDetails, 4, 0);
                                out.writeObject(rating);
                                out.flush();

                            }
                        } else if( (int) queue.get(0) == 5) {

                            Dates roomDetails = (Dates) queue.get(1);
                            int nodeID = (int) queue.get(2);
                            if (nodeID == this.afw_ID) {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Sending Book Request to Worker");
                                ValuePasser bookrequest = new ValuePasser(roomDetails, 5, 0);
                                out.writeObject(bookrequest);
                                out.flush();
                            }

                        } else if( (int) queue.get(0) == 6) {
                            int manager = (int) queue.get(5);
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> Sending Book Request to Worker for manager: " + manager);

                            ValuePasser request = new ValuePasser(null, 6, manager);
                            out.writeObject(request);
                            out.flush();

                            ValuePasser receiver123 = (ValuePasser) in.readObject();
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> I got rooms: " + receiver123.getObjectValues());


                            if (queue.get(6) == null) {

                                queue.set(6, receiver123.getObjectValues());
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> First thread here");
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                workers.remove(0);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());

                            } else {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> After First thread ");
                                ArrayList<String[]> rooms = (ArrayList<String[]>) queue.get(6);
                                rooms.addAll((ArrayList<String[]>) receiver123.getObjectValues());
                                queue.set(6, rooms);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                workers.remove(0);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());
                                //think of code for more actionsforworker threads ginetai ths poutanas me ta casts
                            }

                            if (workers.isEmpty()) {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> mphke edw");
                                //workers.add(this);
                                //workers.add(this);
                                for (int i = 0; i < activeWorkers; i++) {
                                    workers.add(this);
                                }
                                queue.set(0, 0);

                                queue.notifyAll();
                            }



                        } else if( (int) queue.get(0) == 7) {
                            int manager = (int) queue.get(5);
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> Sending DATE INPUT request from Manager: " + manager);

                            ValuePasser request = new ValuePasser(queue.get(8), 7, manager);
                            out.writeObject(request);
                            out.flush();

                            ValuePasser receiver123 = (ValuePasser) in.readObject();
                            System.out.println("ActionsForWorkers Thread " + afw_ID + "> Areas Based on Dates: " + receiver123.getObjectValues());

                            if (queue.get(6) == null) {

                                queue.set(6, receiver123.getObjectValues());
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> First thread here");
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                workers.remove(0);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());

                            } else {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> After First thread ");
                                ArrayList<String[]> areas = (ArrayList<String[]>) queue.get(6);
                                areas.addAll((ArrayList<String[]>) receiver123.getObjectValues());
                                queue.set(6, areas);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers were: " + workers.size());
                                workers.remove(0);
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> Workers are: " + workers.size());
                                //think of code for more actionsforworker threads ginetai ths poutanas me ta casts
                            }

                            if (workers.isEmpty()) {
                                System.out.println("ActionsForWorkers Thread " + afw_ID + "> mphke edw");
                                //workers.add(this);
                                //workers.add(this);
                                for (int i = 0; i < activeWorkers; i++) {
                                    workers.add(this);
                                }
                                queue.set(0, 0);

                                queue.notifyAll();
                            }


                        }


                    }

                }

                //Test receiver = (Test) in.readObject();
                //System.out.println("Worker> " + receiver.getNum());

            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    in.close();
                    out.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }


    }
}



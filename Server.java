package ergasia;

import java.io.*;
import java.net.*;
import java.util.ArrayList;



// Server einai o Master edw ginontai syndeseis tou Console App (Client) me ton Master (Server)

// Client einai to Console App. Se auto syndeontai idiokthtes kai enoikiastes

/* ActionsForClients einai h polynhmatikh xrhsh tou Master.
Dhladh epitrepei se pollous Clients na syndethoun taytoxrona se enan server */

// Workers einai Threads tou Master pou epitrepoun polynhmatikh diaxeirhsh dedomenwn gia mapreduce




public class Server extends Thread { //AUTOS EINAI O MASTER

    //Client and Worker Server opens
    int connectorType;

    int numOfWorkers;
    /* Define the socket that receives requests */

    ServerSocket s;
    ServerSocket sw;
    /* Define the socket that is used to handle the connection */
    Socket providerSocket;

    /*
     * ArrayDeque<String[]> queue
     * Xrhsimopoieitai gia thn epikoinwnia metaksy tou ClientHandler kai tou WorkerHandler
     *
     * queue[0] contains the Action that is happening
     * queue[1] contains the name of the room
     * queue[2] contains the ID of the Worker that must do the calculations
     * queue[3] contains the Dates for rent that are to be added to a room
     * queue[4] same as 2 but needs a different memory
     * queue[5] contains the ID of the Client that sent the data
     * queue[6] contains the Rooms from each Worker
     * queue[7] Booking Start Date
     * queue[8] adds Filters
     * queue[9] Booking End Date
     *
     *
     */
    ArrayList<Object> queue;

    ArrayList<ActionsForWorkers> workers;

    ArrayList<ActionsForClients> clients;

    public Server(int connectorType, int numOfWorkers, ArrayList<ActionsForWorkers> workers, ArrayList<Object> queue, ArrayList<ActionsForClients> clients) {

        this.connectorType = connectorType;
        this.numOfWorkers = numOfWorkers;
        if (connectorType == -1) {
            this.workers = new ArrayList<ActionsForWorkers>();
            this.clients = new ArrayList<ActionsForClients>();
            this.queue = new ArrayList<Object>();
            /*this.queue.add(new String[1]);
            this.queue.add(new String[1]);
            this.queue.add(new String[1]);
            this.queue.add(new String[1]);
            this.queue.add(new String[1]);
            this.queue.add(new String[1]);
            this.queue.add(new String[1]);*/
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
            this.queue.add(null);
        } else {
            this.clients = clients;
            this.workers = workers;
            this.queue = queue;
        }
        //this.queue = queue;
    }
 
    public static void main(String[] args) {

        int workerNum;
        if (args[0] == null) {
            workerNum = 2;
        } else {
            workerNum = Integer.parseInt(args[0]);
        }

        new Server(-1, workerNum, null, null, null).start(); //Starting Thread, passes



    }

    public void run() {
        if (connectorType == 0) {
            try {
                s = new ServerSocket(4321, 10);//the same port as before, 10 connections;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            openServer(numOfWorkers, s, "Client");
        } else if (connectorType == 1) {
            try {
                sw = new ServerSocket(4322, 10); //Socket gia Workers
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            openServer(numOfWorkers, sw, "Worker");
        } else {
            new Server(0, numOfWorkers, workers, queue, clients).start();
            new Server(1, numOfWorkers, workers, queue, clients).start();
        }
    }

    void openServer(int numOfWorkers, ServerSocket sws, String connection) {
        System.out.println("Server> New " + connection + " Server Thread started...");

        try {

            int workerCounter = numOfWorkers;
            boolean canOpen = true;
            if (connection == "Worker") {
                System.out.println("Server> Workers allowed to connect: " + workerCounter);
            }



 
            /* Create Server Socket */
            //s = new ServerSocket(4321, 10);//the same port as before, 10 connections
            //sw = new ServerSocket(4322, 10); //Socket gia Workers

            while (true) {
                /* Accept the connection */

                providerSocket = sws.accept();
                if (connection == "Client") {
                    System.out.println("Server> New Client connected: " + providerSocket);
                    Thread clientThread = new ActionsForClients(providerSocket, queue, workers, numOfWorkers, clients);
                    clientThread.start();
                } else {
                    if (workerCounter == 0) {
                        System.out.println("Server> No more Workers are allowed to connect");
                        canOpen = false;
                    } else {
                        System.out.println("Server> New Worker connected with Port: " + providerSocket);
                    }


                    Thread workerThread = new ActionsForWorkers(providerSocket, numOfWorkers, queue, workers, canOpen);
                    workerThread.start();
                    workerCounter--;


                }

            }

        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                if (providerSocket != null) {
                    providerSocket.close();
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}

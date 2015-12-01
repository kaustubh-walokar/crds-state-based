package crdt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by kaustubh
 *
 * This class represent a node/server that holds a replica of the data.
 *
 */
public class Node extends Thread{

    private  boolean NOT_DOWN = true;
    private  boolean IS_STOPPED = false;
    private static LinkedList<String> queue = new LinkedList<>();

    // A data replica on this node.Also represents a state when merging
    private ACounterType data;
    private ArrayList<Node> adjacent;

    int port=4444;

    public Node(ACounterType data, int port ) throws IOException {
        this.data = data;
        this.port = port;
        this.adjacent = new ArrayList<>();

        System.out.println("Server Started and listening to the port "+port);
    }

    /**
     * Asumption that nodes are known (in a real system this could be unknown
     * and managed by somone else like a broker)
     * @param adjacent
     */
    public void setAdjacent(Node adjacent){
        this.adjacent.add(adjacent);
    }

    public void disconnectNode(){ NOT_DOWN=false;}
    public void connectNode(){ NOT_DOWN=true;}
    public void shutdown(){
        IS_STOPPED = true;
    }



    @Override
    public void run() {

        try {
            go();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void go()
            throws IOException, InterruptedException, ExecutionException {

        AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open();
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", port);
        serverChannel.bind(hostAddress);

        System.out.println("Server channel bound to port: " + hostAddress.getPort());
        System.out.println("Waiting for client to connect... ");

        Future acceptResult = serverChannel.accept();
        AsynchronousSocketChannel clientChannel = (AsynchronousSocketChannel) acceptResult.get();




        if ((clientChannel != null) && (clientChannel.isOpen())) {

            while (true) {

                ByteBuffer buffer = ByteBuffer.allocate(32);
                Future result = clientChannel.read(buffer);

                while (! result.isDone()) {
                    // do nothing
                }

                buffer.flip();
                String message = new String(buffer.array()).trim();

                queue.add(message);

                boolean stop = false;
                if(!queue.isEmpty())
                    stop=processMessage(queue.remove(),clientChannel);
                buffer.clear();

                if(stop)
                    break;


            } // while

            clientChannel.close();

        } //if

        serverChannel.close();
    }

    private boolean processMessage(String message, AsynchronousSocketChannel clientChannel) {


        System.out.print("Message recieved @"+port+" : " + message);

        if (message.equals("shutdown") || IS_STOPPED)
            return true;
        if(message.equals("increment") && NOT_DOWN) {
            this.data.operation();
            System.out.println(" Data with me :"+data.value()+" \t"+data);
        }

        /**
         * simulates a node not rechable event and prevents the replica from
         * being updated
         */
        if(message.equals("disconnect"))
            disconnectNode();
        /**
         * simulates a re connect
         */
        if(message.equals("connect")) {
            connectNode();
        }
        /**
         * gets the value of the counter as seen by the node
         */
        if(message.equals("value")) {
            System.out.println(" Value : "+this.data.value());
            clientChannel.write(ByteBuffer.wrap(this.data.value().toString().getBytes()));
        }

        /**
         * calls a reconcile to merge the values
         * Typically this should be called in response to a trigger in the system
         * My understanding is this should be called when the system detects a
         * inconsistency in the states of the replicas or inresponse to a scheduled task.
         * Although the number of reconciles to make the state consistent can not be
         * predicted, the system can be theoretically brought to a consistent state
         * after n reconciles
         */
        if(message.equals("reconcile"))
            reconcileAll();


        return false;
    }

    /**
     * To ensure that the reconcile stakes place across all the nodes in the system.
     * -could be handeled by a broker.
     */
    private void reconcileAll() {
        for (Node node : adjacent) {
            node.reconcile();
        }
        this.reconcile();
    }

    /**
     * The reconciliation process producees a LUB in the system by calling a merge on
     * the states.
     *
     * The state of the system can be huge though and in practical use only a delta of
     * the state from the last reconciled state
     */
    private void reconcile() {
        //System.out.println("reconciling at " + this.port);
        for (Node node:adjacent) {
            if(this.data.compare(node.data))
                this.data=this.data.merge(node.data);
        }
    }


}

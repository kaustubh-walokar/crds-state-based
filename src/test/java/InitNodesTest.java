import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Future;

/**
 * Created by kaustubh on 11/26/15.
 */
public class InitNodesTest {

    AsynchronousSocketChannel server3;
    AsynchronousSocketChannel server2;
    AsynchronousSocketChannel server1;
    InetSocketAddress hostAddress1;
    InetSocketAddress hostAddress2;
    InetSocketAddress hostAddress3;

    String increment = "increment";
    String disconnect = "disconnect";
    String connect = "connect";
    String value = "value";
    String rocncile="reconcile";

    @Before
    public void setUp() throws Exception {

        /**
         * Creates 3 nodes with one data replica each
         **/
        InitNodes.initialise();

        //wait for initialisation to finish
        Thread.sleep(3000);

        Future future;
        server1 = AsynchronousSocketChannel.open();
        hostAddress1 = new InetSocketAddress("localhost", 3827);
        future = server1.connect(hostAddress1);
        future.get(); // returns null


        server2 = AsynchronousSocketChannel.open();
        hostAddress2 = new InetSocketAddress("localhost", 3828);
        future = server2.connect(hostAddress2);
        future.get(); // returns null


        server3 = AsynchronousSocketChannel.open();
        hostAddress3 = new InetSocketAddress("localhost", 3829);
        future = server3.connect(hostAddress3);
        future.get(); // returns null
    }

    @After
    public void tearDown() throws Exception {
        sendMessage(server1,"shutdown");
        sendMessage(server2,"shutdown");
        sendMessage(server3,"shutdown");

        server1.close();
        server2.close();
        server3.close();
    }


    @Test
    public void doesSystemValueOfCounterRemainSameAfterRecon(){
        sendMessage(server1, increment);//1
        sendMessage(server2, increment);//2
        sendMessage(server3, increment);//3

        sendMessage(server3,rocncile);
        sendMessage(server1, value);
        Assert.assertEquals(3,Integer.parseInt(readValue(server1)));
        sendMessage(server2, value);
        Assert.assertEquals(3,Integer.parseInt(readValue(server2)));
        sendMessage(server3, value);
        Assert.assertEquals(3,Integer.parseInt(readValue(server3)));
    }


    @Test
    public void checkIncrementsOnAllServersBeforeRecon() {


        sendMessage(server1, increment);//1
        sendMessage(server1, increment);//2
        sendMessage(server1, value);
        Assert.assertEquals(2,Integer.parseInt(readValue(server1)));

        sendMessage(server2, increment);//1
        sendMessage(server2, increment);//2
        sendMessage(server2, value);
        Assert.assertEquals(2,Integer.parseInt(readValue(server2)));

        sendMessage(server3, increment);//1
        sendMessage(server3, increment);//2
        sendMessage(server3, value);
        Assert.assertEquals(2,Integer.parseInt(readValue(server3)));



    }

    @Test
    public void doesReconWorkWhenNodeWasNotReachable(){

                                       // the system value of counter
        sendMessage(server1,increment);//1
        sendMessage(server2,increment);//2
        sendMessage(server3,increment);//3

        //no increments in system value since server1 down
        sendMessage(server1,disconnect);

        sendMessage(server1,increment);
        sendMessage(server2,increment);//4
        sendMessage(server3,increment);//5

        sendMessage(server1,connect);


        sendMessage(server1,increment);//6
        sendMessage(server2,increment);//7

        sendMessage(server1,increment);//8
        sendMessage(server2,increment);//9
        sendMessage(server3,increment);//10

        sendMessage(server3,rocncile);


        sendMessage(server1,value);//10
        Assert.assertEquals(10,Integer.parseInt(readValue(server1)));
        sendMessage(server2,value);//10
        Assert.assertEquals(10,Integer.parseInt(readValue(server2)));
        sendMessage(server3,value);//10
        Assert.assertEquals(10,Integer.parseInt(readValue(server3)));

    }


    private Future sendMessage(AsynchronousSocketChannel client, String message){

        ByteBuffer b = ByteBuffer.wrap(message.getBytes());
        Future result = client.write(b);
        b.clear();

        while (!result.isDone()) {
            System.out.println("... ");
        }
        b.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return result;
    }

    private String  readValue(AsynchronousSocketChannel channel){
        ByteBuffer buffer = ByteBuffer.allocate(32);
        Future res = channel.read(buffer);
        while (! res.isDone()) {
            // do nothing
        }
        buffer.flip();
        String message = new String(buffer.array()).trim();

        //System.out.println("message : "+message);
        return message;
    }
}
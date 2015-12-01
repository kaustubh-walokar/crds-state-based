import crdt.ACounterType;
import crdt.Node;

import java.io.IOException;

public class InitNodes {

    public static void main(String[] args) {
            }

    public static void initialise(){
        try {
            Node n1 = new Node(new ACounterType(0,3),3827);
            Node n2 = new Node(new ACounterType(1,3),3828);
            Node n3 = new Node(new ACounterType(2,3),3829);

            n1.setAdjacent(n2);
            n1.setAdjacent(n3);

            n2.setAdjacent(n1);
            n2.setAdjacent(n3);

            n3.setAdjacent(n2);
            n3.setAdjacent(n1);

            n1.start();
            n2.start();
            n3.start();



        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

package crdt;

import java.util.Arrays;

/**
 * Created by kaustubh on 11/24/15.
 *
 * A datatype with an always incrementing value
 * and has operation to Increment
 *
 * The specification is aimed to emulate a datatype that has a join-semilatice
 * semantics and has well defined LUB to which the values eventually converge
 *
 * The reconciliantion result in merge amongst all nodes
 */

public class ACounterType implements IData<ACounterType,Integer> {

    private int initial [];
    private int me; // id of the node to which this replica belongs


    public void operation() {
        initial[me]++;
    }

    public ACounterType(int me, int numNodes) {
        this.initial = new int[numNodes];
        for (int i = 0; i < initial.length; i++)
            initial[i]=0;
        this.me = me;
    }


    /**
     * The value of the counter for the system as seen by itself and returned to the
     * data seeker.this value is eventually supposed to return the value
     * of the system as seen by a the entire system as a whole, after n number of
     * reconciliations done via some 'gossip' protocol.
     */
    @Override
    public Integer value(){
        int x =0 ;
        for (int i = 0; i < initial.length; i++) {
            x+=initial[i] ;
        }
        return x;
    }

    /**
     * The comparision operation ensures the partial order over two states
     * (held by self and the other state)
     * @param y - state of the other replica
     * @return
     */
    @Override
    public boolean compare(ACounterType y){
        if(initial.length!=y.initial.length)
            try {
                throw new Exception("Invalid  operation");
            } catch (Exception e) {
                e.printStackTrace();
            }

        boolean val= false;
        for (int i = 0; i < initial.length; i++) {
            val = (initial[i]<=y.initial[i]);
        }
        return val;
    }

    /**
     * A merge on the data produces the LUB in our case results in the total
     * count in the system
     * @param x
     * @return
     */
    @Override
    public ACounterType merge(ACounterType x){
        if(x.initial.length!=this.initial.length)
            try {
                throw new Exception("Invalid  operation");
            } catch (Exception e) {
                e.printStackTrace();
            }

        ACounterType n  = new ACounterType(me,x.initial.length);
        for (int i = 0; i < x.initial.length; i++) {
            n.initial[i] = Math.max(x.initial[i],this.initial[i]);

        }
        return n;
    }


    @Override
    public String toString() {
        return "ACounterType{" +
                "initial=" + Arrays.toString(initial) +
                '}';
    }

}

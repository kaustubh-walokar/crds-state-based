package crdt;

/**
 * Created by kaustubh on 11/26/15.
 */
public interface IData<T,U>{

    U value();
    boolean compare(T y);
    T merge(T x);
}

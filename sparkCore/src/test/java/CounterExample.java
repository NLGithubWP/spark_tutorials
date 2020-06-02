public class CounterExample {
    public static void main(String[] args) {
        //构造两个实例，让每个线程访问一个实例
        Counter counter1 = new Counter();
        Counter counter2 = new Counter();
        Thread threadA = new CounterThread( counter1);
        Thread threadB = new CounterThread( counter2);
        threadA.start();
        threadB.start();
    }
}

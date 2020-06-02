public class CounterThread extends Thread {
    protected Counter counter = null;
    public CounterThread( Counter counter) {
        this. counter = counter;
    }
    public void run() {
        //用多个线程调用同步实例方法
        for ( int i = 0; i < 5; i++) {
            counter.add();
        }
    }
}

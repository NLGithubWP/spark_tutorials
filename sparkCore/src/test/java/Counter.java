public class Counter {
    static long count = 0;
    //同步实例方法
    public static  void add() {
        count++;
        try {
            Thread. sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System. out.println(Thread. currentThread().getName() + "--" + count);
    }
}

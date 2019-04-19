package simpledb;


        import java.util.ArrayList;

public class main {
    public static void main(String[] args){
        byte A[] = new byte[3];
        for(byte b: A){
            System.out.println((int) b);
            System.out.println(1<<8);
            System.out.println((byte) (b | (1<<7)) < 0);
            System.out.println((int)(byte) (b | (1<<5)));
        }
        A[0] = 0;
        System.out.println(A);
        System.out.println(A);

        System.out.println("-----------");

        ArrayList<String> list = new ArrayList<String>();
        System.out.println(list.getClass());

        list.add("dff1");
        list.add("dff2");
        list.add("dff3");
        list.add("dff4");
        list.add("dff5");
        String[] array = new String[list.size()];
        String[] s = list.toArray(array);
        System.out.println(s.getClass());
        System.out.println(s[2]);
    }
}

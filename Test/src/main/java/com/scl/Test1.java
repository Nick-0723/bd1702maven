package com.scl;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Test1 {


    @Test
    /*
      switch语句,如果没有break,则继续往下执行直到break,或到最后一个case.
     */
    @SuppressWarnings("all")
    public void test1(){
        int i = 1;
        switch (i) {
            default:
                System.out.println("default");
            case 0 :
                System.out.println("zero");
                break;
            case 1 :
                System.out.println("one");
            case 2:
                System.out.println("two");
        }
    }
    @Test
    /*
      lambda表达式, Collection集合有 forEach() 方法
     */
    public void test2(){
        Integer[] arr = new Integer[]{1,1,2,3,1,321,321,21,3,23,213,32,1,5,56,7,3,4,5};
        List<Integer> list = Arrays.asList(arr);
//        list.forEach(System.out::println);
//        list.forEach((a)->{
//            a = 2333;
//            System.out.println(a);
//        });
        list.stream()
                .map(Integer::toBinaryString)
                .forEach(System.out::println);
    }


    @Test
    /*
       测试格式化输出printf
     */
    public void test3(){
        double d = 12.123;
        String s = "nick";
        int i = 12;

        //"%"表示进行格式化输出，"%"之后的内容为格式的定义。
        System.out.printf("%f",d); //"f"表示格式化输出浮点数。
        System.out.println();
        System.out.printf("%9.2f",d); //"9.2"中的9表示输出的长度，2表示小数点后的位数。
        System.out.println();
        System.out.printf("%+9.2f",d); //"+"表示输出的数带正负号。
        System.out.println();
        System.out.printf("%-9.4f",d); //"-"表示输出的数左对齐（默认为右对齐）。
        System.out.println();
        System.out.printf("%+-9.3f",d); //"+-"表示输出的数带正负号且左对齐。
        System.out.println();
        System.out.printf("%d",i); //"d"表示输出十进制整数。
        System.out.println();
        System.out.printf("%o",i); //"o"表示输出八进制整数。
        System.out.println();
        System.out.printf("%x",i); //"d"表示输出十六进制整数。
        System.out.println();
        System.out.printf("%#x",i); //"d"表示输出带有十六进制标志的整数。
        System.out.println();
        System.out.printf("%s",s); //"d"表示输出字符串。
        System.out.println();
        System.out.printf("输出一个浮点数：%f，一个整数：%d，一个字符串：%s",d,i,s);
        //可以输出多个变量，注意顺序。
        System.out.println();
        System.out.printf("字符串：%2$s，%1$d的十六进制数：%1$#x",i,s);
        //"X$"表示第几个变量。
    }

    @Test
    /*
        测试this关键字
    */
    public void test4(){
        new Test1_1().test();
    }
}

class Test1_1{
    void test(){
        System.out.printf("this是%s",this.getClass());
    }
}


class ExaminationDemo{

    public static void main(String[] args){
        System.out.println("运行ExaminationDemo中的main函数，创建D类实例");
        new D();
    }
}
@SuppressWarnings("all")
class E{
    public E(){
        System.out.println("执行E的构造函数");
    }
    public void funcOfE(){
        System.out.println("执行E的函数");
    }
}
@SuppressWarnings("all")
class F{
    public F(){
        System.out.println("执行F的构造函数");
    }
    public void funcOfF(){
        System.out.println("执行F的函数");
    }
}
@SuppressWarnings("all")
class B{
    E e=new E();
    static F f=new F();
    public String sb=getSb();
    static{
        System.out.println("执行B类的static块(B包含E类的成员变量,包含静态F类成员变量)");
        f.funcOfF();
    }
    {
        System.out.println("执行B实例的普通初始化块");
    }
    public B(){
        System.out.println("执行B类的构造函数(B包含E类的成员变量,包含静态F类成员变量)");
        e.funcOfE();
    }
    public String getSb(){
        System.out.println("初始化B的实例成员变量sb");
        return "sb";
    }
}
@SuppressWarnings("all")
class C extends B{
    static{
        System.out.println("执行C的static块(C继承B)");
    }
    {
        System.out.println("执行C的普通初始化块");
    }
    public C(){
        System.out.println("执行C的构造函数(C继承B)");
    }
}
@SuppressWarnings("all")
class D extends C{
    public String sd1=getSd1();
    public static String sd=getSd();
    static{
        System.out.println("执行D的static块(D继承C)");
    }
    {
        System.out.println("执行D实例的普通初始化块");
    }

    public D(){
        System.out.println("执行D的构造函数(D继承C);父类B的实例成员变量sb的值为："+sb+";本类D的static成员变量sd的值为："+sd+";本类D的实例成员变量sd1的值是："+sd1);
    }

    static public String getSd(){
        System.out.println("初始化D的static成员变量sd");
        return "sd";
    }
    public String getSd1(){
        System.out.println("初始化D的实例成员变量sd1");
        return "sd1";
    }
}

@SuppressWarnings("all")
class L {
    static String str = "aaa";

    static {
        System.out.println("L static");
    }

    public L(){
        System.out.println("L de gou zao han shu");
    }

    public void fun(){
        System.out.println("llllll");
    }
}

class X extends L{
    public static void main(String[] args) {
        System.out.println("1");
        System.out.println(L.str);
        new X();
    }
}
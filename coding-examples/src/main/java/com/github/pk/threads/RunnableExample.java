package com.github.pk.threads;



// An application that creates the instance of threads must implement
public class RunnableExample implements Runnable{
    @Override
    public void run() {
        System.out.println("Hi This is Fitst Java thread");
    }
    public static void main(String[] args){
        (new Thread(new RunnableExample())).start ();


    }
}

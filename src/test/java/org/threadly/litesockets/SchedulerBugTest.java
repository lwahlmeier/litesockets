package org.threadly.litesockets;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.litesockets.Server.ClientAcceptor;
import org.threadly.litesockets.tcp.Utils;
import org.threadly.test.concurrent.TestCondition;

public class SchedulerBugTest {

  @Test
  public void test() throws Exception {
    final AtomicInteger ai = new AtomicInteger(0);
    final PriorityScheduler PS = new PriorityScheduler(10);
    ThreadedSocketExecuter TSE = new ThreadedSocketExecuter(PS);
    TSE.start();
    int port = Utils.findTCPPort();
    TCPServer server = TSE.createTCPServer("localhost", port);
    server.setClientAcceptor(new ClientAcceptor() {
      @Override
      public void accept(Client client) {
        System.out.println("New Client, Scheduling!");
        PS.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            System.out.println("Looped:"+ai.get());
            ai.incrementAndGet();
          }}, 10, 10);
      }});
    server.start();
    TCPClient client = TSE.createTCPClient("localhost", port);
    client.connect().get(5000, TimeUnit.MILLISECONDS);
    new TestCondition(){
      @Override
      public boolean get() {
        return ai.get() >= 10;
      }
    }.blockTillTrue(5000);
  }
}

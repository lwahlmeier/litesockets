package org.threadly.litesockets.networkutils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.litesockets.ThreadedSocketExecuter;
import org.threadly.litesockets.tcp.FakeTCPServerClient;
import org.threadly.litesockets.tcp.TCPClient;
import org.threadly.litesockets.tcp.Utils;
import org.threadly.litesockets.utils.MergedByteBuffers;
import org.threadly.test.concurrent.TestCondition;

public class ProfileServerTest {
  PriorityScheduler PS;
  ThreadedSocketExecuter SE;
  int port;
  FakeTCPServerClient clientHandler;
  ProfileServer pServer;
  
  @Before
  public void start() throws IOException {
    port = Utils.findTCPPort();
    PS = new PriorityScheduler(5);
    SE = new ThreadedSocketExecuter(PS);
    SE.start();
    clientHandler = new FakeTCPServerClient(SE);
    pServer = new ProfileServer(SE, "localhost", port, 10);
  }
  
  @After
  public void stop() {
    pServer.stopIfRunning();
    SE.stopIfRunning();
    PS.shutdown();
  }

  @Test
  public void helpTest() throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    client.writeForce(ByteBuffer.wrap("TEST\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > 0;
      }
    }.blockTillTrue(5000);
    pServer.stop();
  }
  
  @Test
  public void DoubleStartTest()  throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    client.writeForce(ByteBuffer.wrap("start\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > 0;
      }
    }.blockTillTrue(5000);
    assertEquals(getMessageAsString(ProfileServer.STARTED_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
    client.writeForce(ByteBuffer.wrap("start\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > 0;
      }
    }.blockTillTrue(5000);
    assertEquals(getMessageAsString(ProfileServer.ALREADY_STARTED_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
    
  }
  
  @Test
  public void AlreadyStoppedTest()  throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    client.writeForce(ByteBuffer.wrap("stop\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > 0;
      }
    }.blockTillTrue(5000);
    assertEquals(getMessageAsString(ProfileServer.ALREADY_STOPPED_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
    
  }
  
  @Test
  public void StartDumpResetDumpStopTest() throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    client.writeForce(ByteBuffer.wrap("start\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() == ProfileServer.STARTED_RESPONSE.remaining();
      }
    }.blockTillTrue(5000);
    assertEquals(getMessageAsString(ProfileServer.STARTED_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
    Thread.sleep(100);
    client.writeForce(ByteBuffer.wrap("dump\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > ProfileServer.START_DUMP.length() + ProfileServer.END_DUMP.length();
      }
    }.blockTillTrue(5000);
    Thread.sleep(100);
    assertEquals(ProfileServer.START_DUMP,clientHandler.map.get(client).getAsString(ProfileServer.START_DUMP.length()));
    clientHandler.map.get(client).discard(clientHandler.map.get(client).remaining());
    
    client.writeForce(ByteBuffer.wrap("reset\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() == ProfileServer.RESET_RESPONSE.remaining();
      }
    }.blockTillTrue(5000);
    Thread.sleep(100);
    assertEquals(getMessageAsString(ProfileServer.RESET_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
    clientHandler.map.get(client).discard(clientHandler.map.get(client).remaining());
    
    client.writeForce(ByteBuffer.wrap("stop\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() == ProfileServer.STOPPED_RESPONSE.remaining();
      }
    }.blockTillTrue(5000);
    assertEquals(getMessageAsString(ProfileServer.STOPPED_RESPONSE),clientHandler.map.get(client).getAsString(clientHandler.map.get(client).remaining()));
  }
  
  @Test
  public void emptyDumpTest() throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    client.writeForce(ByteBuffer.wrap("dump\n".getBytes()));
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.get(client).remaining() > ProfileServer.START_DUMP.length() + ProfileServer.END_DUMP.length();
      }
    }.blockTillTrue(5000);

    assertEquals(ProfileServer.START_DUMP, clientHandler.map.get(client).getAsString(ProfileServer.START_DUMP.length()));
    assertEquals(ProfileServer.END_DUMP, clientHandler.map.get(client).getAsString(ProfileServer.END_DUMP.length()));
    pServer.stop();
  }
  
  @Test
  public void badDataTest() throws IOException, InterruptedException {
    pServer.start();
    final TCPClient client = new TCPClient("localhost", port);
    clientHandler.addTCPClient(client);
    for(int i=0; i<10000; i++) {
      client.writeForce(ByteBuffer.wrap("crap".getBytes()));
    }
    new TestCondition(){
      @Override
      public boolean get() {
        return clientHandler.map.size() == 0;
      }
    }.blockTillTrue(5000);
    pServer.stop();
  }
  
  public static String getMessageAsString(ByteBuffer bb) {
    MergedByteBuffers mbb = new MergedByteBuffers();
    mbb.add(bb.duplicate());
    return mbb.getAsString(mbb.remaining());
  }
  
}

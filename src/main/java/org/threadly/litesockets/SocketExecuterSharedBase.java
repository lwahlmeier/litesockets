package org.threadly.litesockets;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.litesockets.utils.SimpleByteStats;
import org.threadly.litesockets.utils.WatchdogCache;
import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionHandlerInterface;

public abstract class SocketExecuterSharedBase extends AbstractService implements SocketExecuterInterface {
  public static final int WATCHDOG_CLEANUP_TIME = 30000;
  
  protected final ConcurrentMap<SocketChannel, Client> clients = new ConcurrentHashMap<SocketChannel, Client>();
  protected final ConcurrentMap<SelectableChannel, Server> servers = new ConcurrentHashMap<SelectableChannel, Server>();
  protected final SocketExecuterByteStats stats = new SocketExecuterByteStats();
  protected final SimpleSchedulerInterface schedulerPool;
  protected final WatchdogCache dogCache;
  protected final Runnable watchDogCleanup;
  protected volatile ExceptionHandlerInterface exceptionHandler = new EmptyExceptionHandler();
  
  protected SocketExecuterSharedBase(SimpleSchedulerInterface scheduler) {
    ArgumentVerifier.assertNotNull(scheduler, "SimpleSchedulerInterface");
    this.schedulerPool = scheduler;
    dogCache = new WatchdogCache(schedulerPool);
    watchDogCleanup = new PooperScooper();
  }
  
  protected abstract void connectClient(Client client);
  protected abstract void setClientThreadExecutor(Client client);
  protected abstract void addServerToSelectors(Server server);
  protected abstract void removeServerFromSelectors(Server server);
  protected abstract void removeClientFromSelectors(Client client);
  
  @Override
  public void addServer(final Server server) {
    ArgumentVerifier.assertNotNull(server, "Server");
    if(isRunning()) {
      Server sn = servers.putIfAbsent(server.getSelectableChannel(), server);
      if(sn == null) {
        server.setSocketExecuter(this);
        server.setThreadExecutor(schedulerPool);
        addServerToSelectors(server);
      }
    }
  }
  
  @Override
  public void removeServer(Server server) {
    ArgumentVerifier.assertNotNull(server, "Server");
    if(isRunning()) {
      removeServerFromSelectors(server);
      servers.remove(server.getSelectableChannel());
    }
  }
  
  @Override
  public void addClient(final Client client) {
    ArgumentVerifier.assertNotNull(client, "Client");
    if(! client.isClosed() && client.getProtocol() == WireProtocol.TCP && isRunning()) {
      client.setClientsSocketExecuter(this);
      setClientThreadExecutor(client);
      if(client.getChannel() != null && client.getChannel().isConnected()) {
        addConnectedClient(client);
      } else {
        addUnConnectedClient(client);
      }
    }
  }
  
  private void addUnConnectedClient(Client client) {
    if(client.getChannel() == null) {
      client.connect();
    }
    Client nc = clients.putIfAbsent(client.getChannel(), client);
    if(nc == null) {
      connectClient(client);
      dogCache.watch(client.connect(), client.getTimeout());
    }
  }
  
  private void addConnectedClient(Client client) {
    Client nc = clients.putIfAbsent(client.getChannel(), client);
    if(nc == null) {
      if(client.canRead()) {
        this.flagNewRead(client);
      }
      if(client.canWrite()) {
        this.flagNewWrite(client);  
      }
    }
  }
  
  @Override
  public void removeClient(Client client) {
    ArgumentVerifier.assertNotNull(client, "Client");
    if(isRunning()) {
      Client c = clients.remove(client.getChannel());
      if(c != null) {
        removeClientFromSelectors(client);
      }
    }
  }
  

  @Override
  public int getClientCount() {
    return clients.size();
  }

  @Override
  public int getServerCount() {
    return servers.size();
  }
  
  @Override
  public SimpleByteStats getStats() {
    return stats;
  }
  
  @Override
  public void watchFuture(ListenableFuture<?> lf, long delay) {
    dogCache.watch(lf, delay);
  }
  
  @Override
  public SimpleSchedulerInterface getThreadScheduler() {
    return schedulerPool;
  }
  
  @Override
  public void setSocketExecuterExceptionHander(ExceptionHandlerInterface eh) {
    exceptionHandler = eh;
  }
  
  /**
   * This class is a helper runnable to generically add SelectableChannels to a selector for certain operations.
   * 
   */
  protected class AddClientToSelector implements Runnable {
    Client localClient;
    Selector localSelector;
    int registerType;

    public AddClientToSelector(Client client, Selector selector, int registerType) {
      localClient = client;
      localSelector = selector;
      this.registerType = registerType;
    }

    @Override
    public void run() {
      if(isRunning()) {
        try {
          System.out.println("addClient");
          localClient.getChannel().register(localSelector, registerType);
        } catch (ClosedChannelException e) {
          removeClient(localClient);
          localClient.close();
        } catch (CancelledKeyException e) {
          System.out.println("addClient-except");
          removeClient(localClient);
        }
      }
    }
  }
  
  /**
   * This class is a helper runnable to generically remove SelectableChannels from a selector.
   * 
   *
   */
  protected class RemoveFromSelector implements Runnable {
    SelectableChannel localChannel;
    Selector localSelector;

    public RemoveFromSelector(SelectableChannel channel, Selector selector) {
      localChannel = channel;
      localSelector = selector;
    }

    @Override
    public void run() {
      if(isRunning()) {
        System.out.println("removeClient");
        SelectionKey sk = localChannel.keyFor(localSelector);
        if(sk != null) {
          sk.cancel();
          flushOutSelector();
        }
      }
    }
    
    protected void flushOutSelector() {
      try {
        localSelector.selectNow();
      } catch (IOException e) {

      }
    }
  }
  
  /**
   * This class is a helper runnable to generically add SelectableChannels to a selector for certain operations.
   * 
   */
  protected class AddServerToSelector implements Runnable {
    Server localServer;
    Selector localSelector;
    int registerType;

    public AddServerToSelector(Server server, Selector selector, int registerType) {
      localServer = server;
      localSelector = selector;
      this.registerType = registerType;
    }

    @Override
    public void run() {
      if(isRunning()) {
        try {
          localServer.getSelectableChannel().register(localSelector, registerType);
        } catch (ClosedChannelException e) {
          removeServer(localServer);
          localServer.close();
        } catch (CancelledKeyException e) {
          removeServer(localServer);
        }
      }
    }
  }
  
  private class PooperScooper implements Runnable {
    @Override
    public void run() {
      if(isRunning()) {
        try {
          dogCache.cleanup();
        } finally {
          schedulerPool.schedule(this, WATCHDOG_CLEANUP_TIME);
        }
      }
    }
  }
  
  /**
   * Implementation of the SimpleByteStats.
   */
  protected static class SocketExecuterByteStats extends SimpleByteStats {
    @Override
    protected void addWrite(int size) {
      super.addWrite(size);
    }
    
    @Override
    protected void addRead(int size) {
      super.addRead(size);
    }
  }
  
  protected static class EmptyExceptionHandler implements ExceptionHandlerInterface {
    @Override
    public void handleException(Throwable thrown) {
      
    }
  }
}

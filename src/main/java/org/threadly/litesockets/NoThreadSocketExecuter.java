package org.threadly.litesockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * <p>The NoThreadSocketExecuter is a simpler implementation of a {@link SocketExecuterInterface} 
 * that does not create any threads. Since there are no threads operations happen on whatever thread
 * calls .select(), and only 1 thread at a time should ever call it at a time.  Other then
 * that it should be completely thread safe.</p>
 * 
 * <p>This is generally the implementation used by clients.  It can be used for servers
 * but only when not servicing many connections at once.  How many connections is hardware
 * and OS defendant.  For an average multi-core x86 linux server I a connections not to much more
 * then 1000 connections would be its limit, though alot depends on how active those connections are.</p>
 * 
 * <p>It should also be noted that all client read/close callbacks happen on the thread that calls select().</p>
 * 
 * <p>The functions like {@link #addClient(Client)}, {@link #removeClient(Client)}, {@link #addServer(Server)}, and 
 * {@link #removeServer(Server)} can be called from other threads safely.</p>
 * 
 * @author lwahlmeier
 */
public class NoThreadSocketExecuter extends SocketExecuterSharedBase implements SocketExecuterInterface {
  private final NoThreadScheduler scheduler;
  private volatile Selector selector;

  /**
   * Constructs a NoThreadSocketExecuter.  {@link #start()} must still be called before using it.
   */
  public NoThreadSocketExecuter() {
    super(new NoThreadScheduler());
    scheduler = (NoThreadScheduler)schedulerPool;
  }

  /**
   * This is used to wakeup the {@link Selector} assuming it was called with a timeout on it.
   * Most all methods in this class that need to do a wakeup do it automatically, but
   * there are situations where you might want to wake up the thread we are blocked on 
   * manually.
   */
  public void wakeup() {
    if(isRunning()) {
      selector.wakeup();
    }
  }

  @Override
  public void flagNewWrite(Client client) {
    updateClientState(client);
  }

  @Override
  public void flagNewRead(Client client) {
    updateClientState(client);
  }
  
  private void updateClientState(Client client) {
    ArgumentVerifier.assertNotNull(client, "Client");
    if(isRunning() && clients.containsKey(client.getChannel())) {
      if(client.canWrite() && client.canRead()) {
        schedulerPool.execute(new AddClientToSelector(client, selector, SelectionKey.OP_WRITE|SelectionKey.OP_READ));
        selector.wakeup();
      } else if (client.canRead()){
        schedulerPool.execute(new AddClientToSelector(client, selector, SelectionKey.OP_READ));
        selector.wakeup();
      } else if (client.canWrite()){
        schedulerPool.execute(new AddClientToSelector(client, selector, SelectionKey.OP_WRITE));
        selector.wakeup();
      } 
    }
  }

  @Override
  protected void startupService() {
    try {
      selector = Selector.open();
      scheduler.schedule(watchDogCleanup, WATCHDOG_CLEANUP_TIME);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void shutdownService() {
    scheduler.clearTasks();
    selector.wakeup();
    selector.wakeup();
    if(selector != null && selector.isOpen()) {
      scheduler.execute(new Runnable() {
        @Override
        public void run() {
          try {
            selector.close();
          } catch (Exception e) {
            ExceptionUtils.handleException(e);
          }
        }});
    }
    clients.clear();
    servers.clear();
    dogCache.cleanAll();
  }

  /**
   * This will run all ExecuterTasks, check for pending network operations,
   * then run those operations.  There can be a lot of I/O operations so this
   * could take some time to run.  In general it should not be called from things like
   * GUI threads.
   * 
   */
  public void select() {
    select(0);
  }

  /**
   * This is the same as the {@link #select()} but it allows you to set a delay.
   * This delay is the time to wait for socket operations to happen.  It will
   * block the calling thread for up to this amount of time, but it could be less
   * if any network operation happens (including another thread adding a client/server). 
   * 
   * 
   * @param delay Max time in milliseconds to block for.
   */
  public void select(int delay) {
    ArgumentVerifier.assertNotNegative(delay, "delay");
    if(isRunning()) {
      scheduler.tick(null);
      try {
        if(delay == 0) {
          selector.selectNow();
        } else {
          selector.select(delay);
        }
        for(SelectionKey key: selector.selectedKeys()) {
          try {
            if (key.isConnectable() ) {
              SocketChannel sc = (SocketChannel)key.channel();
              final Client client = clients.get(sc);
              doConnect(client);
            }
            if(key.isAcceptable()) {
              ServerSocketChannel server = (ServerSocketChannel) key.channel();
              doAccept(server);
            } else if(key.isReadable()) {
              doRead(key.channel());
            } else if(key.isWritable()) {
              SocketChannel sc = (SocketChannel)key.channel();
              doWrite(sc);
            }
          } catch(CancelledKeyException e) {
            exceptionHandler.handleException(e);
          }
        }
      } catch (IOException e) {
        exceptionHandler.handleException(e);
      } catch(ClosedSelectorException e) {
        exceptionHandler.handleException(e);
      } catch (NullPointerException e) {
        exceptionHandler.handleException(e);
      }
      scheduler.tick(null);
    }
  }
  
  private void doConnect(Client client) {
    if(client.getChannel().isConnectionPending()) {
      try {
        if(client.getChannel().finishConnect()) {
          client.setConnectionStatus(null);
          if(client.canWrite()) {
            flagNewWrite(client);
          } else if (client.canRead()) {
            flagNewRead(client);
          }
        }
      } catch(IOException e) {
        client.setConnectionStatus(e);
        removeClient(client);
        client.close();
      }
    }
  }

  private void doAccept(ServerSocketChannel server) {
    final Server tServer = servers.get(server);
    try {
      SocketChannel client = server.accept();
      if(client != null) {
        client.configureBlocking(false);
        tServer.acceptChannel(client);
      }
    } catch (IOException e) {
      removeServer(tServer);
      tServer.close();
    }    
  }

  private void doRead(SelectableChannel sc) {
    final Client client = clients.get(sc);
    if(client != null) {
      try {
        ByteBuffer readByteBuffer = client.provideReadByteBuffer();
        int origPos = readByteBuffer.position();
        int read = client.getChannel().read(readByteBuffer);
        if(read < 0) {
          removeClient(client);
          client.close();
        } else if( read > 0) {
          stats.addRead(read);
          readByteBuffer.position(origPos);
          ByteBuffer resultBuffer = readByteBuffer.slice();
          readByteBuffer.position(origPos+read);
          resultBuffer.limit(read);
          client.addReadBuffer(resultBuffer.asReadOnlyBuffer());
          if(! client.canWrite()  && ! client.canRead()) {
            client.getChannel().register(selector, 0);
          } else if (! client.canRead()  ) {
            client.getChannel().register(selector, SelectionKey.OP_WRITE);
          }
        } 
      } catch(IOException e) {
        removeClient(client);
        client.close();
      }
    }else {
      final Server server = servers.get(sc);
      if(server != null && server.getServerType() == WireProtocol.UDP) {
        server.acceptChannel((DatagramChannel)server.getSelectableChannel());
      }
    }
  }

  private void doWrite(SocketChannel sc) {
    final Client client = clients.get(sc);
    if(client != null) {
      try {
        int writeSize = sc.write(client.getWriteBuffer());
        stats.addWrite(writeSize);
        client.reduceWrite(writeSize);
        if(! client.canWrite()  && ! client.canRead()) {
          client.getChannel().register(selector, 0);
        } else if (! client.canWrite()  ) {
          client.getChannel().register(selector, SelectionKey.OP_READ);
        }
      } catch(IOException e) {
        removeClient(client);
        client.close();
      }
    }
  }

  @Override
  protected void connectClient(Client client) {
    schedulerPool.execute(new AddClientToSelector(client, selector, SelectionKey.OP_CONNECT));
    wakeup();
  }

  @Override
  protected void setClientThreadExecutor(Client client) {
    client.setClientsThreadExecutor(scheduler);
  }

  @Override
  protected void addServerToSelectors(Server server) {
    if(server.getServerType() == WireProtocol.TCP) {
      schedulerPool.execute(new AddServerToSelector(server, selector, SelectionKey.OP_ACCEPT));
    } else {
      schedulerPool.execute(new AddServerToSelector(server, selector, SelectionKey.OP_READ));
    }
    wakeup();
  }

  @Override
  protected void removeServerFromSelectors(Server server) {
    schedulerPool.execute(new RemoveFromSelector(server.getSelectableChannel(), selector));
    wakeup();
  }

  @Override
  protected void removeClientFromSelectors(Client client) {
    schedulerPool.execute(new RemoveFromSelector(client.getChannel(), selector));
    wakeup();    
  }

}

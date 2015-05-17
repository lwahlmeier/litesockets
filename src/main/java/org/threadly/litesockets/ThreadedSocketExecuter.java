package org.threadly.litesockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ScheduledExecutorService;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.KeyDistributedExecutor;
import org.threadly.concurrent.ScheduledExecutorServiceWrapper;
import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.util.ArgumentVerifier;


/**
 * <p>This is a mutliThreaded implementation of a {@link SocketExecuterInterface}.  It uses separate threads to perform Accepts, Reads and Writes.  
 * Constructing this will create 3 additional threads.  Generally only one of these will ever be needed in a process.</p>
 * 
 * <p>This is generally the {@link SocketExecuterInterface} implementation you want to use for servers, especially if they have to deal with more
 * then just a few connections.  See {@link NoThreadSocketExecuter} for a more efficient implementation when not dealing with many connections.</p>
 * 
 * @author lwahlmeier
 *
 */
public class ThreadedSocketExecuter extends SocketExecuterSharedBase implements SocketExecuterInterface {

  private final SingleThreadScheduler acceptScheduler = 
      new SingleThreadScheduler(new ConfigurableThreadFactory("SocketAcceptor", false, true, Thread.currentThread().getPriority(), null, null));
  private final SingleThreadScheduler readScheduler = 
      new SingleThreadScheduler(new ConfigurableThreadFactory("SocketReader", false, true, Thread.currentThread().getPriority(), null, null));
  private final SingleThreadScheduler writeScheduler = 
      new SingleThreadScheduler(new ConfigurableThreadFactory("SocketWriter", false, true, Thread.currentThread().getPriority(), null, null));
  private final KeyDistributedExecutor clientDistributer;

  protected volatile long readThreadID = 0;
  protected Selector readSelector;
  protected Selector writeSelector;
  protected Selector acceptSelector;

  private AcceptRunner acceptor;
  private ReadRunner reader;
  private WriteRunner writer;


  /**
   * <p>This constructor creates its own {@link SingleThreadScheduler} Threadpool to use for client operations.  This is generally 
   * not recommended unless you are not doing many socket connections/operations.  You should really use your own multiThreaded 
   * thread pool.</p>
   */
  public ThreadedSocketExecuter() {
    this(new SingleThreadScheduler(
        new ConfigurableThreadFactory(
            "SocketClientThread", false, true, Thread.currentThread().getPriority(), null, null)));
  }

  /**
   * <p>This is provided to allow people to use java's generic threadpool scheduler {@link ScheduledExecutorService}.</p>
   * 
   * @param exec The {@link ScheduledExecutorService} to be used for client/server callbacks.
   */
  public ThreadedSocketExecuter(ScheduledExecutorService exec) {
    this(new ScheduledExecutorServiceWrapper(exec));
  }
  
  /**
   * <p>Here you can provide a {@link ScheduledExecutorService} for this {@link SocketExecuterInterface}.  This will be used
   * on accept, read, and close callback events.</p>
   * 
   * @param exec the {@link ScheduledExecutorService} to be used for client/server callbacks.
   */
  public ThreadedSocketExecuter(SimpleSchedulerInterface exec) {
    super(exec);
    clientDistributer = new KeyDistributedExecutor(schedulerPool);
  }

  @Override
  protected void startupService() {
    try {
      acceptSelector = Selector.open();
      readSelector   = Selector.open();
      writeSelector  = Selector.open();
      acceptor = new AcceptRunner();
      reader   = new ReadRunner();
      writer   = new WriteRunner();
      acceptScheduler.execute(acceptor);
      readScheduler.execute(reader);
      writeScheduler.execute(writer);
      schedulerPool.schedule(watchDogCleanup, WATCHDOG_CLEANUP_TIME);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void shutdownService() {
    acceptScheduler.shutdownNow();
    readScheduler.shutdownNow();
    writeScheduler.shutdownNow();
    try {
      acceptSelector.close();
    } catch (IOException e) {
      //Dont care
    }
    try {
      readSelector.close();
    } catch (IOException e) {
    }
    try {
      writeSelector.close();
    } catch (IOException e) {
    }
    clients.clear();
    servers.clear();
    dogCache.cleanAll();
  }

  @Override
  public void flagNewWrite(Client client) {
    ArgumentVerifier.assertNotNull(client, "Client");
    if(isRunning() && clients.containsKey(client.getChannel())) {
      writeScheduler.execute(new AddClientToSelector(client, writeSelector, SelectionKey.OP_WRITE));
      writeSelector.wakeup();
    }
  }

  @Override
  public void flagNewRead(Client client) {
    ArgumentVerifier.assertNotNull(client, "Client");
    if(isRunning() && clients.containsKey(client.getChannel())) {
      readScheduler.execute(new AddClientToSelector(client, readSelector, SelectionKey.OP_READ));
      readSelector.wakeup();
    }
  }

  /**
   * Runnable for the Acceptor thread.  This runs the acceptSelector on the AcceptorThread. 
   */
  private class AcceptRunner implements Runnable {
    @Override
    public void run() {
      if(isRunning()) {
        try {
          acceptSelector.selectedKeys().clear();
          acceptSelector.select();
          if(isRunning()) {
            for(SelectionKey sk: acceptSelector.selectedKeys()) {
                if(sk.isAcceptable()) {
                  ServerSocketChannel server = (ServerSocketChannel) sk.channel();
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
                } else if(sk.isReadable()) {
                  DatagramChannel server = (DatagramChannel) sk.channel();
                  final Server udpServer = servers.get(server);
                  udpServer.acceptChannel(server);
                }
            }
          }
        } catch (IOException e) {
          stopIfRunning();
        } catch (ClosedSelectorException e) {
          stopIfRunning();
        } finally {
          if(isRunning()) {
            acceptScheduler.execute(this);
          }
        }
      }
    }
  }

  /**
   * Runnable for the Read thread.  This runs the readSelector on the ReadThread. 
   */
  private class ReadRunner implements Runnable {
    @Override
    public void run() {
      if(isRunning()) {
        if(readThreadID == 0) {
          readThreadID = Thread.currentThread().getId();
        }
        try {
          readSelector.selectedKeys().clear();
          readSelector.select();
          if(isRunning() && ! readSelector.selectedKeys().isEmpty()) {
            for(SelectionKey sk: readSelector.selectedKeys()) {
              SocketChannel sc = (SocketChannel)sk.channel();
              final Client client = clients.get(sc);
              if(sc.isConnectionPending()) {
                try {
                  if(sc.finishConnect()) {
                    client.setConnectionStatus(null);
                    client.getChannel().register(readSelector, SelectionKey.OP_READ);
                    if(client.canWrite()) {
                      flagNewWrite(client);
                    }
                  }
                } catch(IOException e) {
                  client.setConnectionStatus(e);
                  removeClient(client);
                  client.close();
                }
              }
              if(client != null) {
                try {
                  ByteBuffer readByteBuffer = client.provideReadByteBuffer();
                  int origPos = readByteBuffer.position();
                  int read = sc.read(readByteBuffer);
                  if(read < 0) {
                    removeClient(client);
                    client.close();
                  } else if( read > 0){
                    stats.addRead(read);
                    readByteBuffer.position(origPos);
                    ByteBuffer resultBuffer = readByteBuffer.slice();
                    readByteBuffer.position(origPos+read);
                    resultBuffer.limit(read);
                    client.addReadBuffer(resultBuffer.asReadOnlyBuffer());
                    if(! client.canRead()) {
                      client.getChannel().register(readSelector, 0);
                    }
                  }
                } catch(Exception e) {
                  removeClient(client);
                  client.close();
                }
              }
            }
          }
        } catch (IOException e) {
          stopIfRunning();
        } finally {
          if(isRunning()) {
            readScheduler.execute(this);
          }
        }        
      }
    }
  }

  /**
   * Runnable for the Write thread.  This runs the writeSelector on the WriteThread. 
   */
  private class WriteRunner implements Runnable {
    @Override
    public void run() {
      if(isRunning()) {
        try {
          writeSelector.selectedKeys().clear();
          writeSelector.select();
          if(isRunning() && ! writeSelector.selectedKeys().isEmpty()) {
            for(SelectionKey sk: writeSelector.selectedKeys()) {
              SocketChannel sc = (SocketChannel)sk.channel();
              final Client client = clients.get(sc);
              if(client != null) {
                try {
                  int writeSize = sc.write(client.getWriteBuffer());
                  stats.addWrite(writeSize);
                  client.reduceWrite(writeSize);
                  if(! client.canWrite()) {
                    client.getChannel().register(writeSelector, 0);
                  }
                } catch(Exception e) {
                  removeClient(client);
                  client.close();
                }
              }
            }
          }
        } catch (IOException e) {
          stopIfRunning();
        } finally {
          if(isRunning()) {
            writeScheduler.execute(this);
          }
        }
      }
    }
  }

  @Override
  protected void connectClient(Client client) {
    readScheduler.execute(new AddClientToSelector(client, readSelector, SelectionKey.OP_CONNECT));
    readSelector.wakeup();
  }

  @Override
  protected void setClientThreadExecutor(Client client) {
    client.setClientsThreadExecutor(clientDistributer.getSubmitterForKey(client));
  }

  @Override
  protected void addServerToSelectors(Server server) {
    if(server.getServerType() == WireProtocol.TCP) {
      acceptScheduler.execute(new AddServerToSelector(server, acceptSelector, SelectionKey.OP_ACCEPT));
      acceptSelector.wakeup();
    } else {
      acceptScheduler.execute(new AddServerToSelector(server, acceptSelector, SelectionKey.OP_READ));
      acceptSelector.wakeup();  
    }
  }

  @Override
  protected void removeServerFromSelectors(Server server) {
    if(server.getServerType() == WireProtocol.TCP) {
      acceptScheduler.execute(new RemoveFromSelector(server.getSelectableChannel(), acceptSelector));
      acceptSelector.wakeup();
    } else {
      readScheduler.execute(new RemoveFromSelector(server.getSelectableChannel(), readSelector));
      readSelector.wakeup();  
    }
  }

  @Override
  protected void removeClientFromSelectors(Client client) {
    readScheduler.execute(new RemoveFromSelector(client.getChannel(), readSelector));
    writeScheduler.execute(new RemoveFromSelector(client.getChannel(), writeSelector));
    readSelector.wakeup();
    writeSelector.wakeup();
  }
}

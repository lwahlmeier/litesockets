package org.threadly.litesockets.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.threadly.concurrent.KeyDistributedExecutor;
import org.threadly.litesockets.Server;
import org.threadly.litesockets.SocketExecuterInterface;
import org.threadly.litesockets.SocketExecuterInterface.WireProtocol;


/**
 * This is a UDP socket implementation for litesockets.  This UDPServer is treated like a
 * TCPServer.  It will notify the ClientAcceptor any time a new unique ip:port send a packet to this
 * UDP socket.  The UDPServer does not technically "Accept" new connections it just reads data from the socket
 * and that data also has the host/port of where it came from.
 * 
 * You can also just create a {@link UDPClient} from a server to initiate a connection to another UDP server, if
 * that server sends data back from that same port/ip pair it will show up as a read in the created client.
 */
public class UDPServer extends Server {
  
  protected final DatagramChannel channel;
  private volatile ClientAcceptor clientAcceptor;
  private KeyDistributedExecutor clientDistributer;
  private final ConcurrentHashMap<SocketAddress, UDPClient> clients = new ConcurrentHashMap<SocketAddress, UDPClient>();
  private volatile ServerCloser closer;
  protected volatile Executor sei;
  protected volatile SocketExecuterInterface se;
  protected AtomicBoolean closed = new AtomicBoolean(false);
  
  
  public UDPServer(String host, int port) throws IOException {
    channel = DatagramChannel.open();
    channel.socket().bind(new InetSocketAddress(host, port));
    channel.configureBlocking(false);
  }
  
  @Override
  public void setThreadExecutor(Executor sei) {
    this.sei = sei;
    clientDistributer = new KeyDistributedExecutor(sei);
  }

  @Override
  public void setSocketExecuter(SocketExecuterInterface se) {
    this.se = se;
  }

  @Override
  public SocketExecuterInterface getSocketExecuter() {
    return this.se;
  }

  @Override
  public ServerCloser getCloser() {
    return closer;
  }

  @Override
  public void setCloser(ServerCloser closer) {
    this.closer = closer;
  }

  @Override
  public void acceptChannel(SelectableChannel c) {
    if(c == channel) {
      final ByteBuffer bb = ByteBuffer.allocate(1500);
      try {
        final SocketAddress sa = channel.receive(bb);
        bb.flip();
        sei.execute(new Runnable() {
          @Override
          public void run() {
            if(! clients.containsKey(sa)) {
              UDPClient udpc = new UDPClient(sa, UDPServer.this);
              udpc = clients.putIfAbsent(sa, udpc);
              if(udpc == null) {
                udpc = clients.get(sa);
                udpc.setClientsThreadExecutor(clientDistributer.getSubmitterForKey(udpc));
                udpc.setClientsSocketExecuter(se);
                clientAcceptor.accept(udpc);
              }
            }
            UDPClient udpc = clients.get(sa);
            if(udpc.canRead()) {
              udpc.addReadBuffer(bb);
            }
          }});
      } catch (IOException e) {

      }
    }
  }

  @Override
  public WireProtocol getServerType() {
    return WireProtocol.UDP;
  }

  @Override
  public SelectableChannel getSelectableChannel() {
    return channel;
  }

  @Override
  public ClientAcceptor getClientAcceptor() {
    return clientAcceptor;
  }

  @Override
  public void setClientAcceptor(ClientAcceptor clientAcceptor) {
    this.clientAcceptor = clientAcceptor;
  }

  @Override
  public void close() {
    try {
      channel.close();
    } catch (IOException e) {
      //Dont Care
    } finally {
      this.callCloser();
    }
  }
  
  protected void callCloser() {
    if(sei != null && closer != null) {
      sei.execute(new Runnable() {
        @Override
        public void run() {
          getCloser().onClose(UDPServer.this);
        }});
    }
  }
  
  public UDPClient createUDPClient(String host, int port) {
    InetSocketAddress sa = new InetSocketAddress(host,port);
    if(! clients.containsKey(sa)) {
      UDPClient c = new UDPClient(new InetSocketAddress(host, port), this);
      clients.putIfAbsent(sa, c);
      c.setClientsThreadExecutor(clientDistributer.getSubmitterForKey(c));
      c.setClientsSocketExecuter(se);
    }
    return clients.get(sa);
  }

}

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import thrift.Storage;

import java.util.ResourceBundle;

class Server {

    private static final Logger log = LogManager.getLogger(Server.class);

    void start() {
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("config");
            Integer port = Integer.parseInt(bundle.getString("server.port"));

            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);

            //TServerSocket serverTransport = new TServerSocket(port);

            StorageServiceHandler handler = new StorageServiceHandler();
//            Storage.AsyncProcessor<Storage.AsyncIface> processor =
//                    new Storage.AsyncProcessor<Storage.AsyncIface>(handler);
            TProcessor processor = new Storage.AsyncProcessor<>(handler);

            TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                    processor(processor));
//            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            log.info("Service started on port: " + ((TNonblockingServerSocket) serverTransport).getPort());
            server.serve();
        } catch (TTransportException e) {
            log.error(e.getMessage());
        }
    }

}

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This program is a worker program that runs alongside the master program to
 * compute possible thread integers. Each worker must run on an individual
 * computer. If the program itself is run with no arguments then it will
 * run on the default port. If a port needs to be specified, it can 
 * be inputted as a command line argument.
 * 
 * @author kemalturksonmez
 *
 */
public class BigPrimeWorker {
	/**
	 * Default listening port, if none is specified on the command line.
	 */
	private static final int DEFAULT_PORT = 65000;

	/**
	 * The first and only word on a message representing a close command.
	 */
	private static final String CLOSE_CONNECTION_COMMAND = "close";

	/**
	 * Used to signal an incoming BigPrimeTask. The message will contain the string
	 * "task" then will be followed by a big prime.
	 */
	private static final String TASK_COMMAND = "task";

	/**
	 * Used to signal an outgoing BigPrimeResult. The message will contain the string
	 * "result" then will be followed by a big prime.
	 */
	private static final String RESULT_COMMAND = "result";

	/**
	 * Certainty factor: A probable prime found by this program has a
	 * probability of 1 - (2 raised to the power minus CERTAINTY) of being
	 * prime.
	 */
	private final static int CERTAINTY = 100;

	private static volatile boolean shutdownCommandReceived;

	private static ArrayBlockingQueue<BigInteger> testingQueue; // queue of
																// candidates
	// to be tested

	private static ArrayBlockingQueue<BigInteger> writingQueue; // queue of
																// candidates
	// to be tested

	private final static int cores = Runtime.getRuntime().availableProcessors() - 1;

	public static void main(String[] args) {
		int port = DEFAULT_PORT;

		// If a port besides the default port is specified
		if (args.length != 0) {
			try {
				port = Integer.parseInt(args[0]);
				if (port < 0 || port > 65535)
					throw new NumberFormatException();
			} catch (NumberFormatException e) {
				port = DEFAULT_PORT;
			}
		} // end if

		// Identify that client is listening on a special port
		try {
			System.out.println("Starting listening with " + InetAddress.getLocalHost() + ":" + port);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// initializes blocking queue
		writingQueue = new ArrayBlockingQueue<BigInteger>(cores);
		testingQueue = new ArrayBlockingQueue<BigInteger>(cores);

		// Creates worker threads
		Worker[] worker = new Worker[cores];
		for (int i = 0; i < worker.length; i++) {
			worker[i] = new Worker();
		}

		// Thread objects to communicate with master program
		SupervisorCommunicator sc = null;
		SupervisorListener sl = null;

		// Listen for a connection request from the master program. 
		ServerSocket listener = null;
		try {
			listener = new ServerSocket(port);
		} catch (Exception e) {
			System.out.println("ERROR: Can't create listening socket on port " + port);
			System.exit(1);
		}

		try {
			Socket connection = listener.accept();
			listener.close();
			System.out.println("Accepted connection from " + connection.getInetAddress() + ":" + port);

			sc = new SupervisorCommunicator(connection);
			sl = new SupervisorListener(connection);

		} catch (Exception e) {
			System.out.println("ERROR: Server shut down with error:");
			System.out.println(e);
			System.exit(2);
		}

		while (!shutdownCommandReceived) {
		} // Blocks from ending until shutdown command is received.

		try {
			closeAll(sc, sl, worker, sl.getSocket());
		} catch (InterruptedException e) {

		} catch (IOException e) {

		}

	}

	/**
	 * Stops all threads from running and closes connection with socket
	 * 
	 * @param sc
	 * @param sl
	 * @param worker
	 * @param connection
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void closeAll(Thread sc, Thread sl, Thread[] worker, Socket connection)
			throws InterruptedException, IOException {

		for (int i = 0; i < worker.length; i++) {
			worker[i].interrupt();
			while (worker[i].isAlive()) {
				worker[i].join();
			}
		}

		sc.interrupt();
		while (sc.isAlive()) {
			sc.join();
		}
		sl.interrupt();
		while (sl.isAlive()) {
			sl.join();
		}
		System.out.println("All threads have been stopped");

		System.out.println("Ending connection to " + connection.getInetAddress() + ":" + connection.getLocalPort());
		connection.close();

		System.out.println("Worker shutting down");
	}

	/**
	 * Sends results back to the supervisor
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class SupervisorCommunicator extends Thread {
		Socket connection;
		ObjectOutputStream objOut;

		/**
		 * creates SupervisorCommunicator object
		 * 
		 * @param connection
		 * @throws IOException
		 */
		SupervisorCommunicator(Socket connection) throws IOException {
			this.connection = connection;
			objOut = new ObjectOutputStream(connection.getOutputStream());
			start();
		}

		/**
		 * reports result back to master program
		 */
		public void run() {
			while (!shutdownCommandReceived) {
				try {
					objOut.writeObject((String) RESULT_COMMAND);
					objOut.writeObject((String) writingQueue.take().toString());
					objOut.flush();
				} catch (IOException e) {

				} catch (InterruptedException e) {

				}
			}
		}

	}

	/**
	 * Listens to the supervisor for incoming prime numbers or shutdown commands
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class SupervisorListener extends Thread {
		Socket connection;
		ObjectInputStream objIn;

		/**
		 * Creates a SupervisorListener object
		 * 
		 * @param connection
		 * @throws IOException
		 */
		SupervisorListener(Socket connection) throws IOException {
			this.connection = connection;
			objIn = new ObjectInputStream(connection.getInputStream());
			start();
		}

		/**
		 * reads commands from master program
		 */
		public void run() {
			String in = "";
			BigInteger n = null;
			while (!shutdownCommandReceived) {

				try {
					in = (String) objIn.readObject();
				} catch (ClassNotFoundException | IOException e) {

				}

				if (in.equals(CLOSE_CONNECTION_COMMAND)) {
					shutdownCommandReceived = true;

				} else if (in.equals(TASK_COMMAND)) {
					try {
						n = new BigInteger((String) objIn.readObject());
						testingQueue.put(n);
					} catch (ClassNotFoundException | InterruptedException | IOException e) {
					}
				}
			}
		}

		/**
		 * returns socket of thread
		 * 
		 * @return
		 */
		public Socket getSocket() {
			return connection;
		}

	}

	/**
	 * Performs parallel computations
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class Worker extends Thread {
		Worker() {
			this.start();
		}

		/**
		 * Checks whether a Big Integer could possibly be prime
		 */
		public void run() {
			while (!shutdownCommandReceived) {
				BigInteger n = null;
				try {
					if (testingQueue.size() > 0) {
						n = testingQueue.take();
						if (n.isProbablePrime(CERTAINTY)) {
							try {
								writingQueue.put(n);
							} catch (InterruptedException e) {

							}
						}
					}
				} catch (InterruptedException e) {

				}

			}
		}
	}
}

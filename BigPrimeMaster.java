import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This program can either calculate possible Big Prime integers using multiple
 * threads on one system or using a multi-threaded distributed system. To 
 * calculate Big Prime integers on one computer, solely run the master program.
 * To run on a distributed system, run the BigPrimeWorker on one or multiple 
 * computers on a connected network. Then run the BigPrimeMaster with command
 * line arguments for each computer. If the program is given only ip addresses 
 * for each command line argument, it will assume it's communicating over the 
 * default port. If a port needs to be specified, then input structure is
 * IP:Port where ip and specific port are separated by a colon.
 * 
 * @author kemalturksonmez
 *
 */
public class BigPrimeMaster {
	/**
	 * Default listening port, if none is specified on the command line.
	 */
	private static final int DEFAULT_PORT = 65000;

	/**
	 * The first and only word on a message representing a close command.
	 */
	private static final String CLOSE_CONNECTION_COMMAND = "close";

	/**
	 * Used to signal an outgoing BigPrimeTask. The message will contain the string
	 * "task" then will be followed by a big prime.
	 */
	private static final String TASK_COMMAND = "task";

	/**
	 * Used to signal an incoming BigPrimeResult. The message will contain the string
	 * "result" then will be followed by a big prime.
	 */
	private static final String RESULT_COMMAND = "result";

	/**
	 * The program searches for prime numbers containing this specified amount of bits.
	 */
	private final static int BITS = 2048;

	/**
	 * Certainty factor: A probable prime found by this program has a
	 * probability of 1 - (2 raised to the power minus CERTAINTY) of being
	 * prime.
	 */
	private final static int CERTAINTY = 100;

	/**
	 * Number of minutes for the program to run. The time will actually be a
	 * little longer: At the end, the program waits for threads to complete
	 * their current tasks before exiting.
	 */
	private final static double MINUTES = 1.0;

	/**
	 * Number of worker threads to use.
	 */
	private final static int WORKERS = 4;

	private static final BigInteger TWO = new BigInteger("2"); // 2, as a
																// BigInteger

	private static ArrayBlockingQueue<BigInteger> queue; // queue of candidates
															// to be tested
	private static volatile boolean running = true; // will be set to false to
													// terminate threads
	private static int primeCount = 0; // number of primes found

	public static void main(String[] args) {

		queue = new ArrayBlockingQueue<BigInteger>(20);
		Supervisor boss = new Supervisor();
		boss.start(); // start boss thread

		// Holds thread object that handle communications with individual
		// threads
		WorkerCommunicator[] wc = new WorkerCommunicator[args.length];
		WorkerListener[] wl = new WorkerListener[args.length];

		// number of cores in the master system
		int cores = Runtime.getRuntime().availableProcessors() - 1;
		// Holds thread workers for master class
		SingleSystem[] workers = new SingleSystem[cores];

		// timer variable to initiate timed shutdown
		Timer timer = new Timer(true); // the timer that will stop the

		if (args.length == 0) { // Run non-distributed computation.
			System.out.println("Running on this computer only...");

			// creates thread objects to work on master computer
			for (int i = 0; i < workers.length; i++) {
				workers[i] = new SingleSystem();
				workers[i].start();
			}

			// timer shutdown when running only on master computer
			timer.schedule(new TimerTask() {
				public void run() {
					System.out.println("Commencing shutdown sequence");
					running = false;
					for (int i = 0; i < workers.length; i++) {
						workers[i].interrupt();
					}
					boss.interrupt();
					// If single system was run
					for (int i = 0; i < workers.length; i++) {
						while (workers[i].isAlive()) {
							try {
								workers[i].join();
							} catch (InterruptedException e) {

							}
						}
					}
					while (boss.isAlive()) {
						try {
							boss.join();
						} catch (InterruptedException e) {

						}
					}
					System.out.println("System shutting down");
				}
			}, (int) (60000 * MINUTES));

		} else { // Run a distributed computation.
			for (int i = 0; i < args.length; i++) {
				Socket socket = null; // The socket for the connection.
				String host = args[i];
				int port = DEFAULT_PORT;

				// If a port beyond the one that is defined is added
				int pos = host.indexOf(':');
				if (pos >= 0) {
					// The host string contains a ":", which should be
					// followed by the port number.
					String portString = host.substring(pos + 1);
					host = host.substring(0, pos); // Remove port from host
													// string.
					try {
						port = Integer.parseInt(portString);
					} catch (NumberFormatException e) {
					}
				}

				int threadID = i + 1;

				try {
					socket = new Socket(host, port); // open the connection.
					System.out.println("Accepted connection from " + socket.getInetAddress() + ":" + port);
					wc[i] = new WorkerCommunicator(socket, threadID);
					wl[i] = new WorkerListener(socket, threadID);
				} catch (Exception e) {
					System.out.println("Thread " + threadID + " could not open connection to " + host + ":" + port);
					System.out.println("   Error: " + e);

				}
			}
			// threads;
			// the parmeter makes the times use a daemon thread
			timer.schedule(new TimerTask() {
				// This task's run() method will be called when the specified
				// number of minutes have passed. It sets the global variable
				// running to false to signal all the threads the exits.
				// It interrupts the supervisor thread, since it is probably
				// blocked and needs to be woken up so that it can exit.
				// To be safe, it also interrupts the worker threads, although
				// that is probably not necessary.
				public void run() {
					System.out.println("Commencing shutdown sequence");
					running = false;
					for (int i = 0; i < wc.length && i < wl.length; i++) {
						try {
							wc[i].closeCommunication(); // attempts to close
														// connection with
														// client
						} catch (IOException e1) {

						}

						wl[i].interrupt();
						wc[i].interrupt();
					} // end for

					boss.interrupt();

					// Waits until all threads are dead
					try {
						for (int i = 0; i < wc.length && i < wl.length; i++) {
							// Wait for all the threads to terminate.
							while (wc[i].isAlive()) {
								wc[i].join();
							}
							while (wl[i].isAlive()) {
								wl[i].join();
							}
						}
						boss.join();

					} catch (InterruptedException e) {
					}
					for (int i = 0; i < wc.length; i++) {
						try {
							wc[i].socket.close();
						} catch (IOException e) {

						}
					}

					System.out.println("System shutting down");

				}
			}, (int) (60000 * MINUTES));
		}
	}

	/**
	 * Called by a worker thread when it finds a probable prime.
	 */
	synchronized private static void foundPrime(BigInteger n) {
		primeCount++;
		System.out.printf("%nFound %d-bit prime number %d:%n", BITS, primeCount);
		System.out.printf("     %s%n%n", n.toString());
	}

	/**
	 * Creates a supervisor thread to add potential prime numbers to the
	 * blocking array.
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class Supervisor extends Thread {
		Random rand = new Random();

		/**
		 * Adds primes to blocking queue
		 */
		public void run() {
			BigInteger n = pickStartValue();
			while (running) {
				try {
					queue.put(n);
					if (Math.random() < 0.001)
						n = n.add(TWO);
					else
						n = pickStartValue();
				} catch (InterruptedException e) {
				}
			}
		}

		/**
		 * picks a random start value based on bits and random generated number
		 * 
		 * @return
		 */
		private BigInteger pickStartValue() {
			BigInteger n = new BigInteger(BITS, rand);
			n = n.setBit(0); // Make sure n is odd.
			n = n.setBit(BITS - 1); // Make sure n has BITS bits.
			return n;
		}
	}

	/**
	 * Sends commands to a specific socket
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class WorkerCommunicator extends Thread {
		int id; // thread id
		Socket socket;
		ObjectOutputStream objOut;

		/**
		 * Creates a WorkerCommunicator object
		 * 
		 * @param socket
		 * @param id
		 * @throws IOException
		 */
		WorkerCommunicator(Socket socket, int id) throws IOException {
			this.id = id;
			this.socket = socket;
			objOut = new ObjectOutputStream(socket.getOutputStream());
			this.start();
		}

		/**
		 * Writes potential primes to worker object communicator
		 */
		public void run() {
			while (running) {
				try {
					objOut.writeObject((String) TASK_COMMAND);
					objOut.writeObject((String) queue.take().toString());
					objOut.flush();
				} catch (IOException e) {
				} catch (InterruptedException e) {
				}
			}
		}

		/**
		 * Starts shutdown sequence for that thread's socket specific socket
		 * 
		 * @throws IOException
		 */
		public void closeCommunication() throws IOException {
			objOut.writeObject((String) CLOSE_CONNECTION_COMMAND);
			objOut.flush();
			System.out.println("Ending connection with thread " + id);
		}

	}

	/**
	 * Recieves inputs from a single socket thread
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class WorkerListener extends Thread {
		int id; // thread id
		Socket socket;
		ObjectInputStream objIn;

		/**
		 * Creates a WorkerListener object
		 * 
		 * @param socket
		 * @param id
		 * @throws IOException
		 */
		WorkerListener(Socket socket, int id) throws IOException {
			this.id = id;
			this.socket = socket;
			objIn = new ObjectInputStream(socket.getInputStream());
			this.start();
		}

		/**
		 * reads prime results from a thread's socket connection
		 */
		public void run() {
			String in = "";
			BigInteger n = null;
			while (running) {

				try {
					in = (String) objIn.readObject();
					if (in.equals(RESULT_COMMAND))
						in = (String) objIn.readObject();
					n = new BigInteger(in);
					foundPrime(n);
				} catch (ClassNotFoundException | IOException e) {
				}

			}
		}
	}

	/**
	 * Runs when only master program is run without any workers
	 * 
	 * @author kemalturksonmez
	 *
	 */
	private static class SingleSystem extends Thread {

		/**
		 * Tests whether a specific big integer number is prime
		 */
		public void run() {
			while (running) {
				BigInteger n = null;
				try {
					if (queue.size() > 0) {
						n = queue.take();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				}
				if (n.isProbablePrime(CERTAINTY)) {
					foundPrime(n);

				}
			}
		}
	}

}

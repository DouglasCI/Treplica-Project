#![allow(dead_code, unused_assignments)] 

use std::{
    thread, 
    sync::{mpsc, mpsc::TryRecvError::{Empty, Disconnected}}, 
    time::{Duration, Instant, SystemTime},
    fmt::Debug, 
    env,
    fs::File,
    io::{Write, BufWriter}
};
// use rand::{thread_rng, Rng, distributions::Alphanumeric};

#[derive(Clone, Debug)]
struct Data<T, U> {
    write: T,
    message: U
}

// Debug mode
static mut DEBUG_MODE: bool = false;

const ONE_THOUSAND: u128 = u128::pow(10, 3);
// Main
const PRODUCER_DELAY: u64 = ONE_THOUSAND as u64;
// Disk
const DISK_DELAY: u128 = 5 * ONE_THOUSAND;
const PATIENCE: u128 = DISK_DELAY + 2 * ONE_THOUSAND;
const MIN_BUFFER_SIZE: usize = 1;
// Network
const NETWORK_DELAY: u128 = ONE_THOUSAND;
const MSGS_PER_INTERVAL: usize = 3;

fn main() {
    /* Run in debug mode or not. */
    let args: Vec<_> = env::args().collect();
    unsafe {
        if args.len() > 1 && args[1] == "--debug" { DEBUG_MODE = true }
        if DEBUG_MODE {
            println!("Debug mode is: ON");
        } else {
            println!("Debug mode is: OFF\n(To activate debug mode use \"cargo run -- --debug\")\n")
        }
    }

    /* Create MSPC channels. */
    // Producer -> Disk
    let (tx_prod, rx_disk) = mpsc::channel();
    // Disk -> Network
    let (tx_disk, rx_net) = mpsc::channel();

    /* Start consumer threads. */
    // Disk
    thread::spawn(move || { consumer_disk(rx_disk, tx_disk); });
    // Network
    thread::spawn(move || { consumer_network(rx_net); });
    
    // Loop to generate and send data to consumer.
    loop {
        tx_prod.send(producer()).unwrap();
        thread::sleep(Duration::from_millis(PRODUCER_DELAY));
    }
}

/* ----------------------------- AUXILIARY FUNCTIONS ----------------------------- */

// Get elapsed time since 1970-01-01 00:00:00 UTC in milliseconds.
fn get_unix_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap().as_millis()
}

// Create log file.
fn create_log_file(file_type: &str) -> BufWriter<File> {
    let file_name = format!("{}_log.txt", file_type);
    let error_msg = format!("@[CONSUMER] #ERROR: Unable to create {} log file.", file_type);
    let log_file:File = File::create(file_name)
                                .expect(error_msg.as_str());
    BufWriter::new(log_file) //minimizes system calls
}

// Write to log file.
fn write_to_log_file<U: Debug>(log_file: &mut BufWriter<File>, buffer: &Vec<U>) {
    let now_timestamp = get_unix_timestamp();
    for prod_timestamp in buffer {
        writeln!(log_file, "{:?} {:?}", prod_timestamp, now_timestamp)
            .expect("@[CONSUMER] #ERROR: Unable to write to log file.");
    }
    log_file.flush().unwrap();
}

/* ----------------------------- PRODUCER RELATED ----------------------------- */
fn producer() -> Data<u128, u128> {
    // let w: String = thread_rng()
    // .sample_iter(&Alphanumeric)
    // .take(4)
    // .map(char::from)
    // .collect();

    let timestamp = get_unix_timestamp();
    let data = Data{ write: timestamp, message: timestamp };

    unsafe { if DEBUG_MODE { println!("@[PRODUCER]>> Generated [{:?}].", data) } }

    data
}

/* ----------------------------- DISK RELATED ----------------------------- */
fn consumer_disk<T: Clone + Debug, U: Clone + Debug>
                (rx_disk: mpsc::Receiver<Data<T, U>>, tx_disk: mpsc::Sender<Vec<U>>) {
    // Structures
    let mut disk: Vec<T> = vec![]; //false disk
    let mut disk_buffer: Vec<T> = vec![]; //disk buffer
    let mut network_buffer: Vec<U> = vec![]; //message buffer to be sent to network
    
    // Network buffer send control
    let mut is_past_buffer_ready: bool = true;
    let mut unstable_msg_qty: usize = 0; 
    let mut stable_msg_qty: usize = 0;

    // Time control
    let mut clock = Instant::now();

    // Timestamps log
    let mut log_file = create_log_file("disk");
    
    loop {
        // Non-blocking receiver
        match rx_disk.try_recv() {
            Ok(data) => {
                // Received data in this iteration!
                disk_buffer.push(data.write);
                network_buffer.push(data.message);
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    println!("@[CONSUMER] #ERROR: Consumer disk thread died!");
                }
            }
        }

        let elapsed_time: u128 = clock.elapsed().as_millis();
        // Is the disk available now? 
        if elapsed_time >= DISK_DELAY {
            // Is the buffer full? Or is the disk idle for too long and the buffer is not empty?
            if disk_buffer.len() >= get_min_buffer_size() || (elapsed_time >= PATIENCE && !disk_buffer.is_empty()) {
                // Signals past buffer is ready to be sent through network.
                is_past_buffer_ready = !is_past_buffer_ready;
                
                // Number of disk-stable messages
                stable_msg_qty = unstable_msg_qty;
                unstable_msg_qty = disk_buffer.len();

                flush_to_disk(&mut disk_buffer, &mut disk);

                clock = Instant::now(); //refresh clock
            }

            // Since the disk is avaiable now, the past buffer is stable on the disk,
            if is_past_buffer_ready && stable_msg_qty > (0 as usize) {
                // so get the slice of the network_buffer that contains those stable messages,
                let to_be_sent_buffer: Vec<U> = network_buffer.drain(..stable_msg_qty).collect();
                // log the timestamps
                write_to_log_file(&mut log_file, &to_be_sent_buffer);
                // and start sending the past buffer through the network.
                tx_disk.send(to_be_sent_buffer).unwrap();

                // This buffer is done.
                is_past_buffer_ready = !is_past_buffer_ready;
                stable_msg_qty = 0;
            }
        }
    }
}

// Allows dynamic buffer size.
fn get_min_buffer_size() -> usize { MIN_BUFFER_SIZE }

// Write buffer to disk.
fn flush_to_disk<T: Debug>(disk_buffer: &mut Vec<T>, disk: &mut Vec<T>) {
    unsafe {
        if DEBUG_MODE {
            println!{"@[DISK]>> Disk contains {:?}.", disk};
            println!{"@[DISK]>> Disk is now writing {:?}.", &disk_buffer};
        }
    }

    disk.append(disk_buffer);
}

/* ----------------------------- NETWORK RELATED ----------------------------- */
fn consumer_network<U: Clone + Debug>(rx_net: mpsc::Receiver<Vec<U>>) {
    // Structures
    let mut network_buffer: Vec<U> = vec![]; //buffer for all messages
    let mut network: Vec<U> = vec![]; //sent messages history

    // Time control
    let mut clock = Instant::now();

    // Timestamps log
    let mut log_file = create_log_file("network");

    loop {
        // Non-blocking receiver
        match rx_net.try_recv() {
            Ok(messages) => {
                // Received data in this iteration!
                network_buffer.extend(messages);
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    println!("@[CONSUMER] #ERROR: Consumer network thread died!");
                }
            }
        }
        
        // Send small slices of the network_buffer.
        let elapsed_time: u128 = clock.elapsed().as_millis();
        let msg_qty = get_msgs_per_interval();
        if elapsed_time >= NETWORK_DELAY && network_buffer.len() >= msg_qty {
            let mut v = network_buffer.drain(..msg_qty).collect();

            // Write the timestamps to log file.
            write_to_log_file(&mut log_file, &v);

            send_to_network(&mut v, &mut network);
            
            clock = Instant::now(); //refresh clock
        }
    }
}

// Allows dynamic amount of messages sent to network per interval.
fn get_msgs_per_interval() -> usize { MSGS_PER_INTERVAL }

// Send messages through the network.
fn send_to_network<U: Debug>(network_buffer: &mut Vec<U>, network: &mut Vec<U>) {
    unsafe { if DEBUG_MODE { println!{"@[NETWORK]>> Sent {:?}.", &network_buffer} } }

    network.append(network_buffer);

    unsafe { if DEBUG_MODE { println!{"@[NETWORK]>> Message history {:?}.", network} } }
}

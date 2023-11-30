#![allow(dead_code, unused_assignments)] 

use std::{
    thread, 
    sync::{mpsc, mpsc::TryRecvError::{Empty, Disconnected}}, 
    time::{Duration, Instant, SystemTime},
    fmt::Debug, 
    env,
    fs::File,
    io::{Write, BufWriter},
};
// use rand::{thread_rng, Rng, distributions::Alphanumeric};

#[derive(Clone, Debug)]
struct Data<T, U> {
    write: T,
    message: U
}

enum DataWrapper<T, U> {
    Data(Data<T, U>),
    Shutdown,
}

enum MessageWrapper<U> {
    Message(Vec<U>),
    Shutdown,
}

/* ----------------------------- OPTION PARSING FUNCTIONS ----------------------------- */
fn args_help() {
    println!("--help | -h => Show help message.
--debug | -d => Show debug messages during execution (default = false).
--producer_delay {{Int}} | -pd {{Int}} => Set producer delay in milliseconds (default = 1000).
--num_operations {{Int}} | -no {{Int}} => Set size of data to be generated (default = 50).
--disk_delay {{Int}} | -dd {{Int}} => Set disk delay in seconds (default = 5000).
--msgs_per_interval {{Int}} | -mpi {{Int}} => Set number of messages sent per network interval (default = 2).");
}

fn args_error(error_msg: &str) {
    eprintln!("Error in execution options: {}.", error_msg);
    args_help();
}

fn parse_args(args: Vec<String>) -> Option<(bool, u64, u128, u128, usize)> {
    let mut debug_mode: bool = false;
    let mut producer_delay: u64 = 1000;
    let mut num_operations: u128 = 50;
    let mut disk_delay: u128 = 5000;
    let mut msgs_per_interval: usize = 2;

    let mut i = 1;
    while i < args.len() {
        let option = &args[i];
        match option.as_str() {
            "--help" | "-h" => {
                args_help();
                return None;
            }
            "--debug" | "-d" => { debug_mode = true; }
            "--producer_delay" | "-pd" => {
                if i + 1 < args.len() {
                    let val = &args[i + 1];
                    let num: u64 = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--producer_delay option requires a number");
                            return None;
                        },
                    };
                    producer_delay = num;
                    i += 1;
                } else {
                    args_error("--producer_delay option requires a number");
                    return None;
                }
            }
            "--num_operations" | "-no" => {
                if i + 1 < args.len() {
                    let val = &args[i + 1];
                    let num: u128 = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--num_operations option requires a number");
                            return None;
                        },
                    };
                    num_operations = num;
                    i += 1;
                } else {
                    args_error("--num_operations option requires a number");
                    return None;
                }
            }
            "--disk_delay" | "-dd" => {
                if i + 1 < args.len() {
                    let val = &args[i + 1];
                    let num: u128 = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--disk_delay option requires a number");
                            return None;
                        },
                    };
                    disk_delay = num;
                    i += 1;
                } else {
                    args_error("--disk_delay option requires a number");
                    return None;
                }
            }
            "--msgs_per_interval" | "-mpi" => {
                if i + 1 < args.len() {
                    let val = &args[i + 1];
                    let num: usize = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--msgs_per_interval option requires a number");
                            return None;
                        },
                    };
                    msgs_per_interval = num;
                    i += 1;
                } else {
                    args_error("--msgs_per_interval option requires a number");
                    return None;
                }
            }
            _ => {
                args_error("unknown option");
                return None;
            }
        }
        i += 1;
    }

    Some((debug_mode, producer_delay, num_operations, disk_delay, msgs_per_interval))
}

/* ----------------------------- MAIN ----------------------------- */
fn main() {
    /* Collect all options for this run. */
    let args: Vec<String> = env::args().collect();
    let (debug_mode, producer_delay, num_operations, disk_delay, msgs_per_interval) =
        match parse_args(args) {
            Some(x) => x,
            None => { return; },
        };

    if debug_mode {
        println!("Debug mode is: ON");
    } else {
        println!("Debug mode is: OFF\n(To activate debug mode use \"cargo run -- --debug\")\n")
    }

    println!("@[MAIN] Program is executing...");

    /* Create MSPC channels. */
    // Producer -> Disk
    let (tx_prod, rx_disk) = mpsc::channel();
    // Disk -> Network
    let (tx_disk, rx_net) = mpsc::channel();

    /* Start consumer threads. */
    // Disk
    let h1 = thread::spawn(move || { consumer_disk(rx_disk, tx_disk, disk_delay, debug_mode); });
    // Network
    let h2 = thread::spawn(move || { consumer_network(rx_net, disk_delay, msgs_per_interval, debug_mode); });
    
    // Loop to generate and send data to consumer.
    for _ in 0..num_operations {
        tx_prod.send(producer(debug_mode)).unwrap();
        thread::sleep(Duration::from_millis(producer_delay));
    }

    // Tell threads to finish their work.
    tx_prod.send(DataWrapper::Shutdown).unwrap();

    h1.join().unwrap();
    h2.join().unwrap();
}

/* ----------------------------- AUXILIARY FUNCTIONS ----------------------------- */
// Get elapsed time since 1970-01-01 00:00:00 UTC in milliseconds.
fn get_unix_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap().as_millis()
}

fn create_log_file(file_type: &str) -> BufWriter<File> {
    let file_name = format!("{}_log.txt", file_type);
    let error_msg = format!("@[CONSUMER] #ERROR: Unable to create {} log file.", file_type);
    let log_file:File = File::create(file_name)
                                .expect(error_msg.as_str());
    BufWriter::new(log_file) //minimizes system calls
}

fn write_to_log_file<U: Debug>(log_file: &mut BufWriter<File>, buffer: &Vec<U>) {
    let now_timestamp = get_unix_timestamp();
    for prod_timestamp in buffer {
        writeln!(log_file, "{:?} {:?}", prod_timestamp, now_timestamp)
            .expect("@[CONSUMER] #ERROR: Unable to write to log file.");
    }
}

/* ----------------------------- PRODUCER RELATED ----------------------------- */
fn producer(debug_mode: bool) -> DataWrapper<u128, u128> {
    // let w: String = thread_rng()
    // .sample_iter(&Alphanumeric)
    // .take(4)
    // .map(char::from)
    // .collect();

    let timestamp = get_unix_timestamp();
    let data = Data{ write: timestamp, message: timestamp };

    if debug_mode { println!("@[PRODUCER]>> Generated [{:?}].", data) }

    DataWrapper::Data(data)
}

/* ----------------------------- DISK RELATED ----------------------------- */
fn consumer_disk<T: Clone + Debug, U: Clone + Debug>
        (rx_disk: mpsc::Receiver<DataWrapper<T, U>>, 
        tx_disk: mpsc::Sender<MessageWrapper<U>>, 
        disk_delay: u128, debug_mode: bool) {
    // Structures
    let mut disk: Vec<T> = vec![]; //false disk
    let mut disk_buffer: Vec<T> = vec![]; //disk buffer
    let mut network_buffer: Vec<U> = vec![]; //message buffer to be sent to network
    
    // Network buffer send control
    let mut unstable_msg_qty: usize = 0; 
    let mut stable_msg_qty: usize = 0;

    // Control
    let mut clock = Instant::now();
    let mut shutdown = false;

    // Timestamps log
    let mut log_file = create_log_file("disk");

    loop {
        // Non-blocking receiver
        match rx_disk.try_recv() {
            // Received data in this iteration!
            Ok(data) => match data {
                DataWrapper::Data(dd) => {
                    disk_buffer.push(dd.write);
                    network_buffer.push(dd.message);
                },
                DataWrapper::Shutdown => { shutdown = true; }
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    if !shutdown {
                        println!("@[CONSUMER] #ERROR: Producer thread died!");
                    }
                }
            }
        }

        let elapsed_time: u128 = clock.elapsed().as_millis();
        // Is the disk available now? 
        if elapsed_time >= disk_delay {
            // Number of disk-stable messages
            stable_msg_qty = unstable_msg_qty;
            unstable_msg_qty = disk_buffer.len();

            flush_to_disk(&mut disk_buffer, &mut disk, debug_mode);

            clock = Instant::now(); //refresh clock

            // If we have disk-stable messages,
            if stable_msg_qty > (0 as usize) {
                // get the slice of the network_buffer that contains those stable messages,
                let to_be_sent_buffer: Vec<U> = network_buffer.drain(..stable_msg_qty).collect();
                // log the timestamps,
                write_to_log_file(&mut log_file, &to_be_sent_buffer);
                // and start sending the past buffer through the network.
                tx_disk.send(MessageWrapper::Message(to_be_sent_buffer)).unwrap();

                // The stable buffer is done.
                stable_msg_qty = 0;
            }

            if shutdown && unstable_msg_qty == 0 {
                tx_disk.send(MessageWrapper::Shutdown).unwrap();
                println!("@[CONSUMER] #SHUTDOWN: Finished work in disk thread.");
                break;
            }
        }
    }
}

// Write buffer to disk.
fn flush_to_disk<T: Debug>(disk_buffer: &mut Vec<T>, disk: &mut Vec<T>, debug_mode: bool) {
    if debug_mode {
        println!{"@[DISK]>> Disk contains {:?}.", disk};
        println!{"@[DISK]>> Disk is now writing {:?}.", &disk_buffer};
    }

    disk.append(disk_buffer);
}

/* ----------------------------- NETWORK RELATED ----------------------------- */
fn consumer_network<U: Clone + Debug>(rx_net: mpsc::Receiver<MessageWrapper<U>>, 
        disk_delay: u128, msgs_per_interval: usize, debug_mode: bool) {
    // Structures
    let mut network_buffer: Vec<U> = vec![]; //buffer for all messages
    let mut network: Vec<U> = vec![]; //sent messages history

    // Control
    let mut clock = Instant::now();
    let mut send_interval: u128 = 0;
    let mut num_sends: u128 = 0;
    let mut shutdown = false;

    // Timestamps log
    let mut log_file = create_log_file("network");

    loop {
        // Non-blocking receiver
        match rx_net.try_recv() {
            // Received data in this iteration!
            Ok(messages) => match messages {
                MessageWrapper::Message(mm) => {
                    network_buffer.extend(mm);
                },
                MessageWrapper::Shutdown => { shutdown = true; }
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    if !shutdown {
                        println!("@[CONSUMER] #ERROR: Consumer disk thread died!");
                    }
                }
            }
        }

        if shutdown && network_buffer.is_empty() {
            println!("@[CONSUMER] #SHUTDOWN: Finished work in network thread.");
            break;
        }
        
        // Send small slices of the network_buffer.
        let buffer_size = network_buffer.len();
        if buffer_size > (0 as usize) {
            let elapsed_time: u128 = clock.elapsed().as_millis();
            if elapsed_time >= send_interval {
                // Only recalculate when the entire buffer is sent
                if num_sends == 0 {
                    num_sends = ((buffer_size + (msgs_per_interval - 1)) / msgs_per_interval) as u128;
                    send_interval = disk_delay / num_sends;
                }
                
                let drain_range = std::cmp::min(msgs_per_interval, buffer_size);
                let mut v = network_buffer.drain(..drain_range).collect();

                write_to_log_file(&mut log_file, &v);

                send_to_network(&mut v, &mut network, debug_mode);
                
                num_sends -= 1; //decrement number of sends left for this buffer
                clock = Instant::now(); //refresh clock
            }
        }
    }
}

// Send messages to the network layer.
fn send_to_network<U: Debug>(network_buffer: &mut Vec<U>, network: &mut Vec<U>, debug_mode: bool) {
    if debug_mode { println!{"@[NETWORK]>> Sent {:?}.", &network_buffer} }

    network.append(network_buffer);

    if debug_mode { println!{"@[NETWORK]>> Message history {:?}.", network} }
}
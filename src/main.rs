#![allow(dead_code, unused_assignments)] 

use std::{
    collections::VecDeque, 
    env, 
    fmt::Debug, 
    fs::File, 
    io::{BufWriter, Write}, 
    sync::mpsc::{self, TryRecvError::{Disconnected, Empty}}, 
    thread, time::{Duration, Instant, SystemTime}
};

#[derive(Clone, Debug)]
struct Data<T, U> {
    write: T,
    message: U
}

const ONE_THOUSAND: u128 = 1000;
const ONE_MILLION: u128 = 1000000;

/* ----------------------------- OPTION PARSING FUNCTIONS ----------------------------- */
fn args_help() {
    println!("--help | -h => Show help message.
--debug | -d => Show debug messages during execution (default = false).
Necessary (use ONLY ONE option):
--runtime {{Int}} | -r {{Int}} => Set runtime in seconds for this execution.
--num_writes {{Int}} | -n {{Int}} => Set number of writes the disk will perform in this execution.
Optional:
--buffer_size {{Int}} | -bs {{Int}} => Set buffer size to be written (default = 4096).
--disk_delay {{Int}} | -dd {{Int}} => Set disk delay in milliseconds (default = 20).
--msgs_per_interval {{Int}} | -mpi {{Int}} => Set number of messages sent per network interval (default = 1).");
}

fn args_error(error_msg: &str) {
    eprintln!("\x1b[93mError in options: {}.\x1b[0m", error_msg);
    args_help();
}

fn parse_args(args: Vec<String>) -> Option<(bool, u128, u128, u128, u128, usize)> {
    let mut debug_mode: bool = false;
    let mut runtime: u128 = 0;
    let mut num_writes: u128 = 0;
    let mut buffer_size: u128 = 4096;
    let mut disk_delay: u128 = 20;
    let mut msgs_per_interval: usize = 1;

    let mut i = 1;
    let len = args.len();
    while i < len {
        let option = &args[i];
        match option.as_str() {
            "--help" | "-h" => {
                args_help();
                return None;
            }
            "--debug" | "-d" => { debug_mode = true; }
            "--runtime" | "-r" => {
                if i + 1 < len {
                    let val = &args[i + 1];
                    runtime = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--runtime option requires a number");
                            return None;
                        },
                    };
                    i += 1;
                } else {
                    args_error("--runtime option requires a number");
                    return None;
                }
            }
            "--num_writes" | "-n" => {
                if i + 1 < len {
                    let val = &args[i + 1];
                    num_writes = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--num_writes option requires a number");
                            return None;
                        },
                    };
                    i += 1;
                } else {
                    args_error("--num_writes option requires a number");
                    return None;
                }
            }
            "--buffer_size" | "-bs" => {
                if i + 1 < len {
                    let val = &args[i + 1];
                    buffer_size = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--buffer_size option requires a number");
                            return None;
                        },
                    };
                    i += 1;
                } else {
                    args_error("--buffer_size option requires a number");
                    return None;
                }
            }
            "--disk_delay" | "-dd" => {
                if i + 1 < len {
                    let val = &args[i + 1];
                    disk_delay = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--disk_delay option requires a number");
                            return None;
                        },
                    };
                    i += 1;
                } else {
                    args_error("--disk_delay option requires a number");
                    return None;
                }
            }
            "--msgs_per_interval" | "-mpi" => {
                if i + 1 < len {
                    let val = &args[i + 1];
                    msgs_per_interval = match val.parse() {
                        Ok(n) => { n },
                        Err(_) => {
                            args_error("--msgs_per_interval option requires a number");
                            return None;
                        },
                    };
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

    Some((debug_mode, runtime, num_writes, buffer_size, disk_delay, msgs_per_interval))
}

/* ----------------------------- MAIN ----------------------------- */
fn main() {
    /* Collect all options for this run. */
    env::set_var("RUST_BACKTRACE", "full");
    let args: Vec<String> = env::args().collect();
    let (debug_mode, runtime, mut num_writes,  
        buffer_size, disk_delay, msgs_per_interval) =
        match parse_args(args) {
            Some(x) => x,
            None => { return; },
        };
    
    if runtime == 0 && num_writes == 0 {
        args_error("runtime or num_writes option is required");
        return;
    }

    if runtime != 0 && num_writes != 0 {
        args_error("both runtime and num_writes options were set, only one is required");
        return;
    }

    if runtime != 0 {
        num_writes = runtime * ONE_THOUSAND / disk_delay;
    }

    if debug_mode {
        println!("Debug mode is ON.");
    } else {
        println!("Debug mode is OFF\n(To activate debug mode use the \"--debug\" option.)\n")
    }

    
    println!("@[MAIN] Program is executing...");
    let clock = Instant::now();

    /* Create MSPC channels. */
    // Producer -> Disk
    let (tx_prod, rx_disk) = mpsc::channel();
    // Disk -> Network
    let (tx_disk, rx_net) = mpsc::channel();

    /* Start consumer threads. */
    // Disk
    let h1 = thread::spawn(move || { 
        consumer_disk(rx_disk, tx_disk, disk_delay, debug_mode); 
    });
    // Network
    let h2 = thread::spawn(move || { 
        consumer_network(rx_net, disk_delay, msgs_per_interval, debug_mode); 
    });
    
    /* Loop to generate and send data to consumer. */
    for _ in 0..num_writes {
        let prod_clock = Instant::now();
        for _ in 0..buffer_size {
            tx_prod.send(producer(debug_mode)).unwrap();
        }
        let wait_time = (disk_delay * ONE_MILLION).saturating_sub(prod_clock.elapsed().as_nanos());
        thread::sleep(Duration::from_nanos(wait_time as u64));
    }

    /* Tell threads to finish their work. */
    tx_prod.send(None).unwrap();

    h1.join().unwrap();
    println!("> Disk runtime was {}ms.", clock.elapsed().as_millis());
    h2.join().unwrap();
    println!("> Network runtime was {}ms.", clock.elapsed().as_millis());
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
fn producer(debug_mode: bool) -> Option<Data<u128, u128>> {
    let timestamp = get_unix_timestamp();
    let data = Data{ write: timestamp, message: timestamp };

    if debug_mode { println!("@[PRODUCER]>> Generated [{:?}].", data) }

    Some(data)
}

/* ----------------------------- DISK RELATED ----------------------------- */
fn consumer_disk<T: Clone + Debug, U: Clone + Debug>
        (rx_disk: mpsc::Receiver<Option<Data<T, U>>>, 
        tx_disk: mpsc::Sender<Option<Vec<U>>>, 
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
                Some(d) => {
                    disk_buffer.push(d.write);
                    network_buffer.push(d.message);
                },
                None => { shutdown = true; }
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
                tx_disk.send(Some(to_be_sent_buffer)).unwrap();

                // The stable buffer is done.
                stable_msg_qty = 0;
            }

            if shutdown && unstable_msg_qty == 0 {
                tx_disk.send(None).unwrap();
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
fn consumer_network<U: Clone + Debug>(rx_net: mpsc::Receiver<Option<Vec<U>>>, 
        disk_delay: u128, msgs_per_interval: usize, debug_mode: bool) {
    // Structures
    let mut network_buffers: VecDeque<Vec<U>> = VecDeque::new(); //queue for all message buffers
    let mut buffer_to_be_sent: Vec<U> = vec![]; //buffer that will be sent in slices
    let mut network_history: Vec<U> = vec![]; //sent messages history

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
                Some(m) => { network_buffers.push_back(m); },
                None => { shutdown = true; }
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    if !shutdown {
                        println!("@[CONSUMER] #ERROR: Consumer network thread died!");
                    }
                }
            }
        }

        if shutdown && network_buffers.is_empty() && buffer_to_be_sent.is_empty() {
            println!("@[CONSUMER] #SHUTDOWN: Finished work in network thread.");
            break;
        }
        
        // match network_buffers.pop_front() {
        //     Some(b) => {
        //         buffer_to_be_sent = b;
        //     },
        //     None => { continue; }
        // }
        // send_to_network(&mut buffer_to_be_sent, &mut network_history, debug_mode);
        
        // Send slices of the buffers.
        let elapsed_time: u128 = clock.elapsed().as_nanos();
        if elapsed_time >= send_interval {
            let mut buffer_size = buffer_to_be_sent.len();
            // Start sending the next buffer.
            if num_sends == 0 {
                match network_buffers.pop_front() {
                    Some(b) => {
                        buffer_to_be_sent = b;
                    },
                    None => { continue; }
                }
                buffer_size = buffer_to_be_sent.len();
                num_sends = ((buffer_size + (msgs_per_interval - 1)) / msgs_per_interval) as u128;
                send_interval = ((disk_delay / 2) * ONE_MILLION) / num_sends as u128;
            }
            
            if buffer_size > 0 {
                // There might be N messages left in the buffer, where N < msgs_per_interval.
                let split_index = std::cmp::min(msgs_per_interval, buffer_size);
                let mut v = buffer_to_be_sent.split_off(split_index);
                // Reference swap to retain pointers in the right variables.
                let temp = buffer_to_be_sent;
                buffer_to_be_sent = v;
                v = temp;

                write_to_log_file(&mut log_file, &v);

                send_to_network(&mut v, &mut network_history, debug_mode);
            
                num_sends -= 1; //decrement number of sends left for this buffer
            }

            clock = Instant::now(); //refresh clock
        }
    }
}

// Send messages to the network layer.
fn send_to_network<U: Debug>(network_buffer: &mut Vec<U>, network: &mut Vec<U>, debug_mode: bool) {
    if debug_mode { println!{"@[NETWORK]>> Sent {:?}.", &network_buffer} }
    network.append(network_buffer);
    if debug_mode { println!{"@[NETWORK]>> Message history {:?}.", network} }
}
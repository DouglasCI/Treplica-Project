#![allow(dead_code, unused_assignments)] 

use std::{
    thread, 
    sync::{mpsc, mpsc::TryRecvError::{Empty, Disconnected}}, 
    time::{Duration, Instant},
    fmt::Debug, 
};
use rand::{thread_rng, Rng, distributions::Alphanumeric};

#[derive(Clone, Debug)]
struct Data<T, U> {
    write: T,
    message: U
}

// ############### escrever no arquivo timestamp absoluto estilo unix
// ############### escrever em pares de timestamp (latencia 0)"
// ############### um pro produtor, um pro disco e outro pra rede
// "pares no formato: {timestamp do produtor} {timestamp do disco/rede}\n"
const ONE_THOUSAND: u128 = u128::pow(10, 3);
// Main
const PRODUCER_DELAY: u64 = ONE_THOUSAND as u64;
// Disk
const DISK_DELAY: u128 = 6 * ONE_THOUSAND;
const PATIENCE: u128 = DISK_DELAY + 5 * ONE_THOUSAND;
const BUFFER_SIZE: usize = 5;
// Network
const NETWORK_DELAY: u128 = ONE_THOUSAND;
const MSGS_PER_INTERVAL: usize = 2;

fn main() {
    /* Create MSPC channels */
    // Producer -> Disk
    let (tx_prod, rx_disk) = mpsc::channel();
    // Disk -> Network
    let (tx_disk, rx_net) = mpsc::channel();

    /* Start consumer threads */
    // Disk
    thread::spawn(move || { consumer_disk(rx_disk, tx_disk); });
    // Network
    thread::spawn(move || { consumer_network(rx_net); });
    
    // Loop to generate and send data to consumer
    let mut counter: u32 = 0; //for message field in Data struct
    loop {
        tx_prod.send(producer(counter)).unwrap();
        counter += 1;
        thread::sleep(Duration::from_millis(PRODUCER_DELAY));
    }
}

fn producer(counter: u32) -> Data<String, u32> {
    let w: String = thread_rng()
    .sample_iter(&Alphanumeric)
    .take(4)
    .map(char::from)
    .collect();

    // #################### passar o timestamp da produçao
    let data = Data{ write: w, message: counter };

    println!("@[PRODUCER]>> Generated [{:?}].", data);
    data
}

fn consumer_disk<T: Clone + Debug, U: Clone + Debug>
                (rx_disk: mpsc::Receiver<Data<T, U>>, tx_disk: mpsc::Sender<Vec<U>>) {
    // Structures
    let mut buffer: Vec<Data<T, U>> = vec![]; //data buffer
    let mut disk: Vec<T> = vec![]; //false disk
    let mut not_ready_msg_buffer: Vec<U> = vec![]; //message buffer to be sent to network

    // Time control
    let mut clock = Instant::now();

    loop {
        // Non-blocking receiver
        match rx_disk.try_recv() {
            Ok(data) => {
                // Received data in this iteration!
                buffer.push(data);
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    println!("#[ERROR]>> Consumer disk thread died!");
                }
            }
        }

        let elapsed_time: u128 = clock.elapsed().as_millis();
        // Is the disk available now? 
        if elapsed_time >= DISK_DELAY {
            let mut write_buffer: Vec<T> = vec![];
            let mut ready_msg_buffer: Vec<U> = vec![];
            
            // Since the disk is avaiable now, the not_ready_msg_buffer is ready
            ready_msg_buffer.append(&mut not_ready_msg_buffer);

            // So start sending the ready_msg_buffer through the network
            if !ready_msg_buffer.is_empty() {
                tx_disk.send(ready_msg_buffer).unwrap();
            }
            
            // Is the buffer full? Or is the disk idle for too long and the buffer is not empty?
            if buffer.len() >= get_buffer_size() || (elapsed_time >= PATIENCE && !buffer.is_empty()) {
                // ############### usar o buffer interno do rx_disk ao em vez de let buffer
                (write_buffer, not_ready_msg_buffer) = split_data_buffer(&buffer);
                flush_to_disk(&mut write_buffer, &mut disk);
                buffer.clear();
                clock = Instant::now(); //refresh clock
            }
        }
    }
}

// Allows dynamic buffer size
fn get_buffer_size() -> usize { BUFFER_SIZE }

// Split data buffer into write and message buffers
fn split_data_buffer<T: Clone + Debug, U: Clone + Debug>(buffer: &Vec<Data<T, U>>) -> (Vec<T>, Vec<U>) {
    let mut write_buffer: Vec<T> = vec![];
    let mut msg_buffer: Vec<U> = vec![];

    for d in buffer {
        let dd = d.clone();
        write_buffer.push(dd.write);
        msg_buffer.push(dd.message);
    }

    (write_buffer, msg_buffer)
}

// Write buffer to disk
fn flush_to_disk<T: Debug>(write_buffer: &mut Vec<T>, disk: &mut Vec<T>) {
    // ######################## escrever no arquivo o timestamp que veio do Data e o timestamp de AGORA
    println!{"@[DISK]>> Disk contains {:?}.", disk};
    println!{"@[DISK]>> Disk is now writing {:?}.", &write_buffer};
    disk.append(write_buffer);
}

fn consumer_network<U: Clone + Debug>(rx_net: mpsc::Receiver<Vec<U>>) {
    // Structures
    let mut msg_buffer: Vec<U> = vec![]; //buffer for all messages
    let mut network: Vec<U> = vec![]; //sent messages history

    // Time control
    let mut clock = Instant::now();

    loop {
        // Non-blocking receiver
        match rx_net.try_recv() {
            Ok(messages) => {
                // Received data in this iteration!
                msg_buffer.extend(messages);
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},
                // Channel disconnected!
                Disconnected => {
                    println!("#[ERROR]>> Consumer network thread died!");
                }
            }
        }
        
        // Send small slices of the msg_buffer
        let elapsed_time: u128 = clock.elapsed().as_millis();
        let msg_qty = get_msgs_per_interval();
        if elapsed_time >= NETWORK_DELAY && msg_buffer.len() >= msg_qty {
            let mut v = msg_buffer.drain(..msg_qty).collect();
            send_to_network(&mut v, &mut network);
            clock = Instant::now(); //refresh clock
        }
    }
}

// Allows dynamic amount of messages sent to network per interval
fn get_msgs_per_interval() -> usize { MSGS_PER_INTERVAL }

fn send_to_network<U: Debug>(msg_buffer: &mut Vec<U>, network: &mut Vec<U>) {
    // ######################## escrever no arquivo o timestamp que veio do Data e o timestamp de AGORA
    println!{"@[NETWORK]>> Sent {:?}.", &msg_buffer};
    network.append(msg_buffer);
    println!{"@[NETWORK]>> Message history {:?}.", network};
}

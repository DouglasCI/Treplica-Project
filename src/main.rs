use std::{
    thread, 
    sync::{mpsc, mpsc::TryRecvError::{Empty, Disconnected}}, 
    time::{Duration, Instant},
    fmt::Debug
};
use rand::{thread_rng, Rng, distributions::Alphanumeric};

fn main() {
    // Create MSPC channel
    let (tx, rx) = mpsc::channel();

    // Start consumer thread
    thread::spawn(move || { consumer(rx); });
    
    // Loop to generate and send data to consumer
    loop {
        tx.send(generate_data()).unwrap();
        thread::sleep(Duration::from_secs(1)); //1s delay
    }
}

fn generate_data() -> String {
    // let data = thread_rng().gen_range(0..100);
    let data = thread_rng()
    .sample_iter(&Alphanumeric)
    .take(4)
    .map(char::from)
    .collect();

    println!("Generated [{:?}] in main thread.", data);
    data
}

fn consumer<T: Debug>(rx: mpsc::Receiver<T>) {
    // Buffer and "fake" disk
    let mut buffer: Vec<T> = vec![];
    let mut disk: Vec<T> = vec![];

    // Time control
    let mut now = Instant::now();

    // Loop to receive and consume data
    loop {
        // Non-blocking receiver
        match rx.try_recv() {
            Ok(data) => {
                // Received data in this iteration!
                buffer.push(data);
            }
            Err(error) => match error {
                // Did not receive any data in this iteration!
                Empty => {},

                // Channel disconnected!
                Disconnected => {
                    println!("# ERROR: Consumer thread died!");
                    break
                }
            }
        }

        if is_disk_ready(now, buffer.len()) {
            flush(&mut buffer, &mut disk);
            now = Instant::now(); //refresh clock
        }
    }
}

// Check if it's time to write the buffer to the disk
fn is_disk_ready(now: Instant, buffer_len: usize) -> bool {
    let elapsed_time: u128 = now.elapsed().as_millis();
    const ONE_THOUSAND: u128 = u128::pow(10, 3);
    const DISK_DELAY: u128 = 5 * ONE_THOUSAND; //5000ms delay
    const PATIENCE: u128 = DISK_DELAY + ONE_THOUSAND; //+1000ms delay

    // Is the disk available now? 
    elapsed_time >= DISK_DELAY && 
    // If so, does my buffer have enough elements or
    (buffer_len >= get_buffer_size() ||
    // is it taking too long to fill and it's not empty?
    (elapsed_time >= PATIENCE && buffer_len > 0))
}

// Allows dynamic buffer size
fn get_buffer_size() -> usize { 7 }

// Write buffer to disk
fn flush<T: Debug>(buffer: &mut Vec<T>, disk: &mut Vec<T>) {
    println!{">> Flushed {:?} in consumer thread.", &buffer};
    disk.append(buffer);
    println!{">> Now disk contains {:?}", disk};
}
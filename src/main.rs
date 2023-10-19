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
    let mut elapsed_time: u128;
    const DISK_DELAY: u128 = 5 * u128::pow(10, 6); //5s delay

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

        // Is the disk available now? If so, does my buffer has enough elements?
        elapsed_time = now.elapsed().as_micros();
        if elapsed_time >= DISK_DELAY && buffer.len() >= get_buffer_size() {
            flush(&mut buffer, &mut disk);
            now = Instant::now(); //refresh clock
        }
    }
}

// Allows dynamic buffer size
fn get_buffer_size() -> usize { 1 }

// Write buffer to disk
fn flush<T: Debug>(buffer: &mut Vec<T>, disk: &mut Vec<T>) {
    println!{">> Flushed {:?} in consumer thread.", &buffer};
    disk.append(buffer);
    println!{">> Now disk contains {:?}", disk};
}
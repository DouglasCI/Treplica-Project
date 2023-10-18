use std::thread;
use std::sync::mpsc;

use rand::thread_rng;
use rand::Rng;

fn main() {
    // Create MSPC channel
    let (tx, rx) = mpsc::channel();

    // Thread to generate and send random numbers
    let handle = thread::spawn(move || {
        for _ in 1..10 {
            let num = thread_rng().gen_range(0..100);
            tx.send(num).unwrap();
        }

        drop(tx); //close channel
    });

    // Buffer
    let mut buffer: Vec<i32> = vec![];

    // Push numbers received from thread into the buffer
    for num in &rx {
        buffer.push(num);
    }

    println!{"{:?}", buffer};

    handle.join().unwrap();
}
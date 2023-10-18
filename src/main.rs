use std::thread;
use std::sync::mpsc;

use rand::{thread_rng, Rng};

fn main() {
    // Create MSPC channel
    let (tx, rx) = mpsc::channel();

    // Inverter produtor e consumidor entre as threads
    // adicionar delay, mas adicionar um delay maior (simular disco) para o consumidor consumir o buffer de uma vez
    // gerar como coisas como tipo generico <T>

    // Thread to generate and send random numbers
    let handle = thread::spawn(move || {
        for _ in 1..10 {
            let num = thread_rng().gen_range(0..100);
            tx.send(num).unwrap();
        }
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
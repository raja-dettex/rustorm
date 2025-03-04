use tokio::sync::{Mutex, mpsc};
use std::sync::Arc;
use tokio_postgres:: NoTls;

use crate::client::ClientWrapper;

#[derive(Debug)]
pub struct ConnectionPool {
    sender: mpsc::Sender<ClientWrapper>,
    reciever: Arc<Mutex<mpsc::Receiver<ClientWrapper>>>
}

#[derive(Debug)]
pub struct ConnectionErr {
    msg: String
}

impl ConnectionPool {
    pub async fn new(url : &str, pool_size : usize) -> Result<Self, ConnectionErr> {
        let (sx , rx) = mpsc::channel(pool_size);
        
        for _i in 0..pool_size {
            let (client, connection) = tokio_postgres::connect(url, NoTls).await
            .map_err(|e| ConnectionErr{msg: e.to_string()})?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("{e:?}")
            }
            });
            sx.send(ClientWrapper::new(client)).await.map_err(|e| ConnectionErr{msg: e.to_string()})?;
        }
        Ok(Self {sender: sx, reciever: Arc::new(Mutex::new(rx))})
    }

    pub async fn get_connection(&self) -> Option<ClientWrapper> { 
        self.reciever.lock().await.recv().await
    }

    pub async fn release_connection(&self, client: ClientWrapper) {
        let _ = self.sender.send(client).await;
    }
}


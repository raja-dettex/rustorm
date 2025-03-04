use crate::pool::ConnectionPool;
use crate::sql::{Row, Statement};
use crate::transaction::{IsolationLevel, Transaction, TransactionErr};

use crate::error::{DatabaseError, ExecutionError, ClientNotFound};

#[derive(Debug)]
pub struct DatabaseManager {
    pool: ConnectionPool,
}

impl DatabaseManager { 
    pub fn new(pool: ConnectionPool) -> Self { 
        Self{pool}
    }

    pub async fn query(&mut self, query: Statement) -> Result<Vec<Row>, DatabaseError> { 
        if let Some(client) = self.pool.get_connection().await { 
            let query_result = client.query(query).await;
            let _ = self.pool.release_connection(client).await;
            query_result
        } else { 
            Err(DatabaseError::ClientNotFound(ClientNotFound { msg: "client not available".to_string()}))
        }
        
    }

    pub async fn execute(&mut self, statement: Statement) -> Result<u64, DatabaseError> { 
        if let Some(client) = self.pool.get_connection().await { 
            let execution_result = client.execute(statement).await;
            let _ = self.pool.release_connection(client).await;
            execution_result
        } else { 
            Err(DatabaseError::ClientNotFound(ClientNotFound { msg: "client not available".to_string()}))
        }
        
    }
    pub async fn batch_execute(&mut self, statements: Vec<Statement>) -> Result<(), DatabaseError> { 
        if let Some(client) = self.pool.get_connection().await { 
            let batch_result = client.batch_execute(statements).await;
            let _ = self.pool.release_connection(client).await;
            batch_result
        } else { 
            Err(DatabaseError::ClientNotFound(ClientNotFound { msg: "client not available".to_string()}))
        }
        
    }

    pub async fn begin_transaction(&self, isolation_level: Option<IsolationLevel>) -> Result<Transaction, DatabaseError> { 
        if let Some(client) = self.pool.get_connection().await { 
            return Transaction::begin(isolation_level, client).await.map_err(|err| DatabaseError::ExecutionError(ExecutionError { msg: err.to_string() }));
        }
        Err(DatabaseError::ClientNotFound(ClientNotFound { msg: "client not available".to_string() }))
    }

    pub fn execute_transaction(&self, transaction : &mut Transaction, statement: Statement)  { 
        transaction.execute(statement); 
    }


    pub async fn rollback_transaction(&self, transaction: &mut Transaction) -> Result<(), TransactionErr> { 
        let res = transaction.rollback().await;
        let _ = self.pool.release_connection(transaction.client.clone());
        res
    }

    pub async fn commit(&self, transaction: &mut Transaction) -> Result<(), TransactionErr> { 
        match transaction.commit().await {
            Ok(_) => { 
                let _ = self.pool.release_connection(transaction.client.clone());
                return Ok(());
            },
            Err(err) =>  { 
                let _ = self.rollback_transaction(transaction).await;
                return Err(err)
            },
        }
        
    } 
    
}

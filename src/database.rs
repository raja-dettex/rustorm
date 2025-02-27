use std::collections::HashMap;

use crate::pool::ConnectionPool;
use crate::sql::{Column, ColumnMeta, ColumnValue, Parameter, Row, Statement};
use tokio_postgres::{Client, SimpleQueryMessage};
use crate::error::{DatabaseError, ExecutionError, InvalidSql};

#[derive(Debug)]
pub struct DatabaseClient {
    pool: ConnectionPool,
    active_clients: HashMap<uuid::Uuid, Client>
}

impl DatabaseClient { 
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool: pool, active_clients: HashMap::new()}
    }

    pub fn active_client(&self, id: uuid::Uuid) -> Option<&Client> { 
        match self.active_clients.get(&id) { 
            Some(&ref client) => Some(client),
            None => None
        }
    }

    pub fn add_active_client(&mut self, id: uuid::Uuid, client: Client) -> Option<Client>  {
        self.active_clients.insert(id, client)
    }

    pub fn remove_active_client(&mut self, id: uuid::Uuid) -> Option<Client>{ 
        self.active_clients.remove(&id)
    }

    pub async fn query(&self, query: Statement ) -> Result<Vec<Row>, DatabaseError>
    { 
        if let Some(client) = self.pool.get_connection().await {
            
            // let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|&p| p as &(dyn ToSql + Sync)).collect();
            if let Ok(query_str) = query.tosql() {
                
                match client.simple_query(&query_str).await {
                    Ok(messages) =>  {
                        let mut rows = Vec::new();
                        for message in messages  {
                            if let SimpleQueryMessage::Row(row) = message { 
                                let mut cols = Vec::new();
                                let columns = row.columns();
                                if(row.is_empty()) {
                                    let _ = self.pool.release_connection(client).await;
                                    return Ok(vec![])
                                }
                                for idx in 0..row.len() {
                                    let value = match row.try_get(idx) {
                                        Ok(Some(val)) => Some(ColumnValue::String(val.to_string())),
                                        Ok(None) => None,
                                        Err(_) => None,
                                    };
                                    let meta = match columns.get(idx) {
                                        Some(col) => Some(ColumnMeta{name: col.name().to_string()}),
                                        None => None,
                                    };
                                    let column = Column {value, meta };
                                    let col_cloned = column.clone();
                                    cols.push(col_cloned);
                                    
                                }
                                let addedRow = Row {columns: cols};
                                rows.push(addedRow)
                            }
                        }
                        let _ = self.pool.release_connection(client).await;
                        
                        return Ok(rows);
                    },
                    Err(err) => {
                        let _ = self.pool.release_connection(client).await;
                        return Err(DatabaseError::ExecutionError(ExecutionError{msg: err.to_string()}));
                    }
                }
            } else if let Err(err) = query.tosql(){ 
                return Err(DatabaseError::InvalidSql(err));
            }
            
        }
        Ok(vec![])
    } 
    pub async fn execute(&self, statement: Statement ) -> Result<u64, DatabaseError>
    { 
        
        if let Some(client) = self.pool.get_connection().await {
           
            // let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|&p| p as &(dyn ToSql + Sync)).collect();
            if let Ok(stmt_str) = statement.tosql() {
                match client.simple_query(&stmt_str).await {
                    Ok(messages) =>  {
        
                        if let Some(SimpleQueryMessage::CommandComplete(count)) = messages.get(0) { 
                            self.pool.release_connection(client).await;
                            return Ok(*count);
                        }
                    },
                    Err(err) => {
                        self.pool.release_connection(client).await;
                        return Err(DatabaseError::ExecutionError(ExecutionError{msg: err.to_string()}));
                    }
                }
            } else if let Err(err) = statement.tosql(){ 
                self.pool.release_connection(client).await;
                return Err(DatabaseError::InvalidSql(err));
            }
            
        }
        Ok(0)
    }
    pub async fn batch_execute(&self, statements : Vec<Statement>) { 
        // execute statements in batch 
        let stmts: Vec<Option<String>> = statements.iter().map(|stmt| { 
            if let Ok(sql) = stmt.tosql() { 
                return Some(sql);
            }
            None
        }).collect();
        let mut batch_statement = String::new();
        for stmt in stmts {
            if let Some(sql) = stmt { 
                batch_statement.push_str(format!("{};", sql).as_ref());
            }
        }
        if let Some(client) = self.pool.get_connection().await {
            match client.simple_query(&batch_statement).await {
                Ok(result) => println!("{result:?}"),
                Err(err) => println!("{err:?}"),
            }
        } else { 
            println!("no client found");
        }
    }
}


/* #[tokio::test]
pub async fn test_batch() {
    let params1 = vec![
        Parameter::Int(1),
        Parameter::StringValue("raja".to_string())
    ];
    let params2 = vec![
        Parameter::Int(2),
        Parameter::StringValue("devi".to_string())
    ];
    let statements = vec![
        Statement::new("insert into users values(?,?)".to_string(), Some(params1)),
        Statement::new("insert into users values(?,?)".to_string(), Some(params2))
    ];
    let pool = ConnectionPool::new("postgresql://postgres:raja@localhost:5432/demo-db", 4).await.unwrap();
    let db_client = DatabaseClient::new(pool);
    db_client.batch_execute(statements).await;
} */ 
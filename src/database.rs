use crate::pool::ConnectionPool;
use crate::sql::{InvalidSql, Column, Row, ColumnMeta, ColumnValue, Statement};
use tokio_postgres::SimpleQueryMessage;

#[derive(Debug)]
pub struct Database {
    pool: ConnectionPool
}
#[derive(Debug)]
pub enum DatabaseError { 
    InvalidSql(InvalidSql),
    ExecutionError(ExecutionError)
}

#[derive(Debug)]
pub struct ExecutionError { 
    msg: String
}

impl Database { 
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool: pool}
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
                        
                        return Ok(rows);
                    },
                    Err(err) => return Err(DatabaseError::ExecutionError(ExecutionError{msg: err.to_string()})),
                }
            } else if let Err(err) = query.tosql(){ 
                return Err(DatabaseError::InvalidSql(err));
            }
            
        }
        Ok(vec![])
    } 
    pub async fn execute(&self, statement: Statement  ) -> Result<u64, DatabaseError>
    { 
        if let Some(client) = self.pool.get_connection().await {
            // let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|&p| p as &(dyn ToSql + Sync)).collect();
            if let Ok(stmt_str) = statement.tosql() {
                match client.simple_query(&stmt_str).await {
                    Ok(messages) =>  {
        
                        if let Some(SimpleQueryMessage::CommandComplete(count)) = messages.get(0) { 
                            return Ok(*count);
                        }
                    },
                    Err(err) => return Err(DatabaseError::ExecutionError(ExecutionError{msg: err.to_string()})),
                }
            } else if let Err(err) = statement.tosql(){ 
                return Err(DatabaseError::InvalidSql(err));
            }
            
        }
        Ok(0)
    }
}

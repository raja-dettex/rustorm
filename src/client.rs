use std::sync::{Arc, Mutex};

use crate::error::{DatabaseError, ExecutionError};
use crate::sql::{Column, ColumnMeta, ColumnValue, Row, Statement};
use tokio_postgres::{Client as PostgresClient, SimpleQueryMessage};


#[derive(Clone)]
pub struct ClientWrapper {
    client: Arc<Mutex<PostgresClient>>
}


impl ClientWrapper {
    pub fn new(client: PostgresClient) -> Self {
        Self { client: Arc::new(Mutex::new(client)) }
    }
    pub async fn query(&self, query: Statement) -> Result<Vec<Row>, DatabaseError> {
        // let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|&p| p as &(dyn ToSql + Sync)).collect();
        if let Ok(query_str) = query.tosql() {
            let client = self.client.lock().unwrap();
            match client.simple_query(&query_str).await {
                Ok(messages) => {
                    let mut rows = Vec::new();
                    for message in messages {
                        if let SimpleQueryMessage::Row(row) = message {
                            let mut cols = Vec::new();
                            let columns = row.columns();

                            for idx in 0..row.len() {
                                let value = match row.try_get(idx) {
                                    Ok(Some(val)) => Some(ColumnValue::String(val.to_string())),
                                    Ok(None) => None,
                                    Err(_) => None,
                                };
                                let meta = match columns.get(idx) {
                                    Some(col) => Some(ColumnMeta {
                                        name: col.name().to_string(),
                                    }),
                                    None => None,
                                };
                                let column = Column { value, meta };
                                let col_cloned = column.clone();
                                cols.push(col_cloned);
                            }
                            let added_row = Row { columns: cols };
                            rows.push(added_row)
                        }
                    }

                    return Ok(rows);
                }
                Err(err) => {
                    return Err(DatabaseError::ExecutionError(ExecutionError {
                        msg: err.to_string(),
                    }));
                }
            }
        } else if let Err(err) = query.tosql() {
            return Err(DatabaseError::InvalidSql(err));
        }

        Ok(vec![])
    }
    pub async fn execute(
        &self,
        statement: Statement
    ) -> Result<u64, DatabaseError> {
        // let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|&p| p as &(dyn ToSql + Sync)).collect();
        if let Ok(stmt_str) = statement.tosql() {
            let client = self.client.lock().unwrap();
            match client.simple_query(&stmt_str).await {
                Ok(messages) => {
                    if let Some(SimpleQueryMessage::CommandComplete(count)) = messages.get(0) {
                        return Ok(*count);
                    }
                }
                Err(err) => {
                    return Err(DatabaseError::ExecutionError(ExecutionError {
                        msg: err.to_string(),
                    }));
                }
            }
        } else if let Err(err) = statement.tosql() {
            return Err(DatabaseError::InvalidSql(err));
        }

        Ok(0)
    }
    pub async fn batch_execute(&self, statements: Vec<Statement>) -> Result<(), DatabaseError>{
        // execute statements in batch
        let stmts: Vec<Option<String>> = statements
            .iter()
            .map(|stmt| {
                if let Ok(sql) = stmt.tosql() {
                    return Some(sql);
                }
                None
            })
            .collect();
        let mut batch_statement = String::new();
        for stmt in stmts {
            if let Some(sql) = stmt {
                batch_statement.push_str(format!("{};", sql).as_ref());
            }
        }
        let client = self.client.lock().unwrap();
        match client.simple_query(&batch_statement).await {
            Ok(_) => return Ok(()),
            Err(err) => return Err(DatabaseError::ExecutionError(ExecutionError { msg: err.to_string() })),
        }
    }
}
